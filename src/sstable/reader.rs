use std::collections::BTreeMap;
use std::fs::File;
use std::borrow::Borrow;
use std::io::{BufRead, BufReader, Cursor, Read, Seek, SeekFrom};
use std::path::Path;

use bincode;
use memmap;

use memchr;

use super::*;

use block_reader::{BlockManager, Block};

trait InnerReader {
    fn get(&mut self, key: &str) -> Result<Option<GetResult>>;
}

#[derive(Debug, PartialEq)]
pub enum GetResult<'a> {
    Ref(&'a [u8]),
    Owned(Vec<u8>),
}

impl<'a> GetResult<'a> {
    pub fn as_bytes(&self) -> &[u8] {
        use GetResult::*;
        match self {
            Ref(b) => b,
            Owned(b) => b,
        }
    }
}

impl<'a> AsRef<[u8]> for GetResult<'a> {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

enum MetaData {
    V1_0(MetaV1_0),
}

struct MetaResult {
    meta: MetaData,
    offset: usize,
}

fn read_metadata(file: &mut File) -> Result<MetaResult> {
    file.seek(SeekFrom::Start(0))?;
    let mut reader = posreader::PosReader::new(BufReader::new(file), 0);
    let mut buf = [0; MAGIC.len()];
    if reader.read(&mut buf)? != MAGIC.len() {
        return Err(Error::InvalidData("not an sstable"));
    }
    if buf != MAGIC {
        return Err(Error::InvalidData("not an sstable"));
    }
    let version: Version = bincode::deserialize_from(&mut reader)?;
    let meta = match version {
        VERSION_10 => {
            let meta: MetaV1_0 = bincode::deserialize_from(&mut reader)?;
            MetaData::V1_0(meta)
        }
        _ => return Err(Error::UnsupportedVersion(version)),
    };

    let offset = reader.current_offset();
    let file = reader.into_inner().into_inner();
    file.seek(SeekFrom::Start(offset as u64))?;

    Ok(MetaResult {
        meta: meta,
        offset: offset,
    })
}

enum BlockCacheTypeForMmapSSTableReaderV1_0 {
    Caching(block_reader::CachingDMABlockManager<'static>),
    NotCaching(block_reader::DMABlockManager<'static>),
}

struct MmapSSTableReaderV1_0 {
    #[allow(dead_code)]
    mmap: memmap::Mmap,
    index_start: u64,
    // it's not &'static in reality, but it's bound to mmap's lifetime.
    // It will NOT work with compression.
    index: BTreeMap<&'static str, u64>,
    // cache: block_reader::CachingDMABlockManager<'static>,
    cache: BlockCacheTypeForMmapSSTableReaderV1_0,
}

fn find_bounds<K, T>(map: &BTreeMap<K, T>, key: &str, end_default: T) -> Option<(T, T)>
where K: Borrow<str> + std::cmp::Ord,
      T: Copy,
{
    use std::ops::Bound;

    let start = {
        let mut iter_left = map
            .range::<str, _>((Bound::Unbounded, Bound::Included(key)));
        let closest_left = iter_left.next_back();
        match closest_left {
            Some((_, offset)) => *offset,
            None => return None,
        }
    };

    let end = {
        let mut iter_right = map
            .range::<str, _>((Bound::Excluded(key), Bound::Unbounded));
        let closest_right = iter_right.next_back();
        match closest_right {
            Some((_, offset)) => *offset,
            None => end_default,
        }
    };
    Some((start, end))
}


impl MmapSSTableReaderV1_0 {
    fn new(meta: MetaV1_0, data_start: u64, mut file: File, cache: Option<ReadCache>) -> Result<Self> {
        let mmap = unsafe { memmap::MmapOptions::new().map(&mut file) }?;

        let mut index = BTreeMap::new();

        let index_start = data_start + (meta.data_len as u64);

        let mut index_data = &mmap[(index_start as usize)..];
        if index_data.len() != meta.index_len {
            return Err(Error::InvalidData("invalid index length"));
        }

        let value_length_encoded_size = bincode::serialized_size(&Length(0))? as usize;

        while index_data.len() > 0 {
            let string_end = memchr::memchr(0, index_data);
            let zerobyte = match string_end {
                Some(idx) => idx,
                None => return Err(Error::InvalidData("corrupt index")),
            };
            let key = std::str::from_utf8(&index_data[..zerobyte])?;
            let key: &'static str = unsafe { &*(key as *const str) };
            index_data = &index_data[zerobyte + 1..];
            let value: Length = bincode::deserialize(&index_data[..value_length_encoded_size])?;
            index_data = &index_data[value_length_encoded_size..];
            index.insert(key, value.0);
        }

        let mmap_buf = &mmap[..];
        let mmap_buf: &'static [u8] = unsafe {&* (mmap_buf as *const _)};

        use BlockCacheTypeForMmapSSTableReaderV1_0::*;

        Ok(MmapSSTableReaderV1_0 {
            mmap: mmap,
            index_start: index_start,
            index: index,
            cache: match cache {
                Some(cache) => Caching(block_reader::CachingDMABlockManager::new(mmap_buf, cache)),
                None => NotCaching(block_reader::DMABlockManager::new(mmap_buf))
            }
        })
    }
}

impl InnerReader for MmapSSTableReaderV1_0 {
    fn get<'a, 'b>(&'a mut self, key: &'b str) -> Result<Option<GetResult<'a>>> {
        let (offset, right_bound) = match find_bounds(&self.index, key, self.index_start) {
            Some(v) => v,
            None => return Ok(None)
        };

        use BlockCacheTypeForMmapSSTableReaderV1_0::*;
        let block: &mut dyn Block = match &mut self.cache {
            Caching(bm) => bm.get_block(offset, right_bound)?,
            NotCaching(bm) => bm.get_block(offset, right_bound)?
        };

        let found = block.find_key(key)?;
        Ok(found.map(|v| GetResult::Ref(v)))
    }
}

struct ZlibFactory {}

type ZlibRead<'a> = BufReader<flate2::read::ZlibDecoder<Cursor<&'a [u8]>>>;

impl<'r> block_reader::ReaderFactory<'r, ZlibRead<'r>> for ZlibFactory {
    fn make_reader<'a>(&'a self, buf: &'r [u8]) -> ZlibRead<'r> {
        let cursor = std::io::Cursor::new(buf);
        let reader = flate2::read::ZlibDecoder::new(cursor);
        return BufReader::new(reader);
    }
}

struct ZlibReaderV1_0 {
    #[allow(dead_code)]
    mmap: memmap::Mmap,
    cache: block_reader::DMAThenReadBlockManager<'static, block_reader::CachingReaderBlock<ZlibRead<'static>>, ZlibFactory>,
    meta: MetaV1_0,
    data_start: u64,
    index: BTreeMap<String, u64>,
}

impl ZlibReaderV1_0 {
    fn new(meta: MetaV1_0, data_start: u64, mut file: File, cache: Option<ReadCache>) -> Result<Self> {
        let index_start = data_start + (meta.data_len as u64);

        file.seek(SeekFrom::Start(index_start))?;

        let file_buf_reader = BufReader::new(file);
        let decoder = flate2::read::ZlibDecoder::new(file_buf_reader);
        let mut buf_decoder = BufReader::new(decoder);
        let mut buf = Vec::with_capacity(4096);
        let mut index = BTreeMap::new();

        loop {
            buf.truncate(0);
            let size = buf_decoder.read_until(0, &mut buf)?;
            if size == 0 {
                // Index is read fully.
                break;
            }
            if buf[size - 1] != 0 {
                return Err(Error::InvalidData("corrupt file, no zero"));
            }
            let key = std::str::from_utf8(&buf[..size - 1])?.to_owned();
            let length = bincode::deserialize_from::<_, Length>(&mut buf_decoder)?.0;
            index.insert(key, length);
        }

        // TODO: check that the index size matches metadata
        let mut file = buf_decoder.into_inner().into_inner().into_inner();
        let mmap = unsafe { memmap::MmapOptions::new().map(&mut file) }?;

        let mmap_buf = &mmap[..];
        let mmap_buf: &'static [u8] = unsafe {&* (mmap_buf as *const _)};

        Ok(ZlibReaderV1_0 {
            mmap: mmap,
            cache: block_reader::DMAThenReadBlockManager::new(
                // TODO: replace 32 with cache
                mmap_buf, ZlibFactory{}, 32
            ),
            data_start: data_start,
            meta: meta,
            index: index,
        })
    }
}

impl InnerReader for ZlibReaderV1_0 {
    fn get(&mut self, key: &str) -> Result<Option<GetResult>> {
        let index_start = self.data_start + self.meta.data_len as u64;
        let (offset, right_bound) = match find_bounds(&self.index, key, index_start) {
            Some(v) => v,
            None => return Ok(None)
        };

        let block = self.cache.get_block(offset, right_bound)?;
        let found = block.find_key(key)?;
        Ok(found.map(|v| GetResult::Ref(v)))
    }
}

pub struct SSTableReader {
    inner: Box<dyn InnerReader>,
}

#[derive(Copy,Clone,Debug)]
pub enum ReadCache {
    Blocks(usize),
    Unbounded,
}

#[derive(Copy,Clone,Debug)]
pub struct ReadOptions {
    cache: Option<ReadCache>,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self{cache: Some(ReadCache::Blocks(32))}
    }
}

impl SSTableReader {
    pub fn new<P: AsRef<Path>>(filename: P) -> Result<Self> {
        Self::new_with_options(filename, &ReadOptions::default())
    }

    pub fn new_with_options<P: AsRef<Path>>(filename: P, opts: &ReadOptions) -> Result<Self> {
        let mut file = File::open(filename)?;
        let meta = read_metadata(&mut file)?;
        let data_start = meta.offset as u64;
        let meta = match meta.meta {
            MetaData::V1_0(meta) => meta,
        };
        let inner: Box<dyn InnerReader> = match meta.compression {
            Compression::None => Box::new(MmapSSTableReaderV1_0::new(meta, data_start, file, opts.cache)?),
            Compression::Zlib => Box::new(ZlibReaderV1_0::new(meta, data_start, file, opts.cache)?),
        };
        Ok(SSTableReader { inner: inner })
    }
    pub fn get(&mut self, key: &str) -> Result<Option<GetResult>> {
        self.inner.get(key)
    }
}
