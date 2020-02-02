use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufReader, Cursor, Read, Seek, SeekFrom};
use std::path::Path;

use bincode;
use memmap;

use super::*;

use block_reader::{Block, BlockManager};

trait InnerReader {
    fn get(&mut self, key: &[u8]) -> Result<Option<GetResult>>;
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
    pub fn len(&self) -> usize {
        use GetResult::*;
        match self {
            Ref(b) => b.len(),
            Owned(b) => b.len(),
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

struct MmapSSTableReaderV1_0 {
    #[allow(dead_code)]
    mmap: memmap::Mmap,
    index_start: u64,
    // it's not &'static in reality, but it's bound to mmap's lifetime.
    // It will NOT work with compression.
    index: BTreeMap<&'static [u8], u64>,
    cache: Box<dyn BlockManager>,
}

fn find_bounds<K, T>(map: &BTreeMap<K, T>, key: &[u8], end_default: T) -> Option<(T, T)>
where
    K: Borrow<[u8]> + std::cmp::Ord,
    T: Copy,
{
    use std::ops::Bound;

    let start = {
        let mut iter_left = map.range::<[u8], _>((Bound::Unbounded, Bound::Included(key)));
        let closest_left = iter_left.next_back();
        match closest_left {
            Some((_, offset)) => *offset,
            None => return None,
        }
    };

    let end = {
        let mut iter_right = map.range::<[u8], _>((Bound::Excluded(key), Bound::Unbounded));
        let closest_right = iter_right.next();
        match closest_right {
            Some((_, offset)) => *offset,
            None => end_default,
        }
    };
    Some((start, end))
}

impl MmapSSTableReaderV1_0 {
    fn new(
        meta: MetaV1_0,
        data_start: u64,
        mut file: File,
        cache: Option<ReadCache>,
    ) -> Result<Self> {
        let mmap = unsafe { memmap::MmapOptions::new().map(&mut file) }?;

        let mut index = BTreeMap::new();

        let index_start = data_start + (meta.data_len as u64);

        let mut index_data = &mmap[(index_start as usize)..];
        if index_data.len() as u64 != meta.index_len {
            return Err(Error::InvalidData("invalid index length"));
        }

        let kvoffset_encoded_size = KVOffset::encoded_size();

        while index_data.len() > 0 {
            let kvoffset = bincode::deserialize::<KVOffset>(
                index_data
                    .get(..kvoffset_encoded_size)
                    .ok_or(INVALID_DATA)?,
            )?;
            let key_end = kvoffset_encoded_size + kvoffset.key_length as usize;
            let key = index_data
                .get(kvoffset_encoded_size..key_end)
                .ok_or(INVALID_DATA)?;
            let key: &'static [u8] = unsafe { &*(key as *const _) };
            index.insert(key, kvoffset.offset);
            if index_data.len() == key_end {
                break
            }
            index_data = &index_data[key_end..];
        }

        let mmap_buf = &mmap[..];
        let mmap_buf: &'static [u8] = unsafe { &*(mmap_buf as *const _) };

        Ok(MmapSSTableReaderV1_0 {
            mmap: mmap,
            index_start: index_start,
            index: index,
            cache: match cache {
                Some(cache) => Box::new(block_reader::CachingDMABlockManager::new(mmap_buf, cache)),
                None => Box::new(block_reader::DMABlockManager::new(mmap_buf)),
            },
        })
    }
}

impl InnerReader for MmapSSTableReaderV1_0 {
    fn get<'a, 'b>(&'a mut self, key: &'b [u8]) -> Result<Option<GetResult<'a>>> {
        let (offset, right_bound) = match find_bounds(&self.index, key, self.index_start) {
            Some(v) => v,
            None => return Ok(None),
        };

        let block = self.cache.get_block(offset, right_bound)?;
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
    cache: Box<dyn BlockManager>,
    meta: MetaV1_0,
    data_start: u64,
    index: BTreeMap<Vec<u8>, u64>,
}

impl ZlibReaderV1_0 {
    fn new(
        meta: MetaV1_0,
        data_start: u64,
        mut file: File,
        cache: Option<ReadCache>,
    ) -> Result<Self> {
        let index_start = data_start + (meta.data_len as u64);

        file.seek(SeekFrom::Start(index_start))?;

        let file_buf_reader = BufReader::new(file);
        let decoder = flate2::read::ZlibDecoder::new(file_buf_reader);
        let mut buf_decoder = BufReader::new(decoder);
        let mut index = BTreeMap::new();

        loop {
            let kvoffset = KVOffset::deserialize_from_eof_is_ok(&mut buf_decoder)?;
            let kvoffset = match kvoffset {
                Some(kvoffset) => kvoffset,
                None => break,
            };
            let mut key = vec![0; kvoffset.key_length as usize];
            buf_decoder.read_exact(&mut key)?;
            index.insert(key, kvoffset.offset);
        }

        // TODO: check that the index size matches metadata
        let mut file = buf_decoder.into_inner().into_inner().into_inner();
        let mmap = unsafe { memmap::MmapOptions::new().map(&mut file) }?;

        let mmap_buf = &mmap[..];
        let mmap_buf: &'static [u8] = unsafe { &*(mmap_buf as *const _) };

        let zlib_factory = ZlibFactory {};

        Ok(ZlibReaderV1_0 {
            mmap: mmap,
            cache: match cache {
                Some(cache) => Box::new(block_reader::CachingDMAThenReadBlockManager::new(
                    mmap_buf,
                    zlib_factory,
                    cache,
                )),
                None => Box::new(block_reader::DMAThenReadBlockManager::new(
                    mmap_buf,
                    zlib_factory,
                )),
            },
            data_start: data_start,
            meta: meta,
            index: index,
        })
    }
}

impl InnerReader for ZlibReaderV1_0 {
    fn get(&mut self, key: &[u8]) -> Result<Option<GetResult>> {
        let index_start = self.data_start + self.meta.data_len as u64;
        let (offset, right_bound) = match find_bounds(&self.index, key, index_start) {
            Some(v) => v,
            None => return Ok(None),
        };

        let block = self.cache.get_block(offset, right_bound)?;
        let found = block.find_key(key)?;
        Ok(found.map(|v| GetResult::Ref(v)))
    }
}

pub struct SSTableReader {
    inner: Box<dyn InnerReader>,
}

#[derive(Copy, Clone, Debug)]
pub enum ReadCache {
    Blocks(usize),
    Unbounded,
}

#[derive(Copy, Clone, Debug)]
pub struct ReadOptions {
    pub cache: Option<ReadCache>,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            cache: Some(ReadCache::Blocks(32)),
        }
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
            Compression::None => Box::new(MmapSSTableReaderV1_0::new(
                meta, data_start, file, opts.cache,
            )?),
            Compression::Zlib => Box::new(ZlibReaderV1_0::new(meta, data_start, file, opts.cache)?),
        };
        Ok(SSTableReader { inner: inner })
    }
    pub fn get(&mut self, key: &[u8]) -> Result<Option<GetResult>> {
        self.inner.get(key)
    }
}
