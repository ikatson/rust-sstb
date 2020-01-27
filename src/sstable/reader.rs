use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Cursor, Read, Seek, SeekFrom};
use std::path::Path;

use bincode;
use memmap;

use lru::LruCache;
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

struct MmapSSTableReaderV1_0 {
    mmap: memmap::Mmap,
    index_start: u64,
    // it's not &'static in reality, but it's bound to mmap's lifetime.
    // It will NOT work with compression.
    index: BTreeMap<&'static str, usize>,
    cache: block_reader::DirectMemoryAccessBlockManager<'static>,
}

impl MmapSSTableReaderV1_0 {
    fn new(meta: MetaV1_0, data_start: u64, mut file: File) -> Result<Self> {
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
            index.insert(key, value.0 as usize);
        }

        let mmap_buf = &mmap[..];
        let mmap_buf: &'static [u8] = unsafe {&* (mmap_buf as *const _)};

        Ok(MmapSSTableReaderV1_0 {
            mmap: mmap,
            index_start: index_start,
            index: index,
            cache: block_reader::DirectMemoryAccessBlockManager::new(mmap_buf, 32)
        })
    }
}

impl InnerReader for MmapSSTableReaderV1_0 {
    fn get<'a, 'b>(&'a mut self, key: &'b str) -> Result<Option<GetResult<'a>>> {
        use std::ops::Bound;

        let start = {
            let mut iter_left = self
                .index
                .range::<&str, _>((Bound::Unbounded, Bound::Included(key)));
            let closest_left = iter_left.next_back();
            match closest_left {
                Some((_, offset)) => *offset,
                None => return Ok(None),
            }
        };

        let end = {
            let mut iter_right = self
                .index
                .range::<&str, _>((Bound::Excluded(key), Bound::Unbounded));
            let closest_right = iter_right.next_back();
            match closest_right {
                Some((_, offset)) => *offset,
                None => self.index_start as usize,
            }
        };

        // let mut data = &self.mmap[self.data_start as usize..self.index_start as usize];
        // let data = &self.mmap[offset..right_bound];
        let block = self.cache.get_block(start as u64, end as u64)?;
        let found = block.find_key(key)?;
        Ok(found.map(|v| GetResult::Ref(v)))
        // let found = block.find_key(key)?;
        // return Ok(found.map(|v| GetResult::Ref(unsafe {&* (v as *const _)})))
    }
}

struct ZlibReaderV1_0 {
    mmap: memmap::Mmap,
    meta: MetaV1_0,
    data_start: u64,
    index: BTreeMap<String, u64>,
    block_cache: LruCache<u64, Vec<u8>>,
}

fn find_value<'a, 'b>(mut data: &'a [u8], key: &'b str) -> Result<Option<&'a [u8]>> {
    let value_length_encoded_size = bincode::serialized_size(&Length(0))? as usize;

    while data.len() > 0 {
        let key_end = match memchr::memchr(b'\0', data) {
            Some(idx) => idx as usize,
            None => return Err(Error::InvalidData("corrupt or buggy sstable")),
        };
        let start_key = std::str::from_utf8(&data[..key_end])?;
        data = &data[key_end + 1..];
        let value_length = bincode::deserialize::<Length>(data)?.0 as usize;
        data = &data[value_length_encoded_size..];
        let value = &data[..value_length];
        if value.len() != value_length {
            return Err(Error::InvalidData("corrupt or buggy sstable"));
        }
        if key == start_key {
            return Ok(Some(value));
        }
        data = &data[value_length..];
    }
    return Ok(None);
}

impl ZlibReaderV1_0 {
    fn new(meta: MetaV1_0, data_start: u64, mut file: File, cache_size: usize) -> Result<Self> {
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
        Ok(ZlibReaderV1_0 {
            mmap: mmap,
            data_start: data_start,
            meta: meta,
            index: index,
            block_cache: LruCache::new(cache_size),
        })
    }

    fn read_block(&mut self, offset: u64, right_bound: u64) -> Result<Vec<u8>> {
        dbg!("reading block", offset);
        let cursor = Cursor::new(&self.mmap[offset as usize..right_bound as usize]);
        let zreader = flate2::read::ZlibDecoder::new(cursor);
        let mut zreader = BufReader::new(zreader);
        let mut buf = Vec::new();
        zreader.read_to_end(&mut buf)?;
        Ok(buf)
    }

    fn read_block_cached(&mut self, offset: u64, right_bound: u64) -> Result<&Vec<u8>> {
        match self.block_cache.get(&offset) {
            // this is safe, this is to avoids the borrow checker lifetime issue.
            Some(v) => Ok(unsafe { &*(v as *const _) }),
            None => {
                let block = self.read_block(offset, right_bound)?;
                self.block_cache.put(offset, block);
                Ok(self.block_cache.get(&offset).unwrap())
            }
        }
    }
}

impl InnerReader for ZlibReaderV1_0 {
    fn get(&mut self, key: &str) -> Result<Option<GetResult>> {
        use std::ops::Bound;

        let offset = {
            let mut iter_left = self
                .index
                .range::<str, _>((Bound::Unbounded, Bound::Included(key)));
            let closest_left = iter_left.next_back();
            match closest_left {
                Some((_, offset)) => *offset,
                None => return Ok(None),
            }
        };

        let index_start = self.data_start + self.meta.data_len as u64;

        let right_bound = {
            let mut iter_right = self
                .index
                .range::<str, _>((Bound::Excluded(key), Bound::Unbounded));
            let closest_right = iter_right.next_back();
            match closest_right {
                Some((_, offset)) => *offset,
                None => index_start,
            }
        };

        // let mut data = &self.mmap[self.data_start as usize..self.index_start as usize];
        let block = self.read_block_cached(offset, right_bound)?;

        return find_value(block, key).map(|v| v.map(|v| GetResult::Ref(v)));
    }
}

pub struct SSTableReader {
    inner: Box<dyn InnerReader>,
}

impl SSTableReader {
    pub fn new<P: AsRef<Path>>(filename: P) -> Result<Self> {
        let mut file = File::open(filename)?;
        let meta = read_metadata(&mut file)?;
        let data_start = meta.offset as u64;
        let meta = match meta.meta {
            MetaData::V1_0(meta) => meta,
        };
        // dbg!(&meta, data_start);
        let inner: Box<dyn InnerReader> = match meta.compression {
            Compression::None => Box::new(MmapSSTableReaderV1_0::new(meta, data_start, file)?),
            // TODO: 1024 - make this configurable, and be tied to memory instead.
            Compression::Zlib => Box::new(ZlibReaderV1_0::new(meta, data_start, file, 32)?),
        };
        Ok(SSTableReader { inner: inner })
    }
    pub fn get(&mut self, key: &str) -> Result<Option<GetResult>> {
        self.inner.get(key)
    }
}
