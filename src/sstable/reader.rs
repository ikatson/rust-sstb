use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::Path;

use bincode;
use memmap;
use num_cpus;

use super::*;

use bytes::Bytes;
use lru::LruCache;

enum MetaData {
    V1_0(MetaV1_0),
}

struct MetaResult {
    meta: MetaData,
    offset: usize,
}

fn read_metadata<B: Read + Seek>(mut file: B) -> Result<MetaResult> {
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
    let mut file = reader.into_inner().into_inner();
    file.seek(SeekFrom::Start(offset as u64))?;

    Ok(MetaResult {
        meta: meta,
        offset: offset,
    })
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

trait Index {
    fn find_bounds(&self, key: &[u8], end_default: u64) -> Option<(u64, u64)>;
}

struct MemIndex {
    index: BTreeMap<&'static [u8], u64>,
}

impl MemIndex {
    fn from_static_buf(buf: &'static [u8], expected_len: u64) -> Result<Self> {
        // Build the index from mmap here.
        let mut index = BTreeMap::new();
        let mut index_data = &buf[..];
        if index_data.len() as u64 != expected_len {
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
                break;
            }
            index_data = &index_data[key_end..];
        }

        Ok(Self { index: index })
    }
}

impl Index for MemIndex {
    fn find_bounds(&self, key: &[u8], end_default: u64) -> Option<(u64, u64)> {
        find_bounds(&self.index, key, end_default)
    }
}

struct OwnedIndex {
    index: BTreeMap<Vec<u8>, u64>,
}

impl OwnedIndex {
    fn from_reader<R: Read>(mut reader: R) -> Result<Self> {
        let mut index = BTreeMap::new();

        loop {
            let kvoffset = KVOffset::deserialize_from_eof_is_ok(&mut reader)?;
            let kvoffset = match kvoffset {
                Some(kvoffset) => kvoffset,
                None => break,
            };
            let mut key = vec![0; kvoffset.key_length as usize];
            reader.read_exact(&mut key)?;
            index.insert(key, kvoffset.offset);
        }
        Ok(Self { index: index })
    }
}

impl Index for OwnedIndex {
    fn find_bounds(&self, key: &[u8], end_default: u64) -> Option<(u64, u64)> {
        find_bounds(&self.index, key, end_default)
    }
}

pub struct SSTableReader {
    inner: InnerReader,
}

#[derive(Copy, Clone, Debug)]
pub enum ReadCache {
    Blocks(usize),
    Unbounded,
}

impl ReadCache {
    pub fn lru<K, V>(&self) -> LruCache<K, V>
    where
        K: std::cmp::Eq + std::hash::Hash,
    {
        match self {
            ReadCache::Blocks(b) => LruCache::new(*b),
            ReadCache::Unbounded => LruCache::unbounded(),
        }
    }
}

impl Default for ReadCache {
    fn default() -> Self {
        ReadCache::Unbounded
    }
}

#[derive(Copy, Clone, Debug)]
pub struct ReadOptions {
    pub cache: Option<ReadCache>,
    pub use_mmap: bool,
    pub thread_buckets: Option<usize>,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            cache: Some(ReadCache::default()),
            use_mmap: true,
            thread_buckets: Some(num_cpus::get()),
        }
    }
}

struct InnerReader {
    index: Box<dyn Index>,
    // This is just to hold an mmap reference to be dropped in the end.
    _mmap: Option<memmap::Mmap>,
    page_cache: Box<dyn page_cache::PageCache>,
    meta: MetaV1_0,
    data_start: u64,
}

impl InnerReader {
    pub fn new(
        mut file: File,
        data_start: u64,
        meta: MetaResult,
        opts: &ReadOptions,
    ) -> Result<Self> {
        let meta = match meta.meta {
            MetaData::V1_0(meta) => meta,
        };

        let index_start = data_start + (meta.data_len as u64);

        file.seek(SeekFrom::Start(index_start))?;

        let mmap = if opts.use_mmap {
            Some(unsafe { memmap::Mmap::map(&file) }?)
        } else {
            None
        };
        let mmap_buf = mmap.as_ref().map(|mmap| {
            let buf = &mmap as &[u8];
            let buf = buf as *const [u8];
            let buf: &'static [u8] = unsafe { &*buf };
            buf
        });

        let index: Box<dyn Index> = match meta.compression {
            Compression::None => match mmap_buf {
                Some(mmap) => Box::new(MemIndex::from_static_buf(
                    &mmap[index_start as usize..],
                    meta.index_len,
                )?),
                None => Box::new(OwnedIndex::from_reader(&mut file)?),
            },
            Compression::Zlib => {
                // does not make sense to use mmap for index as we are not going to access
                // the pages anyway.
                let reader = flate2::read::ZlibDecoder::new(&mut file);
                Box::new(OwnedIndex::from_reader(reader)?)
            }
            Compression::Snappy => {
                let reader = snap::Reader::new(&mut file);
                Box::new(OwnedIndex::from_reader(reader)?)
            }
        };

        let pc: Box<dyn page_cache::PageCache> = match mmap_buf {
            Some(mmap) => Box::new(page_cache::StaticBufCache::new(mmap)),
            None => Box::new(page_cache::ReadCache::new(
                file,
                opts.cache.clone().unwrap_or(ReadCache::default()),
            )),
        };

        let uncompressed_cache: Box<dyn page_cache::PageCache> = match meta.compression {
            Compression::None => pc,
            Compression::Zlib => {
                let dec = compression::ZlibUncompress {};
                let cache = opts.cache.clone().unwrap_or(ReadCache::default());
                let wrapped = page_cache::WrappedCache::new(pc, dec, cache);
                Box::new(wrapped)
            }
            Compression::Snappy => {
                let dec = compression::SnappyUncompress {};
                let cache = opts.cache.clone().unwrap_or(ReadCache::default());
                let wrapped = page_cache::WrappedCache::new(pc, dec, cache);
                Box::new(wrapped)
            }
        };

        return Ok(Self {
            _mmap: mmap,
            index: index,
            page_cache: uncompressed_cache,
            data_start: data_start,
            meta: meta,
        });
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<&[u8]>> {
        let index_start = self.data_start + self.meta.data_len as u64;
        let (offset, right_bound) = match self.index.find_bounds(key, index_start) {
            Some(v) => v,
            None => return Ok(None),
        };

        let chunk = self.page_cache.get_chunk(offset, right_bound - offset)?;
        let block = block_reader::ReferenceBlock::new(chunk);
        let found = block.find_key_rb(key)?;
        Ok(found)
    }
}

struct ThreadSafeInnerReader {
    index: Box<dyn Index + Sync + Send>,
    // This is just to hold an mmap reference to be dropped in the end.
    _mmap: Option<memmap::Mmap>,
    page_cache: Box<dyn thread_safe_page_cache::TSPageCache + Sync + Send>,
    meta: MetaV1_0,
    data_start: u64,
}

impl ThreadSafeInnerReader {
    pub fn new(
        mut file: File,
        data_start: u64,
        meta: MetaResult,
        opts: &ReadOptions,
    ) -> Result<Self> {
        let meta = match meta.meta {
            MetaData::V1_0(meta) => meta,
        };

        let index_start = data_start + (meta.data_len as u64);

        file.seek(SeekFrom::Start(index_start))?;

        let mmap = if opts.use_mmap {
            Some(unsafe { memmap::Mmap::map(&file) }?)
        } else {
            None
        };
        let mmap_buf = mmap.as_ref().map(|mmap| {
            let buf = &mmap as &[u8];
            let buf = buf as *const [u8];
            let buf: &'static [u8] = unsafe { &*buf };
            buf
        });

        let index: Box<dyn Index + Send + Sync> = match meta.compression {
            Compression::None => match mmap_buf {
                Some(mmap) => Box::new(MemIndex::from_static_buf(
                    &mmap[index_start as usize..],
                    meta.index_len,
                )?),
                None => Box::new(OwnedIndex::from_reader(&mut file)?),
            },
            Compression::Zlib => {
                // does not make sense to use mmap for index as we are not going to access
                // the pages anyway.
                let reader = flate2::read::ZlibDecoder::new(&mut file);
                Box::new(OwnedIndex::from_reader(reader)?)
            }
            Compression::Snappy => {
                let reader = snap::Reader::new(&mut file);
                Box::new(OwnedIndex::from_reader(reader)?)
            }
        };

        let num_cpus = opts.thread_buckets.unwrap_or_else(|| num_cpus::get());

        let pc: Box<dyn thread_safe_page_cache::TSPageCache + Send + Sync> = match mmap_buf {
            Some(mmap) => Box::new(page_cache::StaticBufCache::new(mmap)),
            None => Box::new(thread_safe_page_cache::FileBackedPageCache::new(
                file,
                opts.cache.clone().unwrap_or(ReadCache::default()),
                num_cpus,
            )),
        };

        let uncompressed_cache: Box<dyn thread_safe_page_cache::TSPageCache + Send + Sync> = match meta.compression {
            Compression::None => pc,
            Compression::Zlib => {
                let dec = compression::ZlibUncompress {};
                let cache = opts.cache.clone().unwrap_or(ReadCache::default());
                let wrapped = thread_safe_page_cache::WrappedCache::new(pc, dec, cache, num_cpus);
                Box::new(wrapped)
            }
            Compression::Snappy => {
                let dec = compression::SnappyUncompress {};
                let cache = opts.cache.clone().unwrap_or(ReadCache::default());
                let wrapped = thread_safe_page_cache::WrappedCache::new(pc, dec, cache, num_cpus);
                Box::new(wrapped)
            }
        };

        return Ok(Self {
            _mmap: mmap,
            index: index,
            page_cache: uncompressed_cache,
            data_start: data_start,
            meta: meta,
        });
    }

    fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let index_start = self.data_start + self.meta.data_len as u64;
        let (offset, right_bound) = match self.index.find_bounds(key, index_start) {
            Some(v) => v,
            None => return Ok(None),
        };

        let chunk: Bytes = self.page_cache.get_chunk(offset, right_bound - offset)?;
        if let Some((start, end)) = block_reader::find_key_offset(&chunk, key)? {
            Ok(Some(chunk.slice(start..end)))
        } else {
            Ok(None)
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
        let inner = InnerReader::new(file, data_start, meta, opts)?;
        Ok(SSTableReader { inner: inner })
    }
    pub fn get(&mut self, key: &[u8]) -> Result<Option<&[u8]>> {
        self.inner.get(key)
    }
}

pub struct ThreadSafeSSTableReader {
    inner: ThreadSafeInnerReader
}

impl ThreadSafeSSTableReader {
    pub fn new<P: AsRef<Path>>(filename: P) -> Result<Self> {
        Self::new_with_options(filename, &ReadOptions::default())
    }

    pub fn new_with_options<P: AsRef<Path>>(filename: P, opts: &ReadOptions) -> Result<Self> {
        let mut file = File::open(filename)?;
        let meta = read_metadata(&mut file)?;
        let data_start = meta.offset as u64;
        let inner = ThreadSafeInnerReader::new(file, data_start, meta, opts)?;
        Ok(Self { inner: inner })
    }
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }
}

pub struct MmapUncompressedSSTableReader {
    index_start: u64,
    mmap: memmap::Mmap,
    index: MemIndex,
}

impl MmapUncompressedSSTableReader {
    pub fn new<P: AsRef<Path>>(filename: P) -> Result<Self> {
        let mut file = File::open(filename)?;
        let meta = read_metadata(&mut file)?;
        let data_start = meta.offset as u64;

        let meta = match meta.meta {
            MetaData::V1_0(meta) => meta,
        };

        if meta.compression != Compression::None {
            return Err(Error::ProgrammingError(
                "cannot use MmapUncompressedSSTableReader with this file",
            ));
        }

        let index_start = data_start + (meta.data_len as u64);

        file.seek(SeekFrom::Start(index_start))?;
        let mmap = unsafe { memmap::Mmap::map(&file) }?;
        let mmap_buf = {
            let buf = &mmap as &[u8];
            let buf = buf as *const [u8];
            let buf: &'static [u8] = unsafe { &*buf };
            buf
        };

        let index = MemIndex::from_static_buf(&mmap_buf[index_start as usize..], meta.index_len)?;
        return Ok(Self {
            mmap: mmap,
            index: index,
            index_start: index_start,
        });
    }
    pub fn get<'a, 'b>(&'a self, key: &'b [u8]) -> Result<Option<&'a [u8]>> {
        let (offset, right_bound) = match self.index.find_bounds(key, self.index_start) {
            Some(v) => v,
            None => return Ok(None),
        };
        let block =
            block_reader::ReferenceBlock::new(&self.mmap[offset as usize..right_bound as usize]);
        let found = block.find_key_rb(key)?;
        Ok(found)
    }
}
