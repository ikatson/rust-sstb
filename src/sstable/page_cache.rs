use std::io::{Read, Seek, SeekFrom, Cursor};

use lru::LruCache;
use super::{Result, error, reader};

pub trait PageCache {
    fn get_chunk(&mut self, offset: u64, length: u64) -> Result<&[u8]>;
}

pub struct StaticBufCache {
    buf: &'static [u8]
}

impl StaticBufCache {
    pub fn new(buf: &'static [u8]) -> Self {
        Self{buf: buf}
    }
}

impl PageCache for StaticBufCache {
    fn get_chunk(&mut self, offset: u64, length: u64) -> Result<&[u8]> {
        dbg!("StaticBufCache:get_chunk");
        self.buf.get(offset as usize..(offset+length) as usize).ok_or(error::INVALID_DATA)
    }
}

pub struct ReadCache<R> {
    reader: R,
    cache: LruCache<u64, Vec<u8>>,
}

impl<R> ReadCache<R> {
    pub fn new(reader: R, cache: reader::ReadCache) -> Self {
        Self{
            reader: reader,
            cache: match cache {
                reader::ReadCache::Unbounded => LruCache::unbounded(),
                reader::ReadCache::Blocks(b) => LruCache::new(b)
            }
        }
    }
}

impl<R: Read + Seek> PageCache for ReadCache<R> {
    fn get_chunk(&mut self, offset: u64, length: u64) -> Result<&[u8]> {
        match self.cache.get(&offset) {
            Some(bytes) => Ok(unsafe {&*(bytes as &[u8] as *const [u8])}),
            None => {
                let mut buf = vec![0; length as usize];
                self.reader.seek(SeekFrom::Start(offset))?;
                self.reader.read_exact(&mut buf)?;
                self.cache.put(offset, buf);
                Ok(self.cache.get(&offset).unwrap())
            }
        }
    }
}

pub struct WrappedCache<PC, U> {
    inner: PC,
    cache: LruCache<u64, Vec<u8>>,
    uncompress: U,
}

impl<PC, U> WrappedCache<PC, U> {
    pub fn new(inner: PC, uncompress: U, cache: reader::ReadCache) -> Self {
        Self{
            inner: inner,
            cache: cache.lru(),
            uncompress: uncompress,
        }
    }
}

pub trait Uncompress {
    fn uncompress(&self, buf: &[u8]) -> Result<Vec<u8>>;
}

// TODO: put all compressors/decompressors under generic encoder/decoder API.
pub struct ZlibUncompress {}

impl Uncompress for ZlibUncompress {
    fn uncompress(&self, buf: &[u8]) -> Result<Vec<u8>> {
        let mut dec = flate2::read::ZlibDecoder::new(Cursor::new(buf));
        // TODO: buf.len() here is a bad heuristic. Need the real number, this can be pulled during
        // compression.
        let mut buf = Vec::with_capacity(buf.len());
        dec.read_to_end(&mut buf)?;
        Ok(buf)
    }
}

impl PageCache for Box<dyn PageCache> {
    fn get_chunk(&mut self, offset: u64, length: u64) -> Result<&[u8]> {
        self.get_chunk(offset, length)
    }
}


impl<PC, U> PageCache for WrappedCache<PC, U>
    where U: Uncompress,
          PC: PageCache,
{
    fn get_chunk(&mut self, offset: u64, length: u64) -> Result<&[u8]> {
        dbg!("get_chunk", offset, length);
        match self.cache.get(&offset) {
            Some(bytes) => Ok(unsafe {&*(bytes as &[u8] as *const [u8])}),
            None => {
                dbg!("inner get_chunk", offset, length);
                let inner_chunk = self.inner.get_chunk(offset, length)?;
                dbg!("inner get_chunk done", offset, length);
                let buf = self.uncompress.uncompress(inner_chunk)?;
                self.cache.put(offset, buf);
                Ok(self.cache.get(&offset).unwrap())
            }
        }
    }
}