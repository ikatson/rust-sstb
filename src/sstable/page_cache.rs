use std::io::{Read, Seek, SeekFrom};
use std::convert::TryFrom;

use super::compression::Uncompress;
use super::{error, Result};
use super::options::ReadCache;

use lru::LruCache;

pub trait PageCache {
    fn get_chunk(&mut self, offset: u64, length: u64) -> Result<&[u8]>;
}

pub struct StaticBufCache {
    buf: &'static [u8],
}

impl StaticBufCache {
    pub fn new(buf: &'static [u8]) -> Self {
        Self { buf }
    }
    pub fn get_buf(&self) -> &'static [u8] {
        self.buf
    }
}

impl PageCache for StaticBufCache {
    fn get_chunk(&mut self, offset: u64, length: u64) -> Result<&[u8]> {
        // if this was mmaped, there will be no truncation.
        #[allow(clippy::cast_possible_truncation)]
        self.buf
            .get(offset as usize..(offset + length) as usize)
            .ok_or(error::INVALID_DATA)
    }
}

pub struct ReadPageCache<R> {
    reader: R,
    cache: LruCache<u64, Vec<u8>>,
}

impl<R> ReadPageCache<R> {
    pub fn new(reader: R, cache: ReadCache) -> Self {
        Self {
            reader,
            cache: cache.lru(),
        }
    }
}

impl<R: Read + Seek> PageCache for ReadPageCache<R> {
    fn get_chunk(&mut self, offset: u64, length: u64) -> Result<&[u8]> {
        match self.cache.get(&offset) {
            Some(bytes) => Ok(unsafe { &*(bytes as &[u8] as *const [u8]) }),
            None => {
                let mut buf = vec![0; usize::try_from(length)?];
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
    pub fn new(inner: PC, uncompress: U, cache: ReadCache) -> Self {
        Self {
            inner,
            cache: cache.lru(),
            uncompress,
        }
    }
}

impl PageCache for Box<dyn PageCache> {
    fn get_chunk(&mut self, offset: u64, length: u64) -> Result<&[u8]> {
        self.as_mut().get_chunk(offset, length)
    }
}

impl<PC, U> PageCache for WrappedCache<PC, U>
where
    U: Uncompress,
    PC: PageCache,
{
    fn get_chunk(&mut self, offset: u64, length: u64) -> Result<&[u8]> {
        match self.cache.get(&offset) {
            Some(bytes) => Ok(unsafe { &*(bytes as &[u8] as *const [u8]) }),
            None => {
                let inner_chunk = self.inner.get_chunk(offset, length)?;
                let buf = self.uncompress.uncompress(inner_chunk)?;
                self.cache.put(offset, buf);
                Ok(self.cache.get(&offset).unwrap())
            }
        }
    }
}
