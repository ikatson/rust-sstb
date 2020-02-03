use super::compression::Uncompress;
use std::collections::hash_map::DefaultHasher;
use super::{error, reader, Result, page_cache};
use std::hash::{Hash, Hasher};
use parking_lot::Mutex;
use lru::LruCache;

use bytes::Bytes;

use std::os::unix::io::RawFd;
use nix::sys::uio::pread;
use std::os::unix::io::AsRawFd;
use std::fs::File;

fn pread_exact(fd: RawFd, mut offset: u64, length: u64) -> Result<Vec<u8>> {
    let mut buf = vec![0u8; length as usize];
    let mut remaining = length;
    while remaining > 0 {
        let size = pread(fd, &mut buf, offset as i64)? as u64;
        if size == 0 {
            return Err(error::INVALID_DATA)
        }
        remaining -= size;
        offset += size;
    }
    Ok(buf)
}

pub trait TSPageCache {
    fn get_chunk(&self, offset: u64, length: u64) -> Result<Bytes>;
}

impl TSPageCache for page_cache::StaticBufCache {
    fn get_chunk(&self, offset: u64, length: u64) -> Result<Bytes> {
        self.get_buf()
            .get(offset as usize..(offset + length) as usize)
            .map(Bytes::from_static)
            .ok_or(error::INVALID_DATA)
    }
}

pub struct FileBackedPageCache {
    file: File,
    caches: Vec<Mutex<LruCache<u64, Bytes>>>,
}

impl FileBackedPageCache {
    pub fn new(file: File, cache: reader::ReadCache, count: usize) -> Self {
        Self{
            file: file,
            caches: core::iter::repeat_with(|| Mutex::new(cache.lru())).take(count).collect(),
        }
    }
    fn read_chunk(&self, offset: u64, length: u64) -> Result<Bytes> {
        let buf = pread_exact(self.file.as_raw_fd(), offset, length)?;
        Ok(Bytes::from(buf))
    }
}

impl TSPageCache for FileBackedPageCache {
    fn get_chunk(&self, offset: u64, length: u64) -> Result<Bytes> {
        let mut hasher = DefaultHasher::new();
        offset.hash(&mut hasher);
        let hash = hasher.finish() as usize;
        let idx = hash % self.caches.len();

        let mut lru = unsafe {self.caches.get_unchecked(idx)}.lock();
        match lru.get(&offset) {
            Some(bytes) => Ok(bytes.clone()),
            None => {
                let bytes = self.read_chunk(offset, length)?;
                lru.put(offset, bytes.clone());
                Ok(bytes)
            }
        }
    }
}

pub struct WrappedCache<PC, U> {
    inner: PC,
    caches: Vec<Mutex<LruCache<u64, Bytes>>>,
    uncompress: U,
}

impl<PC, U> WrappedCache<PC, U> {
    pub fn new(inner: PC, uncompress: U, cache: reader::ReadCache, count: usize) -> Self {
        Self {
            inner: inner,
            caches: core::iter::repeat_with(|| Mutex::new(cache.lru())).take(count).collect(),
            uncompress: uncompress,
        }
    }
}

impl TSPageCache for Box<dyn TSPageCache + Send + Sync> {
    fn get_chunk(&self, offset: u64, length: u64) -> Result<Bytes> {
        self.as_ref().get_chunk(offset, length)
    }
}

impl<PC, U> TSPageCache for WrappedCache<PC, U>
where
    U: Uncompress,
    PC: TSPageCache,
{
    fn get_chunk(&self, offset: u64, length: u64) -> Result<Bytes> {
        let mut hasher = DefaultHasher::new();
        offset.hash(&mut hasher);
        let hash = hasher.finish() as usize;
        let idx = hash % self.caches.len();

        let mut lru = unsafe {self.caches.get_unchecked(idx)}.lock();
        match lru.get(&offset) {
            Some(bytes) => Ok(bytes.clone()),
            None => {
                let uncompressed = self.inner.get_chunk(offset, length)?;
                let buf = self.uncompress.uncompress(&uncompressed)?;
                let bytes = Bytes::from(buf);
                lru.put(offset, bytes.clone());
                Ok(bytes)
            }
        }
    }
}