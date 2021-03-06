use super::compression::Uncompress;
use super::concurrent_lru::ConcurrentLRUCache;
use super::options::ReadCache;
use super::{error, page_cache, Result};

use bytes::Bytes;
use nix::sys::uio::pread;
use std::convert::TryFrom;
use std::fs::File;
use std::os::unix::io::AsRawFd;
use std::os::unix::io::RawFd;

fn pread_exact(fd: RawFd, mut offset: u64, length: u64) -> Result<Vec<u8>> {
    // if this was mmaped, there will be no truncation.
    #[allow(clippy::cast_possible_truncation)]
    let mut buf = vec![0_u8; length as usize];
    let mut remaining = length;
    while remaining > 0 {
        let size = pread(fd, &mut buf, i64::try_from(offset)?)? as u64;
        if size == 0 {
            return Err(error::INVALID_DATA);
        }
        remaining -= size;
        offset += size;
    }
    Ok(buf)
}

pub trait ConcurrentPageCache {
    fn get_chunk(&self, offset: u64, length: u64) -> Result<Bytes>;
}

impl ConcurrentPageCache for page_cache::StaticBufCache {
    fn get_chunk(&self, offset: u64, length: u64) -> Result<Bytes> {
        // if this was mmaped, there will be no truncation.
        #[allow(clippy::cast_possible_truncation)]
        self.get_buf()
            .get(offset as usize..(offset + length) as usize)
            .map(Bytes::from_static)
            .ok_or(error::INVALID_DATA)
    }
}

pub struct FileBackedPageCache {
    file: File,
    caches: ConcurrentLRUCache,
}

impl FileBackedPageCache {
    pub fn new(file: File, cache: Option<ReadCache>, count: usize) -> Self {
        Self {
            file,
            caches: ConcurrentLRUCache::new(count, cache),
        }
    }
    fn read_chunk(&self, offset: u64, length: u64) -> Result<Bytes> {
        let buf = pread_exact(self.file.as_raw_fd(), offset, length)?;
        Ok(Bytes::from(buf))
    }
}

impl ConcurrentPageCache for FileBackedPageCache {
    fn get_chunk(&self, offset: u64, length: u64) -> Result<Bytes> {
        self.caches
            .get_or_insert(offset, || self.read_chunk(offset, length))
    }
}

pub struct WrappedCache<PC, U> {
    inner: PC,
    caches: ConcurrentLRUCache,
    uncompress: U,
}

impl<PC, U> WrappedCache<PC, U> {
    pub fn new(inner: PC, uncompress: U, cache: Option<ReadCache>, count: usize) -> Self {
        Self {
            inner,
            caches: ConcurrentLRUCache::new(count, cache),
            uncompress,
        }
    }
}

impl ConcurrentPageCache for Box<dyn ConcurrentPageCache + Send + Sync> {
    fn get_chunk(&self, offset: u64, length: u64) -> Result<Bytes> {
        self.as_ref().get_chunk(offset, length)
    }
}

impl<PC, U> ConcurrentPageCache for WrappedCache<PC, U>
where
    U: Uncompress,
    PC: ConcurrentPageCache,
{
    fn get_chunk(&self, offset: u64, length: u64) -> Result<Bytes> {
        self.caches.get_or_insert(offset, || {
            let uncompressed = self.inner.get_chunk(offset, length)?;
            let buf = self.uncompress.uncompress(&uncompressed)?;
            let bytes = Bytes::from(buf);
            Ok(bytes)
        })
    }
}
