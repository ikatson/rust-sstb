use super::types::Compression;

use lru::LruCache;

#[derive(Debug, Copy, Clone)]
pub struct WriteOptions {
    pub compression: Compression,
    pub flush_every: usize,
}

impl WriteOptions {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn builder() -> WriteOptionsBuilder {
        WriteOptionsBuilder::new()
    }
}

impl Default for WriteOptions {
    fn default() -> Self {
        WriteOptions {
            compression: Compression::None,
            flush_every: 4096,
        }
    }
}

pub struct WriteOptionsBuilder {
    pub compression: Compression,
    pub flush_every: usize,
}

impl WriteOptionsBuilder {
    pub fn new() -> Self {
        let default = WriteOptions::default();
        Self {
            compression: default.compression,
            flush_every: default.flush_every,
        }
    }
    pub fn compression(&mut self, compression: Compression) -> &mut Self {
        self.compression = compression;
        self
    }
    pub fn flush_every(&mut self, flush_every: usize) -> &mut Self {
        self.flush_every = flush_every;
        self
    }
    pub fn build(&self) -> WriteOptions {
        WriteOptions {
            compression: self.compression,
            flush_every: self.flush_every,
        }
    }
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

pub struct ReadOptionsBuilder {
    pub cache: Option<ReadCache>,
    pub use_mmap: bool,
    pub thread_buckets: Option<usize>,
}

impl ReadOptionsBuilder {
    pub fn new() -> Self {
        let default = ReadOptions::default();
        Self {
            cache: default.cache,
            use_mmap: default.use_mmap,
            thread_buckets: default.thread_buckets,
        }
    }
    pub fn cache(&mut self, cache: Option<ReadCache>) -> &mut Self {
        self.cache = cache;
        self
    }
    pub fn use_mmap(&mut self, use_mmap: bool) -> &mut Self {
        self.use_mmap = use_mmap;
        self
    }
    pub fn thread_buckets(&mut self, thread_buckets: Option<usize>) -> &mut Self {
        self.thread_buckets = thread_buckets;
        self
    }
    pub fn build(&self) -> ReadOptions {
        ReadOptions {
            cache: self.cache,
            use_mmap: self.use_mmap,
            thread_buckets: self.thread_buckets,
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub struct ReadOptions {
    pub cache: Option<ReadCache>,
    pub use_mmap: bool,
    pub thread_buckets: Option<usize>,
}

impl ReadOptions {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn builder() -> ReadOptionsBuilder {
        ReadOptionsBuilder::new()
    }
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
