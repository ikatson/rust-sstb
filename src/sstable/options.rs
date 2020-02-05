use super::types::Compression;

use lru::LruCache;

// The configuration for the bloom filter.
#[derive(Debug, Copy, Clone)]
pub struct BloomConfig {
    pub bitmap_size: usize,
    pub items_count: usize,
}

impl Default for BloomConfig {
    fn default() -> Self {
        return Self{
            bitmap_size: 1_000_000,
            items_count: 1_000_000,
        }
    }
}

/// Options for writing sstables.
#[derive(Debug, Copy, Clone)]
pub struct WriteOptions {
    pub compression: Compression,
    pub flush_every: usize,
    pub bloom: BloomConfig,
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
            bloom: BloomConfig::default()
        }
    }
}

/// The builder for `WriteOptions`
pub struct WriteOptionsBuilder {
    /// Compression to use. The default is None.
    pub compression: Compression,
    /// How often to store the records in the index.
    pub flush_every: usize,
    pub bloom: BloomConfig,
}

impl WriteOptionsBuilder {
    pub fn new() -> Self {
        let default = WriteOptions::default();
        Self {
            compression: default.compression,
            flush_every: default.flush_every,
            bloom: default.bloom,
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
    pub fn bloom(&mut self, bloom: BloomConfig) -> &mut Self {
        self.bloom = bloom;
        self
    }
    pub fn build(&self) -> WriteOptions {
        WriteOptions {
            compression: self.compression,
            flush_every: self.flush_every,
            bloom: self.bloom,
        }
    }
}

impl Default for WriteOptionsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Configures the caches for reading.
#[derive(Copy, Clone, Debug)]
pub enum ReadCache {
    // How many chunks(blocks) to store in LRU.
    Blocks(usize),
    // Unbounded cache, the default.
    Unbounded,
}

impl ReadCache {
    pub fn lru<K, V>(&self) -> LruCache<K, V>
    where
        K: std::cmp::Eq + std::hash::Hash,
    {
        match self {
            Self::Blocks(b) => LruCache::new(*b),
            Self::Unbounded => LruCache::unbounded(),
        }
    }
}

impl Default for ReadCache {
    fn default() -> Self {
        Self::Unbounded
    }
}

/// A builder for `ReadOptions`
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

impl Default for ReadOptionsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Options for reading sstables.
#[derive(Copy, Clone, Debug)]
pub struct ReadOptions {
    /// The caching strategy to use.
    pub cache: Option<ReadCache>,
    /// If mmap can be used for reading the sstable from disk.
    pub use_mmap: bool,
    /// How many buckets to split the caches into for efficient
    /// thread-safe access.
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
