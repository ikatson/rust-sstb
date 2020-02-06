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
        return Self {
            bitmap_size: 1_000_000,
            items_count: 1_000_000,
        };
    }
}

/// Options for writing sstables.
#[derive(Debug, Copy, Clone)]
pub struct WriteOptions {
    /// Compression to use. The default is None.
    pub compression: Compression,
    /// How often to store the records in the index.
    pub flush_every: usize,
    /// Options for the bloom filter.
    pub bloom: BloomConfig,
}

impl WriteOptions {
    pub fn new() -> Self {
        Self::default()
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
}

impl Default for WriteOptions {
    fn default() -> Self {
        WriteOptions {
            compression: Compression::None,
            flush_every: 4096,
            bloom: BloomConfig::default(),
        }
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
    // Set if you want to use bloom filters during lookups.
    // This has a performance penalty for positive lookups,
    // but if you have a lot of maybe-negative, it should make things faster.
    pub use_bloom: bool,
}

impl ReadOptions {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn cache(&mut self, cache: Option<ReadCache>) -> &mut Self {
        self.cache = cache;
        self
    }
    pub fn use_mmap(&mut self, use_mmap: bool) -> &mut Self {
        self.use_mmap = use_mmap;
        self
    }
    pub fn use_bloom(&mut self, use_bloom: bool) -> &mut Self {
        self.use_bloom = use_bloom;
        self
    }
    pub fn thread_buckets(&mut self, thread_buckets: Option<usize>) -> &mut Self {
        self.thread_buckets = thread_buckets;
        self
    }
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            cache: Some(ReadCache::default()),
            use_mmap: true,
            thread_buckets: Some(num_cpus::get()),
            use_bloom: true,
        }
    }
}

/// Options for "get" method.
#[derive(Copy, Clone, Debug)]
pub struct GetOptions {
    /// Set this if you want to use the bloom filter to speed
    /// up negative lookups at a cost for positive lookup.
    pub use_bloom: bool,
}

impl GetOptions {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn use_bloom(&mut self, use_bloom: bool) -> &mut Self {
        self.use_bloom = use_bloom;
        self
    }
}

impl Default for GetOptions {
    fn default() -> Self {
        Self {
            use_bloom: true,
        }
    }
}
