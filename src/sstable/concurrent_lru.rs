use super::{ReadCache, Result};
use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

struct Inner {
    value: RwLock<Option<Bytes>>,
}

impl Inner {
    fn new() -> Self {
        Self {
            value: RwLock::new(None),
        }
    }
    fn get_or_insert<F>(&self, func: F) -> Result<Bytes>
    where
        F: Fn() -> Result<Bytes>,
    {
        {
            let g = self.value.read();
            if let Some(bytes) = g.as_ref() {
                return Ok(bytes.clone());
            }
        }
        let mut g = self.value.write();
        match g.as_mut() {
            Some(bytes) => Ok(bytes.clone()),
            None => {
                let value = func()?;
                g.replace(value.clone());
                Ok(value)
            }
        }
    }
}

/// An LRU cache of Bytes that can be used by multiple threads
/// concurrently.
///
/// Works by sharding the single-threaded LRUCache into multiple shards.
pub struct ConcurrentLRUCache {
    caches: Option<Vec<Mutex<lru::LruCache<u64, Arc<Inner>>>>>,
}

impl ConcurrentLRUCache {
    pub fn new(shards: usize, cache: Option<ReadCache>) -> Self {
        Self {
            caches: cache.map(|cache| core::iter::repeat_with(|| Mutex::new(cache.lru()))
                .take(shards)
                .collect()),
        }
    }

    /// Get or insert the value into the cache. The inserted value is computed
    /// using the provided callback.
    ///
    /// The shard-level lock is NOT held during the computation.
    /// During the computation the chunk-level lock is held, so only threads contending
    /// on the specific chunk will get blocked.
    pub fn get_or_insert<F>(&self, offset: u64, func: F) -> Result<Bytes>
    where
        F: Fn() -> Result<Bytes>,
    {
        let caches = match self.caches.as_ref() {
            Some(caches) => caches,
            None => return func()
        };

        let mut hasher = DefaultHasher::new();
        offset.hash(&mut hasher);
        // it's ok to truncate the hash.
        #[allow(clippy::cast_possible_truncation)]
        let hash = hasher.finish() as usize;
        let idx = hash % caches.len();

        let inner = {
            let mut lru = unsafe { caches.get_unchecked(idx) }.lock();
            match lru.get(&offset) {
                Some(inner) => inner.clone(),
                None => {
                    let inner = Arc::new(Inner::new());
                    lru.put(offset, inner.clone());
                    inner
                }
            }
        };
        inner.get_or_insert(func)
    }
}
