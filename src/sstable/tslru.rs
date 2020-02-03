use super::{ReadCache, Result};
use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

struct Inner {
    value: Mutex<Option<Bytes>>,
}

impl Inner {
    fn new() -> Self {
        Self {
            value: Mutex::new(None),
        }
    }
    fn get_or_insert<F>(&self, func: F) -> Result<Bytes>
    where
        F: Fn() -> Result<Bytes>,
    {
        let mut g = self.value.lock();
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

pub struct TSLRUCache {
    caches: Vec<Mutex<lru::LruCache<u64, Arc<Inner>>>>,
}

impl TSLRUCache {
    pub fn new(count: usize, cache: ReadCache) -> Self {
        Self {
            caches: core::iter::repeat_with(|| Mutex::new(cache.lru()))
                .take(count)
                .collect(),
        }
    }
    pub fn get_or_insert<F>(&self, offset: u64, func: F) -> Result<Bytes>
    where
        F: Fn() -> Result<Bytes>,
    {
        let mut hasher = DefaultHasher::new();
        offset.hash(&mut hasher);
        let hash = hasher.finish() as usize;
        let idx = hash % self.caches.len();

        let inner = {
            let mut lru = unsafe { self.caches.get_unchecked(idx) }.lock();
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
