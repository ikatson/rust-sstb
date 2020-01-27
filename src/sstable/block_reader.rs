use super::*;

use lru::LruCache;
use std::collections::hash_map::{HashMap};

pub trait Block<'a> {
    fn find_key<'b>(&'a mut self, key: &'b str) -> Result<Option<&'a [u8]>>;
}

pub trait BlockManager<'a, B: Block<'a>> {
    fn get_block(&'a mut self, offset: u64, limit: u64) -> Result<&'a mut B>;
}

pub struct ReferenceBlock<'a> {
    buf: &'a[u8],
    cursor: usize,
    last_read_key: Option<&'a str>,
    seen_keys: HashMap<&'a str, &'a [u8]>
}

impl<'a> Block<'a> for ReferenceBlock<'a> {
    fn find_key<'b>(&'a mut self, key: &'b str) -> Result<Option<&'a [u8]>> {
        match self.seen_keys.get(key) {
            Some(val) => return Ok(Some(*val)),
            None => {
                if self.last_read_key.map_or(false, |v| v > key) {
                    return Ok(None)
                }

                let value_length_encoded_size = bincode::serialized_size(&Length(0))? as usize;

                while self.cursor < self.buf.len() {
                    let (start_key, cursor) = {
                        let key_end = match memchr::memchr(0, &self.buf[self.cursor..]) {
                            Some(idx) => idx as usize + self.cursor,
                            None => return Err(Error::InvalidData("corrupt or buggy sstable")),
                        };
                        let start_key = std::str::from_utf8(&self.buf[self.cursor..key_end])?;
                        (start_key, key_end + 1)
                    };

                    let (value, cursor) = {
                        let (value_length, cursor) = {
                            let value_length_end = cursor + value_length_encoded_size;
                            let value_length = bincode::deserialize::<Length>(&self.buf[cursor..value_length_end])?.0 as usize;
                            (value_length, value_length_end + 1)
                        };

                        let value_end = cursor + value_length;
                        let value = &self.buf[cursor..value_end];
                        if value.len() != value_length {
                            return Err(Error::InvalidData("corrupt or buggy sstable"));
                        }
                        (value, value_end + 1)
                    };
                    self.seen_keys.insert(start_key, value);
                    self.cursor = cursor;

                    if key == start_key {
                        return Ok(Some(value));
                    }
                }
                return Ok(None)
            }
        }
    }
}

pub struct DirectMemoryAccessBlockManager<'a, B: Block<'a> = ReferenceBlock<'a>> {
    buf: &'a [u8],
    block_cache: LruCache<u64, B>
}

impl<'a, B: Block<'a>> DirectMemoryAccessBlockManager<'a, B> {
    pub fn new(buf: &'a [u8], cache_capacity: usize) -> Self {
        Self{
            buf: buf,
            block_cache: LruCache::new(cache_capacity)
        }
    }
}

impl<'a> BlockManager<'a, ReferenceBlock<'a>> for DirectMemoryAccessBlockManager<'a, ReferenceBlock<'a>> {
    fn get_block(&'a mut self, offset: u64, limit: u64) -> Result<&'a mut ReferenceBlock<'a>> {
        match self.block_cache.get_mut(&offset) {
            Some(block) => Ok(unsafe {&mut *(block as *mut _)}),
            None => {
                let block = ReferenceBlock{
                    buf: &self.buf[offset as usize..limit as usize],
                    cursor: 0,
                    last_read_key: None,
                    seen_keys: HashMap::new()
                };
                self.block_cache.put(offset, block);
                Ok(self.block_cache.get_mut(&offset).unwrap())
            }
        }
    }
}