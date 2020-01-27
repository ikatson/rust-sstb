use super::*;

use lru::LruCache;
use std::collections::hash_map::{HashMap};

pub trait Block {
    fn find_key<'a, 'b>(&'a mut self, key: &'b str) -> Result<Option<&'a [u8]>>;
}

pub trait BlockManager<B: Block> {
    fn get_block<'a, 'b>(&'a mut self, start: u64, end: u64) -> Result<&'a mut B>;
}

pub struct ReferenceBlock<'a> {
    buf: &'a[u8],
    cursor: usize,
    last_read_key: Option<&'a str>,
    seen_keys: HashMap<&'a str, &'a [u8]>
}

impl<'r> Block for ReferenceBlock<'r> {
    fn find_key<'a, 'b>(&'a mut self, key: &str) -> Result<Option<&'a [u8]>> {
        match self.seen_keys.get(key) {
            Some(val) => return Ok(Some(*val)),
            None => {
                if self.last_read_key.map_or(false, |v| v > key) {
                    return Ok(None)
                }

                macro_rules! buf_get {
                    ($x:expr) => ({
                        self.buf.get($x).ok_or(Error::InvalidData("corrupt or buggy sstable"))?
                    })
                }

                let value_length_encoded_size = bincode::serialized_size(&Length(0))? as usize;

                while self.cursor < self.buf.len() {
                    let (start_key, cursor) = {
                        let key_end = match memchr::memchr(0, &self.buf[self.cursor..]) {
                            Some(idx) => idx as usize + self.cursor,
                            None => return Err(Error::InvalidData("corrupt or buggy sstable")),
                        };
                        let start_key = std::str::from_utf8(buf_get!(self.cursor..key_end))?;
                        (start_key, key_end + 1)
                    };

                    let (value, cursor) = {
                        let (value_length, cursor) = {
                            let value_length_end = cursor + value_length_encoded_size;
                            let value_length = bincode::deserialize::<Length>(buf_get!(cursor..value_length_end))?.0 as usize;
                            (value_length, value_length_end)
                        };

                        let value_end = cursor + value_length;
                        let value = buf_get!(cursor..value_end);
                        if value.len() != value_length {
                            return Err(Error::InvalidData("corrupt or buggy sstable"));
                        }
                        (value, value_end)
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

pub struct DirectMemoryAccessBlockManager<'a, B: Block = ReferenceBlock<'a>> {
    buf: &'a [u8],
    block_cache: LruCache<u64, B>
}

impl<'a, B: Block> DirectMemoryAccessBlockManager<'a, B> {
    pub fn new(buf: &'a [u8], cache_capacity: usize) -> Self {
        Self{
            buf: buf,
            block_cache: LruCache::new(cache_capacity)
        }
    }
}

impl<'r> BlockManager<ReferenceBlock<'r>> for DirectMemoryAccessBlockManager<'r, ReferenceBlock<'r>> {
    fn get_block<'a>(&'a mut self, start: u64, end: u64) -> Result<&'a mut ReferenceBlock<'r>> {
        match self.block_cache.get_mut(&start) {
            Some(block) => Ok(unsafe {&mut *(block as *mut _)}),
            None => {
                let block = ReferenceBlock{
                    buf: &self.buf[start as usize..end as usize],
                    cursor: 0,
                    last_read_key: None,
                    seen_keys: HashMap::new()
                };
                self.block_cache.put(start, block);
                Ok(self.block_cache.get_mut(&start).unwrap())
            }
        }
    }
}