use super::*;

use lru::LruCache;
use std::collections::hash_map::{HashMap, Entry};

use std::io::BufRead;

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
                    self.last_read_key.replace(start_key);

                    if key == start_key {
                        return Ok(Some(value));
                    }

                    if start_key > key {
                        return Ok(None)
                    }
                }
                return Ok(None)
            }
        }
    }
}

pub struct ReaderBlock<R> {
    reader: R,
    last_read_key: Option<String>,
    finished: bool,
    seen_keys: HashMap<String, Vec<u8>>
}

impl<R: BufRead> Block for ReaderBlock<R> {
    fn find_key<'a, 'b>(&'a mut self, key: &str) -> Result<Option<&'a [u8]>> {
        match self.seen_keys.get(key) {
            Some(val) => return Ok(Some(unsafe {&* (val as *const Vec<u8>)})),
            None => {
                if self.last_read_key.as_ref().map_or(false, |v| v.as_str() > key) {
                    return Ok(None)
                }

                let value_length_encoded_size = bincode::serialized_size(&Length(0))? as usize;

                while !self.finished {
                    let mut buf = Vec::new();
                    let size = self.reader.read_until(0, &mut buf)?;
                    if size == 0 {
                        self.finished = true;
                        break
                    }
                    if buf.last().map_or(false, |v| *v != 0) {
                        return Err(Error::InvalidData("corrupt sstable"));
                    }
                    buf.pop();
                    let start_key = String::from_utf8(buf)?;
                    let value_length = bincode::deserialize_from::<_, Length>(&mut self.reader)?.0;
                    let mut value = Vec::with_capacity(value_length as usize);
                    self.reader.read_exact(&mut value)?;

                    self.last_read_key = Some(match self.last_read_key.take() {
                        Some(mut old) => {
                            old.truncate(0);
                            old.push_str(&start_key);
                            old
                        },
                        None => start_key.clone()
                    });

                    match self.seen_keys.entry(start_key) {
                        Entry::Occupied(occupied) => {
                            unreachable!()
                        },
                        Entry::Vacant(vacant) => {
                            let equal = vacant.key() == key;
                            let reference = unsafe {&mut *(vacant.insert(value) as *mut Vec<u8>)};
                            if equal {
                                return Ok(Some(reference))
                            }
                        }
                    };
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

pub struct DMAThenReadBlockManager<'a, B, F> {
    buf: &'a [u8],
    block_cache: LruCache<u64, B>,
    factory: F,
}

impl<'a, R, F> DMAThenReadBlockManager<'a, ReaderBlock<R>, F>
    where R: BufRead,
          F: Fn(&'a [u8]) -> R
{
    pub fn new(buf: &'a [u8], factory: F, cache_capacity: usize) -> Self {
        Self{
            buf: buf,
            block_cache: LruCache::new(cache_capacity),
            factory: factory,
        }
    }
}

impl<'r, R, F> BlockManager<ReaderBlock<R>> for DMAThenReadBlockManager<'r, ReaderBlock<R>, F>
    where R: BufRead,
          F: Fn(&'r [u8]) -> R
{
    fn get_block<'a>(&'a mut self, start: u64, end: u64) -> Result<&'a mut ReaderBlock<R>> {
        match self.block_cache.get_mut(&start) {
            Some(block) => Ok(unsafe {&mut *(block as *mut _)}),
            None => {
                let block = ReaderBlock{
                    reader: (self.factory)(&self.buf[start as usize..end as usize]),
                    last_read_key: None,
                    finished: false,
                    seen_keys: HashMap::new()
                };
                self.block_cache.put(start, block);
                Ok(self.block_cache.get_mut(&start).unwrap())
            }
        }
    }
}