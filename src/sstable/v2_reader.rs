use super::Result;
use std::sync::{Arc, Mutex};
use bytes::Bytes;
use std::io::Read;


pub trait Block {
    fn find_key(&self, key: &[u8]) -> Result<Option<Bytes>>;
}

pub trait BlockCache<B> {
    fn get_block(&self, start: u64) -> Option<B>;
}

trait ReadFactory<R: Read> {
    fn get_reader(&self, start: u64, end: u64) -> Result<R>;
}

pub trait BlockManager<B: Block> {
    // Converts file offsets into block API
    // caching, compression etc is abstracted away.
    fn get_block(&self, start: u64, end: u64) -> Result<B>;
}

trait Reader {
    fn get(&mut self, key: &[u8]) -> Result<Option<Bytes>>;
}

struct BlockManagerImpl<R, RF, C> {
    read_factory: RF,
    cache: C,
    read: std::marker::PhantomData<R>
}

impl<B, R, RF, C> BlockManager<B> for BlockManagerImpl<R, RF, C>
    where C: BlockCache<B>,
          RF: ReadFactory<R>,
          R: Read,
          B: Block + Clone,
 {
    fn get_block(&self, start: u64, end: u64) -> Result<B> {
        if let Some(b) = self.cache.get_block(start) {
            return Ok(b.clone())
        };
        let reader = self.read_factory.get_reader(start, end)?;
        unimplemented!()
    }
}

#[derive(Copy, Clone, Debug)]
pub enum ReadCache {
    Blocks(usize),
    Unbounded,
}

#[derive(Copy, Clone, Debug)]
pub struct ReadOptions {
    cache: Option<ReadCache>,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            cache: Some(ReadCache::Blocks(32)),
        }
    }
}