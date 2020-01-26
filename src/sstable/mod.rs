// Design
//
// The sstables are written from BTrees, or some sorted iterators.
// Actually, sorted key value iterators are the best.
// keys and values are u8 slices.

// Readers CAN be mmap'ed files.
// However, in this case you can't GZIP.
// file-system level gzip would work best here.

// Writers can use buffered file API.

// So we better implement various ways with the same API
//
// Variants:
// gzip OR some other compression OR no compression
// memmap readers or not
//
// File structure:
// [MAGIC][VERSION][META][DATA][INDEX]
// META is the struct of format
// Magic is \x80LSM
//
// index structure
//

use std::collections::BTreeMap;
use std::path::Path;
use std::io::Result;
use std::io::BufWriter;

use memmap;

const MAGIC: &[u8] = b"\x80LSM";

struct Version {
    major: u16,
    minor: u16,
}

enum Compression {
    None,
    GZIP,
}

struct MetaV1_0 {
    data_len: usize,
    index_len: usize,
    items: usize,
    compression: Compression,
    // updating this field is done as the last step.
    // it's presence indicates that the file is good.
    finished: bool,
    checksum: u32,
}

pub trait SSTableReader {
    fn get(&self, key: &[u8]) -> Result<Option<&[u8]>>;
    fn close(self) -> Result<()>;
}

pub struct Options {
    compression: Compression,
    flush_every: usize,
}

impl Options {
    fn compression(&mut self, c: Compression) -> &mut Self {
        self.compression = c;
        self
    }
    fn flush_every(&mut self, e: usize) -> &mut Self {
        self.flush_every = e;
        self
    }
}

impl Default for Options {
    fn default() -> Self {
        Options{
            compression: Compression::None,
            flush_every: 4096,
        }
    }
}

pub trait RawSSTableWriter {
    /// Set the key to the value. This method MUST be called in the sorted
    /// order.
    /// The keys MUST be unique.
    /// Set of empty value is equal to a delete, and is recorded too.
    fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()>;
    fn close(self) -> Result<()>;
}

struct SSTableWriterV1 {
    // This be abstracted away so that compression could be added.
    file: BufWriter<File>,
    flush_every: usize,
    total_offset: usize,
    chunk_offset: usize,
    sparse_index: BTreeMap<Vec<u8>, usize>,
}

impl UncompressedSSTableWriterV1 {
    fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut file = File::open(path)?;

    }
    fn write_index(self) -> Result<()> {
        unimplemented!()
    }
}

impl RawSSTableWriter for UncompressedSSTableWriterV1 {
    fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        unimplemented!()
    }
    fn close(self) -> Result<()> {
        self.write_index()
    }
}

pub fn open(filename: AsRef<Path>) -> Box<dyn SSTableReader> {

}

pub fn write<P: AsRef<Path>(map: BTreeMap<Vec<u8>, Vec<u8>>, filename: P, options: Option<Options>) -> Result<()> {
    let options = options.unwrap_or_else(|| Options::default());
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        //
    }
}