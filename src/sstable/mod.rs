// Design
//
// The sstables are written from BTrees, or some sorted iterators.
// Actually, sorted key value iterators are the best.
// keys are strings, values are byte slices.

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

use bincode;
use memmap;
use serde::{Deserialize, Serialize};

const MAGIC: &[u8] = b"\x80LSM";
const VERSION_10: Version = Version { major: 1, minor: 0 };
type KeyLength = u16;

const KEY_LENGTH_MAX: usize = core::u16::MAX as usize;
const VALUE_LENGTH_MAX: usize = core::u32::MAX as usize;

type ValueLength = u32;
type OffsetLength = u64;
const KEY_LENGTH_SIZE: usize = core::mem::size_of::<KeyLength>();
const VALUE_LENGTH_SIZE: usize = core::mem::size_of::<ValueLength>();

const OFFSET_SIZE: usize = core::mem::size_of::<OffsetLength>();

mod compression;
mod block_reader;
mod compress_ctx_writer;
pub mod error;
mod posreader;
mod poswriter;
mod page_cache;

pub mod reader;
pub mod writer;

pub use reader::ReadOptions;
pub use reader::ReadCache;
pub use reader::SSTableReader;

use error::{Error, INVALID_DATA};

use std::io::{Read, Write};

pub type Result<T> = core::result::Result<T, Error>;

#[derive(Serialize, Default, Deserialize, Debug, PartialEq, Eq)]
pub struct Version {
    major: u16,
    minor: u16,
}

#[derive(Serialize, Deserialize, Copy, Clone, Debug, PartialEq)]
pub enum Compression {
    None,
    Zlib,
    Snappy,
}

impl Default for Compression {
    fn default() -> Self {
        Compression::None
    }
}

/// An efficient way to deserialize and NOT fail when the reader is at EOF right
/// from the start, without any allocations.
fn deserialize_from_eof_is_ok<T: serde::de::DeserializeOwned, R: Read>(
    reader: R,
) -> Result<Option<T>> {
    let mut pr = posreader::PosReader::new(reader, 0);
    let result = bincode::deserialize_from::<_, T>(&mut pr);
    match result {
        Ok(val) => Ok(Some(val)),
        Err(e) => match &*e {
            bincode::ErrorKind::Io(ioe) => {
                if ioe.kind() == std::io::ErrorKind::UnexpectedEof {
                    if pr.current_offset() == 0 {
                        // This is actually fine and we hit EOF right away.
                        return Ok(None)
                    }
                }
                return Err(e)?
            }
            _ => Err(e)?,
        },
    }
}

#[derive(Serialize, Deserialize, Debug, Default)]
struct KVLength {
    key_length: KeyLength,
    value_length: ValueLength,
}

impl KVLength {
    fn new(k: usize, v: usize) -> Result<Self> {
        if k > KEY_LENGTH_MAX {
            return Err(Error::KeyTooLong(k));
        }
        if v > VALUE_LENGTH_MAX {
            return Err(Error::ValueTooLong(v));
        }
        Ok(Self {
            key_length: k as KeyLength,
            value_length: v as ValueLength,
        })
    }
    const fn encoded_size() -> usize {
        KEY_LENGTH_SIZE + VALUE_LENGTH_SIZE
    }
    fn serialize_into<W: Write>(&self, w: W) -> Result<()> {
        Ok(bincode::serialize_into(w, self)?)
    }
}

#[derive(Serialize, Deserialize, Debug, Default)]
struct KVOffset {
    key_length: KeyLength,
    offset: OffsetLength,
}

impl KVOffset {
    fn new(k: usize, offset: OffsetLength) -> Result<Self> {
        if k > KEY_LENGTH_MAX {
            return Err(Error::KeyTooLong(k));
        }
        Ok(Self {
            key_length: k as KeyLength,
            offset: offset,
        })
    }
    const fn encoded_size() -> usize {
        return KEY_LENGTH_SIZE + OFFSET_SIZE
    }
    fn deserialize_from_eof_is_ok<R: Read>(r: R) -> Result<Option<Self>> {
        Ok(deserialize_from_eof_is_ok(r)?)
    }
    fn serialize_into<W: Write>(&self, w: W) -> Result<()> {
        Ok(bincode::serialize_into(w, self)?)
    }
}

#[derive(Serialize, Deserialize, Default, Debug)]
struct MetaV1_0 {
    data_len: u64,
    index_len: u64,
    items: u64,
    compression: Compression,
    // updating this field is done as the last step.
    // it's presence indicates that the file is good.
    finished: bool,
    checksum: u32,
}

pub trait RawSSTableWriter {
    /// Set the key to the value. This method MUST be called in the sorted
    /// order.
    /// The keys MUST be unique.
    /// Set of empty value is equal to a delete, and is recorded too.
    fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()>;
    /// Close the writer and flush everything to the underlying storage.
    fn close(self) -> Result<()>;
}

pub struct WriteOptionsBuilder {
    pub compression: Compression,
    pub flush_every: usize,
}

impl WriteOptionsBuilder {
    pub fn new() -> Self {
        let default = WriteOptions::default();
        Self{
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
        WriteOptions{
            compression: self.compression,
            flush_every: self.flush_every,
        }
    }
}

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

pub fn write_btree_map<K: AsRef<[u8]>, V: AsRef<[u8]>, P: AsRef<Path>>(
    map: &BTreeMap<K, V>,
    filename: P,
    options: Option<WriteOptions>,
) -> Result<()> {
    let options = options.unwrap_or_else(|| WriteOptions::default());
    let mut writer = writer::SSTableWriterV1::new_with_options(filename, options)?;

    for (key, value) in map.iter() {
        writer.set(key.as_ref(), value.as_ref())?;
    }
    writer.close()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn test_basic_sanity(options: WriteOptions, filename: &str) {
        let mut map = BTreeMap::new();
        map.insert(b"foo", b"some foo");
        map.insert(b"bar", b"some bar");
        write_btree_map(&map, filename, Some(options)).unwrap();

        let mut reader =
            reader::SSTableReader::new_with_options(filename, &reader::ReadOptions::default())
                .unwrap();

        assert_eq!(
            reader.get(b"foo").unwrap(),
            Some(b"some foo" as &[u8])
        );
        assert_eq!(
            reader.get(b"bar").unwrap(),
            Some(b"some bar" as &[u8])
        );
        assert_eq!(
            reader
                .get(b"foobar")
                .unwrap()
                ,
            None
        );
    }

    #[test]
    fn test_uncompressed_basic_sanity() {
        let mut options = WriteOptions::default();
        options.compression = Compression::None;
        test_basic_sanity(options, "/tmp/sstable");
    }

    #[test]
    fn test_compressed_with_zlib_basic_sanity() {
        let mut options = WriteOptions::default();
        options.compression = Compression::Zlib;
        test_basic_sanity(options, "/tmp/sstable_zlib");
    }

    #[test]
    fn test_compressed_with_snappy_basic_sanity() {
        let mut options = WriteOptions::default();
        options.compression = Compression::Snappy;
        test_basic_sanity(options, "/tmp/sstable_snappy");
    }
}
