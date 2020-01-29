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
const U32_SIZE: usize = core::mem::size_of::<u32>();
const U32_MAX: usize = core::u32::MAX as usize;
const U64_SIZE: usize = core::mem::size_of::<u64>();

mod block_reader;
mod compress_ctx_writer;
pub mod error;
mod posreader;
mod poswriter;

pub mod reader;
pub mod writer;

#[cfg(test)]
mod sorted_string_iterator;
use error::{Error, INVALID_DATA};

use std::io::{Read, Write};

type Result<T> = core::result::Result<T, Error>;

#[derive(Serialize, Default, Deserialize, Debug, PartialEq, Eq)]
pub struct Version {
    major: u16,
    minor: u16,
}

#[derive(Serialize, Deserialize, Copy, Clone, Debug)]
pub enum Compression {
    None,
    Zlib,
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
    key_length: u32,
    value_length: u32,
}

impl KVLength {
    fn new(k: usize, v: usize) -> Result<Self> {
        if k > U32_MAX {
            return Err(Error::KeyTooLong(k));
        }
        if v > U32_MAX {
            return Err(Error::ValueTooLong(v));
        }
        Ok(Self {
            key_length: k as u32,
            value_length: v as u32,
        })
    }
    fn encoded_size() -> usize {
        bincode::serialized_size(&Self::default()).unwrap() as usize
    }
    fn deserialize_from<R: Read>(r: R) -> Result<Self> {
        Ok(bincode::deserialize_from(r)?)
    }
    fn deserialize_from_eof_is_ok<R: Read>(mut r: R) -> Result<Option<Self>> {
        Ok(deserialize_from_eof_is_ok(&mut r)?)
    }
    fn serialize_into<W: Write>(&self, w: W) -> Result<()> {
        Ok(bincode::serialize_into(w, self)?)
    }
}

#[derive(Serialize, Deserialize, Debug, Default)]
struct KVOffset {
    key_length: u32,
    offset: u64,
}

impl KVOffset {
    fn new(k: usize, offset: u64) -> Result<Self> {
        if k > U32_MAX {
            return Err(Error::KeyTooLong(k));
        }
        Ok(Self {
            key_length: k as u32,
            offset: offset,
        })
    }
    fn encoded_size() -> usize {
        bincode::serialized_size(&Self::default()).unwrap() as usize
    }
    fn deserialize_from<R: Read>(r: R) -> Result<Self> {
        Ok(bincode::deserialize_from(r)?)
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

#[derive(Debug)]
pub struct WriteOptions {
    pub compression: Compression,
    pub flush_every: usize,
}

impl Default for WriteOptions {
    fn default() -> Self {
        WriteOptions {
            compression: Compression::None,
            flush_every: 4096,
        }
    }
}

pub fn open<P: AsRef<Path>>(_filename: P) -> reader::SSTableReader {
    unimplemented!()
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
    use sorted_string_iterator::SortedStringIterator;
    use std::collections::BTreeMap;

    fn get_current_pid_rss() -> usize {
        let pid = format!("{}", std::process::id());
        let out = std::process::Command::new("ps")
            .args(&["-p", &pid, "-o", "rss"])
            .output()
            .unwrap();
        let out = String::from_utf8(out.stdout).unwrap();
        let pid_line = out.lines().nth(1).unwrap();
        pid_line.trim().parse::<usize>().unwrap()
    }

    fn test_basic_sanity(options: WriteOptions, filename: &str) {
        let mut map = BTreeMap::new();
        map.insert(b"foo", b"some foo");
        map.insert(b"bar", b"some bar");
        write_btree_map(&map, filename, Some(options)).unwrap();

        let mut reader =
            reader::SSTableReader::new_with_options(filename, &reader::ReadOptions::default())
                .unwrap();

        assert_eq!(
            reader.get(b"foo").unwrap().as_ref().map(|v| v.as_bytes()),
            Some(b"some foo" as &[u8])
        );
        assert_eq!(
            reader.get(b"bar").unwrap().as_ref().map(|v| v.as_bytes()),
            Some(b"some bar" as &[u8])
        );
        assert_eq!(
            reader
                .get(b"foobar")
                .unwrap()
                .as_ref()
                .map(|v| v.as_bytes()),
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

    fn test_large_file_with_options(
        opts: WriteOptions,
        filename: &str,
        expected_max_rss_kb: usize,
        values: usize,
    ) {
        let mut writer = writer::SSTableWriterV1::new_with_options(filename, opts).unwrap();

        let buf = [0; 1024];

        let mut iter = SortedStringIterator::new(10, values);
        while let Some(key) = iter.next() {
            writer.set(key.as_bytes(), &buf).unwrap();
        }

        writer.finish().unwrap();

        let read_opts = reader::ReadOptions::default();
        let mut reader = reader::SSTableReader::new_with_options(filename, &read_opts).unwrap();
        iter.reset();
        while let Some(key) = iter.next() {
            let val = reader.get(key.as_bytes()).unwrap().expect(key);
            assert_eq!(val.as_bytes().len(), 1024);
        }
        let rss = get_current_pid_rss();
        dbg!("RSS KB", rss);
        assert!(
            rss < expected_max_rss_kb,
            "RSS usage is {}Kb, but expected less than {}Kb",
            rss,
            expected_max_rss_kb
        );
    }

    #[test]
    fn test_large_mmap_file() {
        let mut opts = WriteOptions::default();
        opts.compression = Compression::None;
        let filename = "/tmp/sstable_big";
        test_large_file_with_options(opts, filename, 3_000_000, 3_000_000);
    }

    #[test]
    fn test_large_zlib_file() {
        let mut opts = WriteOptions::default();
        opts.compression = Compression::Zlib;
        let filename = "/tmp/sstable_big_zlib";
        test_large_file_with_options(opts, filename, 1_000_000, 1_000_000);
    }
}
