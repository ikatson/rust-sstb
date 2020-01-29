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

use memchr;

const MAGIC: &[u8] = b"\x80LSM";
const VERSION_10: Version = Version { major: 1, minor: 0 };

mod block_reader;
mod compress_ctx_writer;
pub mod error;
mod posreader;
mod poswriter;

pub mod reader;
pub mod writer;

#[cfg(test)]
mod sorted_string_iterator;

use error::Error;

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

#[derive(Serialize, Deserialize, Debug)]
struct Length(u64);

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
    fn set(&mut self, key: &str, value: &[u8]) -> Result<()>;
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

pub fn write_btree_map<D: AsRef<[u8]>, P: AsRef<Path>>(
    map: &BTreeMap<String, D>,
    filename: P,
    options: Option<WriteOptions>,
) -> Result<()> {
    let options = options.unwrap_or_else(|| WriteOptions::default());
    let mut writer = writer::SSTableWriterV1::new_with_options(filename, options)?;

    for (key, value) in map.iter() {
        writer.set(key, value.as_ref())?;
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
        let out = std::process::Command::new("ps").args(&["-p", &pid, "-o", "rss"]).output().unwrap();
        let out = String::from_utf8(out.stdout).unwrap();
        let pid_line = out.lines().nth(1).unwrap();
        pid_line.trim().parse::<usize>().unwrap()
    }

    fn test_basic_sanity(options: WriteOptions, filename: &str) {
        let mut map = BTreeMap::new();
        map.insert("foo".into(), b"some foo");
        map.insert("bar".into(), b"some bar");
        write_btree_map(&map, filename, Some(options)).unwrap();

        let mut reader = reader::SSTableReader::new_with_options(filename, &reader::ReadOptions::default()).unwrap();

        assert_eq!(
            reader.get("foo").unwrap().as_ref().map(|v| v.as_bytes()),
            Some(b"some foo" as &[u8])
        );
        assert_eq!(
            reader.get("bar").unwrap().as_ref().map(|v| v.as_bytes()),
            Some(b"some bar" as &[u8])
        );
        assert_eq!(
            reader.get("foobar").unwrap().as_ref().map(|v| v.as_bytes()),
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

    fn test_large_file_with_options(opts: WriteOptions, filename: &str, expected_max_rss_kb: usize, values: usize) {
        let mut writer = writer::SSTableWriterV1::new_with_options(filename, opts).unwrap();

        let buf = [0; 1024];

        let mut iter = SortedStringIterator::new(10, values);
        while let Some(key) = iter.next() {
            writer.set(key, &buf).unwrap();
        }

        writer.finish().unwrap();

        let read_opts = reader::ReadOptions::default();
        let mut reader = reader::SSTableReader::new_with_options(filename, &read_opts).unwrap();
        iter.reset();
        while let Some(key) = iter.next() {
            let val = reader.get(key).unwrap().expect(key);
            assert_eq!(val.as_bytes().len(), 1024);
        }
        let rss = get_current_pid_rss();
        dbg!("RSS KB", rss);
        assert!(rss < expected_max_rss_kb, "RSS usage is {}Kb, but expected less than {}Kb", rss, expected_max_rss_kb);
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
