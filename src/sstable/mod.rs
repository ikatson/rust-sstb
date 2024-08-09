//! Implementations of sstables stored as files on disk.
//!
//! Single and multi-threaded SSTable readers, single-threaded writer.
//!
//! For writing sstables look at the `writer` module.
//!
//! There is a also a convenience function `write_btree_map`.
//!
//! For reading sstables, there are multiple implementations with tradeoffs.
//!
//! For example, the simplest and thread-safe implementation of a reader is
//! `MmapUncompressedSSTableReader`. But it only works with uncompressed files.
//!
//! Also if the tables are larger than memory, mmap may start causing issues, although that
//! needs to be measured on the target setup.
//!

use std::collections::BTreeMap;
use std::path::Path;

mod compress_ctx_writer;
mod compression;
mod concurrent_lru;
mod concurrent_page_cache;
mod error;
mod ondisk_format;
mod options;
mod page_cache;
mod posreader;
mod poswriter;
mod result;
mod types;
mod utils;

pub mod reader;
pub mod writer;

pub use reader::ConcurrentSSTableReader;
pub use reader::MmapUncompressedSSTableReader;
pub use reader::SSTableReader;

pub use writer::RawSSTableWriter;
pub use writer::SSTableWriterV2;

pub use error::{Error, INVALID_DATA};
pub use options::*;
pub use result::Result;
pub use types::*;

/// A convenience function to write a btree map to a file.
///
///
/// Example:
/// ```
/// use std::collections::BTreeMap;
/// use sstb::sstable::{write_btree_map, WriteOptions};
///
/// let mut map = BTreeMap::new();
/// let filename = "/tmp/some-sstable";
/// let write_options = WriteOptions::default();
///
/// map.insert(b"foo", b"some foo");
/// map.insert(b"bar", b"some bar");
/// write_btree_map(&map, filename, Some(write_options)).unwrap();
/// ```
pub fn write_btree_map<K: AsRef<[u8]>, V: AsRef<[u8]>, P: AsRef<Path>>(
    map: &BTreeMap<K, V>,
    filename: P,
    options: Option<WriteOptions>,
) -> Result<()> {
    let options = options.unwrap_or_default();
    let mut writer = writer::SSTableWriterV2::new_with_options(filename, &options)?;

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

    fn write_basic_map(filename: &str, options: WriteOptions) {
        let mut map: BTreeMap<&[u8], &[u8]> = BTreeMap::new();
        map.insert(b"foo", b"some foo");
        map.insert(b"fail", b"some fail");
        map.insert(b"bar", b"some bar");
        write_btree_map(&map, filename, Some(options)).unwrap();
    }

    fn test_basic_sanity(options: WriteOptions, filename: &str) {
        write_basic_map(filename, options);
        let mut reader =
            reader::SSTableReader::new_with_options(filename, &ReadOptions::default()).unwrap();

        assert_eq!(reader.get(b"foo").unwrap(), Some(b"some foo" as &[u8]));
        assert_eq!(reader.get(b"bar").unwrap(), Some(b"some bar" as &[u8]));
        assert_eq!(reader.get(b"foobar").unwrap(), None);
    }

    fn test_basic_sanity_threads(options: WriteOptions, filename: &str) {
        write_basic_map(filename, options);

        let reader =
            reader::ConcurrentSSTableReader::new_with_options(filename, &ReadOptions::default())
                .unwrap();

        crossbeam::scope(|s| {
            s.spawn(|_| {
                assert_eq!(
                    reader.get(b"foo").unwrap().as_deref(),
                    Some(b"some foo" as &[u8])
                );
            });
            s.spawn(|_| {
                assert_eq!(
                    reader.get(b"bar").unwrap().as_deref(),
                    Some(b"some bar" as &[u8])
                );
            });
            s.spawn(|_| {
                assert_eq!(reader.get(b"foobar").unwrap(), None);
            });
        })
        .unwrap();
    }

    #[test]
    fn test_uncompressed_basic_sanity() {
        let mut options = WriteOptions::default();
        options.compression = Compression::None;
        test_basic_sanity(options, "/tmp/sstable");
    }

    #[test]
    fn test_mmap_uncompressed_basic_sanity() {
        let filename = "/tmp/sstable_mmap_uncompressed";
        let mut options = WriteOptions::default();
        options.compression = Compression::None;
        write_basic_map(filename, options);

        let reader = reader::MmapUncompressedSSTableReader::new(filename).unwrap();
        assert_eq!(reader.get(b"foo").unwrap(), Some(b"some foo" as &[u8]));
        assert_eq!(reader.get(b"bar").unwrap(), Some(b"some bar" as &[u8]));
        assert_eq!(reader.get(b"foobar").unwrap(), None);
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

    #[test]
    fn test_uncompressed_basic_sanity_threads() {
        let mut options = WriteOptions::default();
        options.compression = Compression::None;
        test_basic_sanity_threads(options, "/tmp/sstable_threads");
    }

    #[test]
    fn test_mmap_uncompressed_basic_sanity_threads() {
        let filename = "/tmp/sstable_mmap_uncompressed_threads";
        let mut options = WriteOptions::default();
        options.compression = Compression::None;
        write_basic_map(filename, options);

        let reader = reader::MmapUncompressedSSTableReader::new(filename).unwrap();

        crossbeam::scope(|s| {
            s.spawn(|_| {
                assert_eq!(reader.get(b"foo").unwrap(), Some(b"some foo" as &[u8]));
            });
            s.spawn(|_| {
                assert_eq!(reader.get(b"bar").unwrap(), Some(b"some bar" as &[u8]));
            });
            s.spawn(|_| {
                assert_eq!(reader.get(b"foobar").unwrap(), None);
            });
        })
        .unwrap();
    }

    #[test]
    fn test_compressed_with_zlib_basic_sanity_threads() {
        let mut options = WriteOptions::default();
        options.compression = Compression::Zlib;
        test_basic_sanity_threads(options, "/tmp/sstable_zlib_threads");
    }

    #[test]
    fn test_compressed_with_snappy_basic_sanity_threads() {
        let mut options = WriteOptions::default();
        options.compression = Compression::Snappy;
        test_basic_sanity_threads(options, "/tmp/sstable_snappy_threads");
    }
}
