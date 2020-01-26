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

mod compress_ctx_writer;
mod error;
mod posreader;
mod poswriter;
mod reader;
mod writer;
use error::Error;

type Result<T> = core::result::Result<T, Error>;

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct Version {
    major: u16,
    minor: u16,
}

#[derive(Serialize, Deserialize, Copy, Clone, Debug)]
enum Compression {
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
    data_len: usize,
    index_len: usize,
    items: usize,
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
    fn close(self) -> Result<()>;
}

#[derive(Debug)]
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
        Options {
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
    options: Option<Options>,
) -> Result<()> {
    let options = options.unwrap_or_else(|| Options::default());
    let mut writer = writer::SSTableWriterV1::new(filename, options)?;

    for (key, value) in map.iter() {
        writer.set(key, value.as_ref())?;
    }
    writer.close()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;

    #[test]
    fn test_uncompressed_works() {
        use std::collections::BTreeMap;

        let mut map = BTreeMap::new();
        map.insert("foo".into(), b"some foo");
        map.insert("bar".into(), b"some bar");

        let mut options = Options::default();
        options.compression(Compression::None);
        write_btree_map(&map, "/tmp/sstable", Some(options)).unwrap();

        let reader = reader::SSTableReader::new("/tmp/sstable").unwrap();

        assert_eq!(reader.get("foo").unwrap(), Some(b"some foo" as &[u8]));
        assert_eq!(reader.get("bar").unwrap(), Some(b"some bar" as &[u8]));
        assert_eq!(reader.get("foobar").unwrap(), None);
    }

    #[test]
    fn bench_memory_usage() {
        let mut writer =
            writer::SSTableWriterV1::new("/tmp/sstable_big", Options::default()).unwrap();
        let mut input = File::open("/dev/zero").unwrap();

        let mut buf = [0; 1024];
        use std::io::Read;

        input.read(&mut buf).unwrap();

        let letters = b"abcdefghijklmnopqrstuvwxyz";

        for i in letters {
            for j in letters {
                for k in letters {
                    for m in letters {
                        // for n in letters {
                        let key = [*i, *j, *k, *m];
                        let skey = unsafe { std::str::from_utf8_unchecked(&key) };
                        writer.set(skey, &buf).unwrap();
                        // }
                    }
                }
            }
        }

        writer.write_index().unwrap();

        let reader = reader::SSTableReader::new("/tmp/sstable_big").unwrap();

        for i in letters {
            for j in letters {
                for k in letters {
                    for m in letters {
                        // for n in letters {
                        let key = [*i, *j, *k, *m];
                        let skey = unsafe { std::str::from_utf8_unchecked(&key) };
                        let val = reader.get(skey).unwrap().unwrap();
                        assert_eq!(val.len(), 1024);
                        // }
                    }
                }
            }
        }
    }
}
