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

use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::BufWriter;
use std::io::{Seek, SeekFrom, Write};
use std::mem::MaybeUninit;
use std::path::Path;

use bincode;
use memmap;
use serde::{Deserialize, Serialize};

use memchr;

const MAGIC: &[u8] = b"\x80LSM";
const VERSION_10: Version = Version { major: 1, minor: 0 };

mod poswriter;
mod error;
use error::Error;
use poswriter::PosWriter;

type Result<T> = core::result::Result<T, Error>;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Version {
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

pub trait SSTableReader {
    fn get(&self, key: &str) -> Result<Option<&[u8]>>;
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

pub trait CompressionContextWriter<I: Write>: Write {
    fn relative_offset(&mut self) -> Result<usize>;
    fn reset_compression_context(&mut self) -> Result<usize>;
    fn into_inner(self: Box<Self>) -> Result<PosWriter<I>>;
}

struct UncompressedWriter<W> {
    writer: PosWriter<W>,
    initial: usize,
}

impl<W> UncompressedWriter<W> {
    pub fn new(writer: PosWriter<W>) -> Self {
        UncompressedWriter {
            initial: writer.current_offset(),
            writer: writer,
        }
    }
}

impl<W: Write> Write for UncompressedWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.writer.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

impl<W: Write> CompressionContextWriter<W> for UncompressedWriter<W> {
    fn relative_offset(&mut self) -> Result<usize> {
        Ok(self.writer.current_offset() - self.initial)
    }
    fn reset_compression_context(&mut self) -> Result<usize> {
        Ok(self.writer.current_offset())
    }
    fn into_inner(self: Box<Self>) -> Result<PosWriter<W>> {
        Ok(self.writer)
    }
}

struct ZlibWriter<W: Write> {
    encoder: MaybeUninit<flate2::write::ZlibEncoder<PosWriter<W>>>,
}

impl<W: Write> Write for ZlibWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        unimplemented!()
    }

    fn flush(&mut self) -> std::io::Result<()> {
        unimplemented!()
    }
}

impl<W: Write> ZlibWriter<W> {
    fn new(w: PosWriter<W>) -> Self {
        let pos_writer = w;
        let encoder = flate2::write::ZlibEncoder::new(pos_writer, flate2::Compression::default());
        ZlibWriter {
            encoder: MaybeUninit::new(encoder),
        }
    }
}

impl<W: Write> CompressionContextWriter<W> for ZlibWriter<W> {
    fn relative_offset(&mut self) -> Result<usize> {
        unimplemented!()
    }
    fn reset_compression_context(&mut self) -> Result<usize> {
        unimplemented!()
    }
    fn into_inner(self: Box<Self>) -> Result<PosWriter<W>> {
        unimplemented!()
    }
}

pub trait RawSSTableWriter {
    /// Set the key to the value. This method MUST be called in the sorted
    /// order.
    /// The keys MUST be unique.
    /// Set of empty value is equal to a delete, and is recorded too.
    fn set(&mut self, key: &str, value: &[u8]) -> Result<()>;
    fn close(self) -> Result<()>;
}

struct SSTableWriterV1 {
    file: Box<dyn CompressionContextWriter<BufWriter<File>>>,
    meta: MetaV1_0,
    meta_start: usize,
    data_start: usize,
    flush_every: usize,
    sparse_index: BTreeMap<String, usize>,
}

impl SSTableWriterV1 {
    fn new<P: AsRef<Path>>(path: P, options: Options) -> Result<Self> {
        let file = File::create(path)?;
        let writer = BufWriter::new(file);
        let mut writer = PosWriter::new(writer, 0);
        writer.write(MAGIC)?;
        bincode::serialize_into(&mut writer, &VERSION_10)?;

        let meta_start = writer.current_offset();

        let mut meta = MetaV1_0::default();
        meta.compression = options.compression;
        bincode::serialize_into(&mut writer, &meta)?;

        let data_start = writer.current_offset();

        let file = match options.compression {
            Compression::None => Box::new(UncompressedWriter::new(writer)) as Box<_>,
            Compression::Zlib => Box::new(ZlibWriter::new(writer)) as Box<_>,
        };

        Ok(Self {
            file: file,
            meta: meta,
            meta_start: meta_start,
            data_start: data_start,
            flush_every: options.flush_every,
            sparse_index: BTreeMap::new(),
        })
    }
    fn write_index(self) -> Result<()> {
        match self {
            SSTableWriterV1 {
                file,
                mut meta,
                meta_start,
                data_start,
                flush_every,
                sparse_index,
            } => {
                let mut writer = file.into_inner()?;
                let index_start = writer.current_offset();
                for (key, value) in sparse_index.into_iter() {
                    writer.write_all(key.as_bytes())?;
                    writer.write_all(b"\0")?;
                    bincode::serialize_into(&mut writer, &Length(value as u64))?;
                }
                let index_len = writer.current_offset() - index_start;
                meta.finished = true;
                meta.index_len = index_len;
                meta.data_len = index_start - data_start;
                let mut writer = writer.into_inner();
                writer.seek(SeekFrom::Start(meta_start as u64))?;
                bincode::serialize_into(&mut writer, &meta)?;
                Ok(())
            },
        }
    }
}

impl RawSSTableWriter for SSTableWriterV1 {
    fn set(&mut self, key: &str, value: &[u8]) -> Result<()> {
        // If the current offset is too high, flush, and add this record to the index.
        //
        // Also reset the compression to a fresh state.
        if self.file.relative_offset()? >= self.flush_every || self.meta.items == 0 {
            let offset = self.file.reset_compression_context()?;
            self.sparse_index.insert(key.to_owned(), offset);
        }
        self.file.write_all(key.as_bytes())?;
        self.file.write_all(b"\0")?;
        bincode::serialize_into(&mut self.file, &Length(value.len() as u64))?;
        self.file.write_all(value)?;
        self.meta.items += 1;
        Ok(())
    }

    fn close(self) -> Result<()> {
        self.write_index()
    }
}

pub struct MmapSSTableReader {
    meta: MetaV1_0,
    mmap: memmap::Mmap,
    data_start: u64,
    index_start: u64,
    // it's not &'static in reality, but it's bound to mmap's lifetime.
    // It will NOT work with compression.
    index: BTreeMap<&'static str, usize>
}

impl MmapSSTableReader {
    pub fn new<P: AsRef<Path>>(filename: P) -> Result<Self> {
        let file = File::open(filename)?;
        let mmap = unsafe {memmap::MmapOptions::new().map(&file)?};

        let version_offset = MAGIC.len();

        if &mmap[0..version_offset] != MAGIC {
            return Err(Error::invalid_data("not an sstable, magic does not match"))
        }

        let version = bincode::deserialize(&mmap[version_offset..])?;
        dbg!(&version);

        if version != VERSION_10 {
            return Err(Error::UnsupportedVersion(version));
        }

        let meta_offset = (version_offset as u64) + bincode::serialized_size(&version)?;
        let meta: MetaV1_0 = bincode::deserialize(&mmap[meta_offset as usize..])?;
        dbg!(&meta);
        let meta_size = bincode::serialized_size(&meta)?;

        let data_start = meta_offset + meta_size;

        let mut index = BTreeMap::new();

        let index_start = data_start + (meta.data_len as u64);

        let mut index_data = &mmap[(index_start as usize)..];
        if index_data.len() != meta.index_len {
            return Err(Error::InvalidData("invalid index length"))
        }

        while index_data.len() > 0 {
            let string_end = memchr::memchr(b'\0', index_data);
            let zerobyte = match string_end {
                Some(idx) => idx,
                None => return Err(Error::InvalidData("corrupt index"))
            };
            let key = std::str::from_utf8(&index_data[..zerobyte])?;
            // Make it &'static
            let key: &'static str = unsafe {&*(key as *const str)};
            let value_length_encoded_size = bincode::serialized_size(&Length(0))? as usize;
            index_data = &index_data[zerobyte+1..];
            let value: Length = bincode::deserialize(&index_data[..value_length_encoded_size])?;
            index_data = &index_data[value_length_encoded_size..];
            index.insert(key, value.0 as usize);
        }

        dbg!(&index);

        Ok(MmapSSTableReader{
            meta: meta,
            mmap: mmap,
            data_start: data_start,
            index_start: index_start,
            index: index,
        })
    }
}

impl SSTableReader for MmapSSTableReader {
    fn get(&self, key: &str) -> Result<Option<&[u8]>> {
        use std::ops::Bound;


        let offset = {
            let mut iter_left = self.index.range::<&str, _>((Bound::Unbounded, Bound::Included(key)));
            let closest_left = iter_left.next_back();
            match closest_left {
                Some((_, offset)) => *offset,
                None => return Ok(None)
            }
        };

        let right_bound = {
            let mut iter_right = self.index.range::<&str, _>((Bound::Excluded(key), Bound::Unbounded));
            let closest_right = iter_right.next_back();
            match closest_right {
                Some((_, offset)) => *offset,
                None => self.index_start as usize
            }
        };

        // let mut data = &self.mmap[self.data_start as usize..self.index_start as usize];
        let mut data = &self.mmap[offset..right_bound];

        let value_length_encoded_size = bincode::serialized_size(&Length(0))? as usize;

        while data.len() > 0 {
            let key_end = match memchr::memchr(b'\0', data) {
                Some(idx) => idx as usize,
                None => return Err(Error::InvalidData("corrupt or buggy sstable"))
            };
            let start_key = std::str::from_utf8(&data[..key_end])?;
            data = &data[key_end+1..];
            let value_length = bincode::deserialize::<Length>(data)?.0 as usize;
            dbg!((start_key, value_length));
            data = &data[value_length_encoded_size..];
            let value = &data[..value_length];
            if value.len() != value_length {
                return Err(Error::InvalidData("corrupt or buggy sstable"));
            }
            if key == start_key {
                return Ok(Some(value))
            }
            data = &data[value_length..];
        }
        return Ok(None)
    }
    fn close(self) -> Result<()> {Ok(())}
}

pub fn open<P: AsRef<Path>>(filename: P) -> Box<dyn SSTableReader> {
    unimplemented!()
}

pub fn write<D: AsRef<[u8]>, P: AsRef<Path>>(
    map: &BTreeMap<String, D>,
    filename: P,
    options: Option<Options>,
) -> Result<()> {
    let options = options.unwrap_or_else(|| Options::default());
    let mut writer = SSTableWriterV1::new(filename, options)?;
    for (key, value) in map.iter() {
        writer.set(key, value.as_ref())?;
    }
    writer.close()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn it_works() {
        use std::collections::BTreeMap;

        let mut map = BTreeMap::new();
        map.insert("foo".into(), b"some foo");
        map.insert("bar".into(), b"some bar");

        write(&mut map, "/tmp/sstable", None).unwrap();

        let reader = MmapSSTableReader::new("/tmp/sstable").unwrap();

        assert_eq!(reader.get("foo").unwrap(), Some(b"some foo" as &[u8]));
        assert_eq!(reader.get("bar").unwrap(), Some(b"some bar" as &[u8]));
        assert_eq!(reader.get("foobar").unwrap(), None);
    }
}
