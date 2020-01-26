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
use std::fs::File;
use std::io::BufWriter;
use std::io::{Result, Seek, SeekFrom, Write};
use std::mem::MaybeUninit;
use std::path::Path;

use bincode;
use memmap;
use serde::{Deserialize, Serialize};

const MAGIC: &[u8] = b"\x80LSM";
const VERSION_10: Version = Version { major: 1, minor: 0 };

mod poswriter;

use poswriter::PosWriter;

#[derive(Serialize, Deserialize)]
struct Version {
    major: u16,
    minor: u16,
}

#[derive(Serialize, Deserialize)]
enum Compression {
    None,
    Zlib,
}

impl Default for Compression {
    fn default() -> Self {
        Compression::None
    }
}

#[derive(Serialize, Deserialize)]
struct Length(u64);

#[derive(Serialize, Deserialize, Default)]
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
        Options {
            compression: Compression::None,
            flush_every: 4096,
        }
    }
}

pub trait CompressionContextWriter<I: Write>: Write {
    fn relative_offset(&mut self) -> Result<usize>;
    fn reset_compression_context(&mut self) -> Result<usize>;
    fn into_inner(self) -> Result<PosWriter<I>>;
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
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.writer.write(buf)
    }

    fn flush(&mut self) -> Result<()> {
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
    fn into_inner(self) -> Result<PosWriter<W>> {
        Ok(self.writer)
    }
}

struct ZlibWriter<W: Write> {
    encoder: MaybeUninit<flate2::write::ZlibEncoder<PosWriter<W>>>,
}

impl<W: Write> Write for ZlibWriter<W> {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        unimplemented!()
    }

    fn flush(&mut self) -> Result<()> {
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
    fn into_inner(self) -> Result<PosWriter<W>> {
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
    data_start: usize,
    flush_every: usize,
    sparse_index: BTreeMap<String, usize>,
}

fn bincode_err_into_io_err(e: bincode::Error) -> std::io::Error {
    match *e {
        bincode::ErrorKind::Io(e) => e,
        e => std::io::Error::new(std::io::ErrorKind::Other, "error serializing"),
    }
}

impl SSTableWriterV1 {
    fn new<P: AsRef<Path>>(path: P, options: Options) -> Result<Self> {
        let file = File::open(path)?;
        let writer = BufWriter::new(file);
        let mut writer = PosWriter::new(writer, 0);
        writer.write(MAGIC)?;
        bincode::serialize_into(&mut writer, &VERSION_10).map_err(bincode_err_into_io_err)?;

        let meta_start = writer.current_offset();

        let mut meta = MetaV1_0::default();
        bincode::serialize_into(&mut writer, &meta).map_err(bincode_err_into_io_err)?;

        let data_start = writer.current_offset();

        let file = match options.compression {
            Compression::None => Box::new(UncompressedWriter::new(writer)) as Box<_>,
            Compression::Zlib => Box::new(ZlibWriter::new(writer)) as Box<_>,
        };

        Ok(Self {
            file: file,
            meta: meta,
            data_start: data_start,
            flush_every: options.flush_every,
            sparse_index: BTreeMap::new(),
        })
    }
    fn write_index(self) -> Result<()> {
        unimplemented!()
    }
}

impl RawSSTableWriter for SSTableWriterV1 {
    fn set(&mut self, key: &str, value: &[u8]) -> Result<()> {
        // If the current offset is too high, flush, and add this record to the index.
        //
        // Also reset the compression to a fresh state.
        if self.file.relative_offset()? >= self.flush_every {
            let offset = self.file.reset_compression_context()?;
            self.sparse_index.insert(key.to_owned(), offset);
        }
        self.file.write_all(key.as_bytes())?;
        bincode::serialize_into(&mut self.file, &Length(value.len() as u64)).map_err(bincode_err_into_io_err)?;
        self.file.write_all(value)
    }

    fn close(self) -> Result<()> {
        self.write_index()
    }
}

pub fn open<P: AsRef<Path>>(filename: P) -> Box<dyn SSTableReader> {
    unimplemented!()
}

pub fn write<P: AsRef<Path>>(
    map: BTreeMap<String, Vec<u8>>,
    filename: P,
    options: Option<Options>,
) -> Result<()> {
    let options = options.unwrap_or_else(|| Options::default());
    unimplemented!()
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        //
    }
}
