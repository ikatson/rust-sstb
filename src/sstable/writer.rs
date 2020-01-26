use std::collections::BTreeMap;
use std::fs::File;
use std::io::BufWriter;
use std::io::{Seek, SeekFrom, Write};
use std::mem::MaybeUninit;
use std::path::Path;

use bincode;

use super::*;
use poswriter::PosWriter;

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
    initial_offset: usize,
}

impl<W: Write> ZlibWriter<W> {
    unsafe fn get_mut_encoder(&mut self) -> &mut flate2::write::ZlibEncoder<PosWriter<W>> {
        &mut *self.encoder.as_mut_ptr()
    }
    fn get_flushed_writer(&mut self) -> Result<&PosWriter<W>> {
        let e = unsafe { self.get_mut_encoder() };
        e.flush()?;
        Ok(e.get_ref())
    }
}

impl<W: Write> Write for ZlibWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let e = unsafe { self.get_mut_encoder() };
        e.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let e = unsafe { self.get_mut_encoder() };
        e.flush()
    }
}

impl<W: Write> ZlibWriter<W> {
    fn new(w: PosWriter<W>) -> Self {
        let pos_writer = w;
        let initial_offset = pos_writer.current_offset();
        let encoder = flate2::write::ZlibEncoder::new(pos_writer, flate2::Compression::default());
        ZlibWriter {
            initial_offset: initial_offset,
            encoder: MaybeUninit::new(encoder),
        }
    }
}

impl<W: Write> CompressionContextWriter<W> for ZlibWriter<W> {
    fn relative_offset(&mut self) -> Result<usize> {
        let off = self.initial_offset;
        let w = self.get_flushed_writer()?;
        Ok(off - w.current_offset())
    }
    fn reset_compression_context(&mut self) -> Result<usize> {
        let encoder =
            unsafe { std::mem::replace(&mut self.encoder, MaybeUninit::uninit()).assume_init() };
        let writer = encoder.flush_finish()?;
        let offset = writer.current_offset();
        self.encoder = MaybeUninit::new(flate2::write::ZlibEncoder::new(
            writer,
            flate2::Compression::default(),
        ));
        Ok(offset)
    }
    fn into_inner(self: Box<Self>) -> Result<PosWriter<W>> {
        let encoder = unsafe { self.encoder.assume_init() };
        Ok(encoder.flush_finish()?)
    }
}

pub struct SSTableWriterV1 {
    file: Box<dyn CompressionContextWriter<BufWriter<File>>>,
    meta: MetaV1_0,
    meta_start: usize,
    data_start: usize,
    flush_every: usize,
    sparse_index: BTreeMap<String, usize>,
}

impl SSTableWriterV1 {
    pub fn new<P: AsRef<Path>>(path: P, options: Options) -> Result<Self> {
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
    pub fn write_index(self) -> Result<()> {
        match self {
            SSTableWriterV1 {
                file,
                mut meta,
                meta_start,
                data_start,
                flush_every: _,
                sparse_index,
            } => {
                let mut writer = file;
                let index_start = writer.reset_compression_context()?;
                for (key, value) in sparse_index.into_iter() {
                    writer.write_all(key.as_bytes())?;
                    writer.write_all(b"\0")?;
                    bincode::serialize_into(&mut writer, &Length(value as u64))?;
                }
                let index_len = writer.reset_compression_context()? - index_start;
                meta.finished = true;
                meta.index_len = index_len;
                meta.data_len = index_start - data_start;
                let mut writer = writer.into_inner()?.into_inner();
                writer.seek(SeekFrom::Start(meta_start as u64))?;
                bincode::serialize_into(&mut writer, &meta)?;
                Ok(())
            }
        }
    }
}

impl RawSSTableWriter for SSTableWriterV1 {
    fn set(&mut self, key: &str, value: &[u8]) -> Result<()> {
        // If the current offset is too high, flush, and add this record to the index.
        //
        // Also reset the compression to a fresh state.
        if self.file.relative_offset()? + value.len() >= self.flush_every || self.meta.items == 0 {
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
