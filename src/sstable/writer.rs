use std::fs::File;
use std::io::BufWriter;
use std::io::{Seek, SeekFrom, Write};

use std::path::Path;

use bincode;

use super::compression;
use super::compress_ctx_writer::*;
use super::ondisk::*;
use super::options::*;
use super::poswriter::PosWriter;
use super::result::Result;
use super::types::*;


pub trait RawSSTableWriter {
    /// Set the key to the value. This method MUST be called in the sorted
    /// order.
    /// The keys MUST be unique.
    /// Set of empty value is equal to a delete, and is recorded too.
    fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()>;
    /// Close the writer and flush everything to the underlying storage.
    fn close(self) -> Result<()>;
}

/// SSTableWriterV1 writes SSTables to disk.
pub struct SSTableWriterV1 {
    file: PosWriter<Box<dyn CompressionContextWriter<PosWriter<BufWriter<File>>>>>,
    meta: MetaV1_0,
    meta_start: u64,
    data_start: u64,
    flush_every: usize,
    sparse_index: Vec<(Vec<u8>, u64)>,
}

impl SSTableWriterV1 {
    /// Make a new SSTable writer with default options.
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::new_with_options(path, WriteOptions::default())
    }
    /// Make a new SSTable writer with explicit options.
    pub fn new_with_options<P: AsRef<Path>>(path: P, options: WriteOptions) -> Result<Self> {
        let file = File::create(path)?;
        let mut writer = PosWriter::new(BufWriter::new(file), 0);
        writer.write(MAGIC)?;

        bincode::serialize_into(&mut writer, &VERSION_10)?;

        let meta_start = writer.current_offset() as u64;

        let mut meta = MetaV1_0::default();
        meta.compression = options.compression;

        bincode::serialize_into(&mut writer, &meta)?;

        let data_start = writer.current_offset() as u64;

        let file: Box<dyn CompressionContextWriter<PosWriter<BufWriter<File>>>> =
            match options.compression {
                Compression::None => Box::new(UncompressedWriter::new(writer)),
                Compression::Zlib => Box::new(CompressionContextWriterImpl::new(
                    writer,
                    compression::ZlibCompressorFactory::new(None),
                )),
                Compression::Snappy => Box::new(CompressionContextWriterImpl::new(
                    writer,
                    compression::SnappyCompressorFactory::new(),
                )),
            };

        Ok(Self {
            // TODO: this cast is safe, however concerning...
            // maybe PosWriter should be u64 instead of usize?
            file: PosWriter::new(file, data_start as usize),
            meta: meta,
            meta_start: meta_start,
            data_start: data_start,
            flush_every: options.flush_every,
            sparse_index: Vec::new(),
        })
    }
    /// Write all the metadata to the sstable, and flush it.
    pub fn finish(self) -> Result<()> {
        match self {
            SSTableWriterV1 {
                file,
                mut meta,
                meta_start,
                data_start,
                flush_every: _,
                sparse_index,
            } => {
                let mut writer = file.into_inner();
                let index_start = self.data_start + writer.reset_compression_context()? as u64;
                for (key, offset) in sparse_index.into_iter() {
                    KVOffset::new(key.len(), offset)?.serialize_into(&mut writer)?;
                    writer.write_all(&key)?;
                }
                let index_len =
                    self.data_start + writer.reset_compression_context()? as u64 - index_start;
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
    fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        // If the current offset is too high, flush, and add this record to the index.
        //
        // Also reset the compression to a fresh state.
        let approx_msg_len = key.len() + 5 + value.len();
        if self.meta.items == 0 {
            self.sparse_index.push((key.to_owned(), self.data_start));
        } else {
            if self.file.current_offset() + approx_msg_len >= self.flush_every {
                let total_offset =
                    self.data_start + self.file.get_mut().reset_compression_context()? as u64;
                self.file.reset_offset(0);
                self.sparse_index
                    .push((key.to_owned(), total_offset as u64));
            }
        }
        KVLength::new(key.len(), value.len())?.serialize_into(&mut self.file)?;
        self.file.write_all(key)?;
        self.file.write_all(value)?;
        self.meta.items += 1;
        Ok(())
    }

    fn close(self) -> Result<()> {
        self.finish()
    }
}
