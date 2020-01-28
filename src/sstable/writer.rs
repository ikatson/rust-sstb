use std::collections::BTreeMap;
use std::fs::File;
use std::io::BufWriter;
use std::io::{Seek, SeekFrom, Write};

use std::path::Path;

use bincode;

use super::*;
use poswriter::PosWriter;

use compress_ctx_writer::*;

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
        let mut writer = PosWriter::new(BufWriter::new(file), 0);
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
        let approx_msg_len = key.len() + 5 + value.len();
        if self.file.written_bytes_size_hint()? + approx_msg_len >= self.flush_every || self.meta.items == 0 {
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
