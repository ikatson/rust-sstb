use std::io::Write;

use super::compression::*;
use super::{Error, Result};
use super::poswriter::PosWriter;
use std::convert::TryFrom;

const COMPRESSOR_MISSING: Error = Error::ProgrammingError("compressor missing");

/// A writer that maybe compresses the input.
///
/// The difference with a regular writer, is that if you call reset_compression_context()
/// all compression state will be reset and flushed, and the offset in the underlying
/// writer will be returned.
pub trait CompressionContextWriter<I: Write>: Write {
    /// Reset and flush compression state.
    ///
    /// It must be possible to read from the returned offset with a newly
    /// created decompressor.
    ///
    /// Returns number of bytes written so far, i.e. relative offset
    /// from the creation of Self.
    fn reset_compression_context(&mut self) -> Result<usize>;
    fn into_inner(self: Box<Self>) -> Result<I>;
}

pub struct UncompressedWriter<W> {
    writer: PosWriter<W>,
}

impl<W> UncompressedWriter<W> {
    pub fn new(writer: W) -> Self {
        UncompressedWriter {
            writer: PosWriter::new(writer, 0),
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
    fn reset_compression_context(&mut self) -> Result<usize> {
        Ok(usize::try_from(self.writer.current_offset())?)
    }
    fn into_inner(self: Box<Self>) -> Result<W> {
        Ok(self.writer.into_inner())
    }
}

/// A version of CompressionContextWriter that knows
/// how to create new compressors (encoders) from a factory.
pub struct CompressionContextWriterImpl<F, C, W> {
    factory: F,
    compressor: Option<C>,
    _w: std::marker::PhantomData<W>,
}

impl<F, C, W> CompressionContextWriterImpl<F, C, W>
where
    F: CompressorFactory<PosWriter<W>, C>,
    W: Write,
    C: Compressor<PosWriter<W>>,
{
    pub fn new(writer: W, factory: F) -> Self {
        Self {
            compressor: Some(factory.from_writer(PosWriter::new(writer, 0))),
            factory,
            _w: std::marker::PhantomData {},
        }
    }
    fn get_mut_compressor(&mut self) -> Result<&mut C> {
        Ok(self.compressor.as_mut().ok_or(COMPRESSOR_MISSING)?)
    }
}

impl<F, C, W> Write for CompressionContextWriterImpl<F, C, W>
where
    F: CompressorFactory<PosWriter<W>, C>,
    W: Write,
    C: Compressor<PosWriter<W>>,
{
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.get_mut_compressor().unwrap().write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.get_mut_compressor().unwrap().flush()
    }
}

impl<F, C, W> CompressionContextWriter<W> for CompressionContextWriterImpl<F, C, W>
where
    F: CompressorFactory<PosWriter<W>, C>,
    W: Write,
    C: Compressor<PosWriter<W>>,
{
    fn reset_compression_context(&mut self) -> Result<usize> {
        let enc = self.compressor.take().ok_or(COMPRESSOR_MISSING)?;
        let pos_writer = enc.into_inner()?;
        let offset = pos_writer.current_offset();
        self.compressor.replace(self.factory.from_writer(pos_writer));
        Ok(usize::try_from(offset)?)
    }
    fn into_inner(mut self: Box<Self>) -> Result<W> {
        let enc = self.compressor.take().ok_or(COMPRESSOR_MISSING)?;
        Ok(enc.into_inner()?.into_inner())
    }
}
