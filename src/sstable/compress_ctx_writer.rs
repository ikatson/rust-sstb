use std::io::Write;

use super::*;
use poswriter::PosWriter;

use snap;

const ENCODER_MISSING: Error = Error::ProgrammingError("encoder missing");

pub trait CompressionContextWriter<I: Write>: Write {
    // Returns number of bytes written so far.
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
        Ok(self.writer.current_offset())
    }
    fn into_inner(self: Box<Self>) -> Result<W> {
        Ok(self.writer.into_inner())
    }
}

pub struct CompressionContextWriterImpl<F, C, W> {
    factory: F,
    encoder: Option<C>,
    _w: std::marker::PhantomData<W>
}

impl<F, C, W> CompressionContextWriterImpl<F, C, W>
    where F: compression::CompressorFactory<PosWriter<W>, C>,
          W: Write,
          C: compression::Compressor<PosWriter<W>>
{
    fn new(writer: W, factory: F) -> Self {
        Self {
            encoder: Some(factory.from_writer(PosWriter::new(writer, 0))),
            factory,
            _w: std::marker::PhantomData{}
        }
    }
    fn get_mut_encoder(&mut self) -> Result<&mut C> {
        Ok(self.encoder.as_mut().ok_or(ENCODER_MISSING)?)
    }
}

impl<F, C, W> Write for CompressionContextWriterImpl<F, C, W>
where F: compression::CompressorFactory<PosWriter<W>, C>,
W: Write,
C: compression::Compressor<PosWriter<W>>
{
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.get_mut_encoder().unwrap().write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.get_mut_encoder().unwrap().flush()
    }
}

impl<F, C, W> CompressionContextWriter<W> for CompressionContextWriterImpl<F, C, W>
    where F: compression::CompressorFactory<PosWriter<W>, C>,
    W: Write,
    C: compression::Compressor<PosWriter<W>>
{
    fn reset_compression_context(&mut self) -> Result<usize> {
        let enc = self.encoder.take().ok_or(ENCODER_MISSING)?;
        let pos_writer = enc.into_inner()?;
        let offset = pos_writer.current_offset();
        self.encoder.replace(self.factory.from_writer(pos_writer));
        Ok(offset)
    }
    fn into_inner(mut self: Box<Self>) -> Result<W> {
        let enc = self.encoder.take().ok_or(ENCODER_MISSING)?;
        Ok(enc.into_inner()?.into_inner())
    }
}





pub struct ZlibWriter<W: Write> {
    encoder: Option<flate2::write::ZlibEncoder<PosWriter<W>>>,
}

impl<W: Write> ZlibWriter<W> {
    fn get_mut_encoder(&mut self) -> Result<&mut flate2::write::ZlibEncoder<PosWriter<W>>> {
        Ok(self
            .encoder
            .as_mut()
            .ok_or(ENCODER_MISSING)?)
    }
    fn take_encoder(&mut self) -> Result<flate2::write::ZlibEncoder<PosWriter<W>>> {
        Ok(self
            .encoder
            .take()
            .ok_or(ENCODER_MISSING)?)
    }
}

impl<W: Write> Write for ZlibWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let e = self.get_mut_encoder().unwrap();
        e.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let e = self.get_mut_encoder().unwrap();
        e.flush()
    }
}

impl<W: Write> ZlibWriter<W> {
    pub fn new(w: W) -> Self {
        let encoder = flate2::write::ZlibEncoder::new(PosWriter::new(w, 0), flate2::Compression::default());
        ZlibWriter {
            encoder: Some(encoder),
        }
    }
}

impl<W: Write> CompressionContextWriter<W> for ZlibWriter<W> {
    fn reset_compression_context(&mut self) -> Result<usize> {
        {
            let enc = self.get_mut_encoder()?;
            if enc.total_in() == 0 {
                return Ok(enc.get_ref().current_offset());
            }
        }
        let encoder = self.take_encoder()?;
        let writer = encoder.finish()?;
        let offset = writer.current_offset();
        self.encoder = Some(flate2::write::ZlibEncoder::new(
            writer,
            flate2::Compression::default(),
        ));
        Ok(offset)
    }
    fn into_inner(mut self: Box<Self>) -> Result<W> {
        let encoder = self.take_encoder()?;
        Ok(encoder.flush_finish()?.into_inner())
    }
}

pub struct SnappyWriter<W: Write> {
    encoder: Option<snap::Writer<PosWriter<W>>>,
}

impl<W: Write> SnappyWriter<W> {
    fn get_mut_encoder(&mut self) -> Result<&mut snap::Writer<PosWriter<W>>> {
        Ok(self.encoder.as_mut().ok_or(ENCODER_MISSING)?)
    }
}

impl<W: Write> Write for SnappyWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.get_mut_encoder().unwrap().write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.get_mut_encoder().unwrap().flush()
    }
}

impl<W: Write> SnappyWriter<W> {
    pub fn new(w: W) -> Self {
        SnappyWriter {
            encoder: Some(snap::Writer::new(PosWriter::new(w, 0)))
        }
    }
}

impl<W: Write> CompressionContextWriter<W> for SnappyWriter<W> {
    fn reset_compression_context(&mut self) -> Result<usize> {
        let enc = self.encoder.take().ok_or(ENCODER_MISSING)?;
        let pos_writer = match enc.into_inner() {
            Ok(writer) => writer,
            Err(e) => {
                let kind = e.error().kind();
                self.encoder.replace(e.into_inner());
                return Err(std::io::Error::from(kind))?
            }
        };
        let offset = pos_writer.current_offset();
        self.encoder.replace(snap::Writer::new(pos_writer));
        Ok(offset)
    }
    fn into_inner(mut self: Box<Self>) -> Result<W> {
        let enc = self.encoder.take().ok_or(ENCODER_MISSING)?;
        let pos_writer = match enc.into_inner() {
            Ok(writer) => writer,
            Err(e) => {
                let kind = e.error().kind();
                self.encoder.replace(e.into_inner());
                return Err(std::io::Error::from(kind))?
            }
        };
        Ok(pos_writer.into_inner())
    }
}

