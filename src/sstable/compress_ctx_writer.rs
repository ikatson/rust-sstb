use std::io::Write;
use std::mem::MaybeUninit;

use super::*;
use poswriter::PosWriter;

pub trait CompressionContextWriter<I: Write>: Write {
    fn relative_offset(&mut self) -> Result<usize>;
    fn reset_compression_context(&mut self) -> Result<usize>;
    fn into_inner(self: Box<Self>) -> Result<PosWriter<I>>;
}

pub struct UncompressedWriter<W> {
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

pub struct ZlibWriter<W: Write> {
    encoder: Option<flate2::write::ZlibEncoder<PosWriter<W>>>,
    initial_offset: usize,
}

impl<W: Write> ZlibWriter<W> {
    fn get_mut_encoder(&mut self) -> Result<&mut flate2::write::ZlibEncoder<PosWriter<W>>> {
        Ok(self.encoder.as_mut().ok_or(Error::ProgrammingError("encoder missing"))?)
    }
    fn take_encoder(&mut self) -> Result<flate2::write::ZlibEncoder<PosWriter<W>>> {
        Ok(self.encoder.take().ok_or(Error::ProgrammingError("encoder missing"))?)
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
    pub fn new(w: PosWriter<W>) -> Self {
        let pos_writer = w;
        let initial_offset = pos_writer.current_offset();
        let encoder = flate2::write::ZlibEncoder::new(pos_writer, flate2::Compression::default());
        ZlibWriter {
            initial_offset: initial_offset,
            encoder: Some(encoder),
        }
    }
}

impl<W: Write> CompressionContextWriter<W> for ZlibWriter<W> {
    fn relative_offset(&mut self) -> Result<usize> {
        let initial = self.initial_offset;

        // let current_offset = self.get_mut_encoder()?.get_ref().current_offset();

        // let current_offset = {
        //     let enc = self.get_mut_encoder()?;
        //     enc.flush()?;
        //     enc.get_ref().current_offset()
        // };

        let current_offset = self.get_mut_encoder()?.total_out() as usize;

        Ok(current_offset)
    }
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
    fn into_inner(mut self: Box<Self>) -> Result<PosWriter<W>> {
        let encoder = self.take_encoder()?;
        Ok(encoder.flush_finish()?)
    }
}
