use super::Result;

use std::io::{Read, Write, Cursor};
use snap;
use super::Error;

pub trait CompressorFactory<W: Write, C: Compressor<W>> {
    fn from_writer(&self, writer: W) -> C;
}

pub trait Compressor<W: Write>: Write {
    fn into_inner(self) -> Result<W>;
}

pub trait Uncompress {
    fn uncompress(&self, buf: &[u8]) -> Result<Vec<u8>>;
}

/// ZLIB
pub struct ZlibCompressorFactory<W: Write> {
    compression: flate2::Compression,
    marker: std::marker::PhantomData<W>
}

pub struct ZlibCompressor<W: Write> {
    inner: flate2::write::ZlibEncoder<W>
}

pub struct ZlibUncompress {}

impl<W: Write> ZlibCompressor<W> {
    pub fn new(writer: W, compression: flate2::Compression) -> Self {
        Self{inner: flate2::write::ZlibEncoder::new(writer, compression)}
    }
}

impl<W: Write> Write for ZlibCompressor<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

impl<W: Write> Compressor<W> for ZlibCompressor<W> {
    fn into_inner(self) -> Result<W> {
        Ok(self.inner.finish()?)
    }
}

impl <W: Write> ZlibCompressorFactory<W> {
    pub fn new(compression: Option<flate2::Compression>) -> Self {
        ZlibCompressorFactory{
            compression: compression.unwrap_or_default(),
            marker: std::marker::PhantomData{}
        }
    }
}

impl<W: Write> CompressorFactory<W, ZlibCompressor<W>> for ZlibCompressorFactory<W> {
    fn from_writer(&self, writer: W) -> ZlibCompressor<W> {
        ZlibCompressor::new(writer, self.compression)
    }
}

impl Uncompress for ZlibUncompress {
    fn uncompress(&self, buf: &[u8]) -> Result<Vec<u8>> {
        let mut dec = flate2::read::ZlibDecoder::new(Cursor::new(buf));
        // TODO: buf.len() here is a bad heuristic. Need the real number, this can be pulled during
        // compression.
        let mut buf = Vec::with_capacity(buf.len());
        dec.read_to_end(&mut buf)?;
        Ok(buf)
    }
}


/// Snappy
pub struct SnappyCompressorFactory<W: Write> {
    marker: std::marker::PhantomData<W>
}

pub struct SnappyCompressor<W: Write> {
    inner: snap::Writer<W>
}


impl<W: Write> SnappyCompressor<W> {
    pub fn new(writer: W) -> Self {
        Self{inner: snap::Writer::new(writer)}
    }
}

pub struct SnappyUncompress {}

impl<W: Write> Write for SnappyCompressor<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

impl<W: Write> Compressor<W> for SnappyCompressor<W> {
    fn into_inner(self) -> Result<W> {
        self.inner.into_inner().map_err(|e| {
            let kind = e.error().kind();
            let io = std::io::Error::from(kind);
            Error::from(io)
        })
    }
}

impl <W: Write> SnappyCompressorFactory<W> {
    pub fn new() -> Self {
        Self{
            marker: std::marker::PhantomData{}
        }
    }
}

impl<W: Write> CompressorFactory<W, SnappyCompressor<W>> for SnappyCompressorFactory<W> {
    fn from_writer(&self, writer: W) -> SnappyCompressor<W> {
        SnappyCompressor::new(writer)
    }
}



impl Uncompress for SnappyUncompress {
    fn uncompress(&self, buf: &[u8]) -> Result<Vec<u8>> {
        let mut dec = snap::Reader::new(Cursor::new(buf));
        // TODO: buf.len() here is a bad heuristic. Need the real number, this can be pulled during
        // compression.
        let mut buf = Vec::with_capacity(buf.len());
        dec.read_to_end(&mut buf)?;
        Ok(buf)
    }
}