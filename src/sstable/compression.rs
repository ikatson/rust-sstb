use super::Result;

use std::io::{Read, Write, Cursor};

pub trait CompressorFactory<W: Write, C: Compressor<W>> {
    fn from_writer(&self, writer: W) -> C;
}

pub trait DecompressorFactory<R: Read, D: Decompressor<R>> {
    fn from_reader(&self, reader: R) -> D;
}

pub trait Compressor<W: Write>: Write {
    fn into_inner(self) -> Result<W>;
}

pub trait Decompressor<R: Read>: Read {
    fn into_inner(self) -> Result<R>;
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

pub struct ZlibDecompressorFactory<R: Read> {
    marker: std::marker::PhantomData<R>
}

pub struct ZlibDecompressor<R: Read> {
    inner: flate2::read::ZlibDecoder<R>
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

impl<R: Read> ZlibDecompressor<R> {
    pub fn new(reader: R) -> Self {
        ZlibDecompressor{inner: flate2::read::ZlibDecoder::new(reader)}
    }
}
impl <R: Read> Read for ZlibDecompressor<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buf)
    }
}
impl <R: Read> Decompressor<R> for ZlibDecompressor<R> {
    fn into_inner(self) -> Result<R> {
        Ok(self.inner.into_inner())
    }
}
impl <R: Read> DecompressorFactory<R, ZlibDecompressor<R>> for ZlibDecompressorFactory<R> {
    fn from_reader(&self, reader: R) -> ZlibDecompressor<R> {
        ZlibDecompressor::new(reader)
    }
}
impl <R: Read> ZlibDecompressorFactory<R> {
    pub fn new() -> Self {
        ZlibDecompressorFactory{marker: std::marker::PhantomData{}}
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

pub struct SnappyUncompress {}

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