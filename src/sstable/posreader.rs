use std::io::{Read, Result};

/// PosReader is a reader that remembers the position read and can
/// report it back at any time.
#[derive(Debug)]
pub struct PosReader<R> {
    r: R,
    offset: usize,
}

impl<R> PosReader<R> {
    pub fn new(r: R, offset: usize) -> Self {
        PosReader {
            r,
            offset,
        }
    }
    pub fn current_offset(&self) -> usize {
        self.offset
    }
    pub fn into_inner(self) -> R {
        self.r
    }
}

impl<R: Read> Read for PosReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let l = self.r.read(buf)?;
        self.offset += l;
        Ok(l)
    }
}
