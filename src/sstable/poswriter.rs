use std::io::{Result, Write};

#[derive(Debug)]
pub struct PosWriter<W> {
    w: W,
    offset: usize,
}

impl<W> PosWriter<W> {
    pub fn new(w: W, offset: usize) -> Self {
        PosWriter {
            w,
            offset,
        }
    }
    pub fn current_offset(&self) -> usize {
        self.offset
    }
    pub fn reset_offset(&mut self, offset: usize) {
        self.offset = offset;
    }
    pub fn get_mut(&mut self) -> &mut W {
        &mut self.w
    }
    pub fn into_inner(self) -> W {
        self.w
    }
}

impl<W: Write> Write for PosWriter<W> {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let l = self.w.write(buf)?;
        self.offset += l;
        Ok(l)
    }

    fn flush(&mut self) -> Result<()> {
        self.w.flush()
    }
}
