use std::io::{Result, Write};

// PosWriter is a Writer that remembers the position and can report it at any time.
#[derive(Debug)]
pub struct PosWriter<W> {
    w: W,
    offset: u64,
}

impl<W> PosWriter<W> {
    pub fn new(w: W, offset: u64) -> Self {
        PosWriter { w, offset }
    }
    pub fn current_offset(&self) -> u64 {
        self.offset
    }
    pub fn reset_offset(&mut self, offset: u64) {
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
        self.offset += l as u64;
        Ok(l)
    }

    fn flush(&mut self) -> Result<()> {
        self.w.flush()
    }
}
