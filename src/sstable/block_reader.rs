use crate::sstable::*;

use std::cmp::{Ord, Ordering};
use super::ondisk::KVLength;

pub fn find_key_offset(buf: &[u8], key: &[u8]) -> Result<Option<(usize, usize)>> {
    macro_rules! buf_get {
        ($x:expr) => {{
            buf.get($x).ok_or(INVALID_DATA)?
        }};
    }

    let kvlen_encoded_size = KVLength::encoded_size();

    let mut offset = 0;
    while offset < buf.len() {
        let kvlength = bincode::deserialize::<KVLength>(&buf)?;
        let (start_key, cursor) = {
            let key_start = offset + kvlen_encoded_size;
            let key_end = key_start + kvlength.key_length as usize;
            (buf_get!(key_start..key_end), key_end)
        };

        let (start, end) = {
            let value_end = cursor + kvlength.value_length as usize;
            (cursor, value_end)
        };
        offset = end;

        match start_key.cmp(key) {
            Ordering::Equal => {
                return Ok(Some((start, end)));
            }
            Ordering::Greater => return Ok(None),
            Ordering::Less => continue,
        }
    }
    return Ok(None);
}

pub struct ReferenceBlock<'a> {
    buf: &'a [u8],
}

impl<'r> ReferenceBlock<'r> {
    pub fn new(b: &'r [u8]) -> Self {
        Self { buf: b }
    }
}

impl<'r> ReferenceBlock<'r> {
    pub fn find_key_rb<'a, 'b>(&'a self, key: &[u8]) -> Result<Option<&'r [u8]>> {
        if let Some((start, end)) = find_key_offset(&self.buf, key)? {
            self.buf
                .get(start..end)
                .ok_or(INVALID_DATA)
                .map(|v| Some(v))
        } else {
            Ok(None)
        }
    }
}
