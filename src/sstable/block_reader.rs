use super::*;

use std::cmp::{Ord, Ordering};

pub struct ReferenceBlock<'a> {
    buf: &'a [u8],
}

impl<'r> ReferenceBlock<'r> {
    pub fn new(b: &'r [u8]) -> Self {
        Self{buf: b}
    }
}

impl<'r> ReferenceBlock<'r> {
    pub fn find_key_rb<'a, 'b>(&'a self, key: &[u8]) -> Result<Option<&'r [u8]>> {
        macro_rules! buf_get {
            ($x:expr) => {{
                self.buf
                    .get($x)
                    .ok_or(INVALID_DATA)?
            }};
        }

        let kvlen_encoded_size = KVLength::encoded_size();

        let mut offset = 0;
        while offset < self.buf.len() {
            let kvlength = bincode::deserialize::<KVLength>(&self.buf)?;
            let (start_key, cursor) = {
                let key_start = offset + kvlen_encoded_size;
                let key_end = key_start + kvlength.key_length as usize;
                (buf_get!(key_start..key_end), key_end)
            };

            let (value, cursor) = {
                let value_end = cursor + kvlength.value_length as usize;
                let value = buf_get!(cursor..value_end);
                (value, value_end)
            };
            offset = cursor;

            match start_key.cmp(key) {
                Ordering::Equal => {
                    return Ok(Some(value));
                },
                Ordering::Greater => {
                    return Ok(None)
                }
                Ordering::Less => continue
            }
        }
        return Ok(None);
    }
}