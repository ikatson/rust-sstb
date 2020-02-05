use crate::sstable::*;

use std::cmp::{Ord, Ordering};
use super::ondisk::KVLength;

/// Find the key in the chunk by scanning sequentially.
///
/// This assumes the chunk was fetched from disk and has V1 ondisk format.
///
/// Returns the start and end index of the value.
pub fn find_value_offset_v1(buf: &[u8], key: &[u8]) -> Result<Option<(usize, usize)>> {
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
    Ok(None)
}

/// A "Block" is a remainder from one of the original implementations.
/// The Block had some internal state that cached the reads so that
/// sequential scan is not performed on every key access.
///
/// The "ReferenceBlock" was the implementation that did not cache anything.
/// With the current code layout, it does not make sense to have this at all,
/// and instead a mere function would be simpler.
///
/// However, it's not clear yet that this is the way to go, and maybe actually caching
/// the found values might still make sense, this needs to be benchmarked.
pub struct ReferenceBlock<'a> {
    buf: &'a [u8],
}

impl<'r> ReferenceBlock<'r> {
    pub fn new(b: &'r [u8]) -> Self {
        Self { buf: b }
    }
}

impl<'r> ReferenceBlock<'r> {
    pub fn find_key_rb<'a>(&'a self, key: &[u8]) -> Result<Option<&'r [u8]>> {
        if let Some((start, end)) = find_value_offset_v1(&self.buf, key)? {
            self.buf
                .get(start..end)
                .ok_or(INVALID_DATA)
                .map(Some)
        } else {
            Ok(None)
        }
    }
}
