//! Ondisk format structs, serialized with "bincode".
//!
//! Ondisk format has the following preamble.
//!
//! | MAGIC: [u8; 4] | version: struct{u16, u16} |
//!
//! Then depending on version the rest of the file is structured.
//! Full v1 format looks like this:
//!
//! | MAGIC: [u8; 4] | Version_1_0: [1u16, 0u16] | Meta_V1_0 | DATA | INDEX_DATA |
//!
//! V1 data has the following layout
//!
//! | KVLength | key: [u8] | value: [u8] |
//!
//! V1 index data has the following layout
//!
//! | KVOffset | key: [u8] | offset: Offset |

use serde::{Deserialize, Serialize};

/// The resulting sstable files MUST have this prefix.
pub const MAGIC: &[u8] = b"\x80LSM";
pub type KeyLength = u16;
pub type ValueLength = u32;
pub type Offset = u64;

use super::error::{Error, INVALID_DATA};
use super::result::Result;
use super::types::Compression;
use super::utils::deserialize_from_eof_is_ok;
use core::mem::size_of;
use std::cmp::{Ord, Ordering};
use std::convert::TryFrom;
use std::io::{Read, Write};

pub use super::types::Version;

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct KVLength {
    pub key_length: KeyLength,
    pub value_length: ValueLength,
}

impl KVLength {
    pub fn new(k: usize, v: usize) -> Result<Self> {
        Ok(Self {
            key_length: KeyLength::try_from(k).map_err(|_| Error::KeyTooLong(k))?,
            value_length: ValueLength::try_from(v).map_err(|_| Error::ValueTooLong(v))?,
        })
    }
    pub const fn encoded_size() -> usize {
        // can't use sizeof Self as bincode has no padding while the struct might.
        size_of::<KeyLength>() + size_of::<ValueLength>()
    }
    pub fn serialize_into<W: Write>(&self, w: W) -> Result<()> {
        Ok(bincode::serialize_into(w, self)?)
    }
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct KVOffset {
    pub key_length: KeyLength,
    pub offset: Offset,
}

impl KVOffset {
    pub fn new(k: usize, offset: Offset) -> Result<Self> {
        Ok(Self {
            key_length: KeyLength::try_from(k).map_err(|_| Error::KeyTooLong(k))?,
            offset,
        })
    }
    pub const fn encoded_size() -> usize {
        // can't use sizeof Self as bincode has no padding while the struct might.
        size_of::<KeyLength>() + size_of::<Offset>()
    }
    pub fn deserialize_from_eof_is_ok<R: Read>(r: R) -> Result<Option<Self>> {
        Ok(deserialize_from_eof_is_ok(r)?)
    }
    pub fn serialize_into<W: Write>(&self, w: W) -> Result<()> {
        Ok(bincode::serialize_into(w, self)?)
    }
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct BloomV2_0 {
    pub bitmap_bits: u64,
    pub k_num: u32,
    pub sip_keys: [(u64, u64); 2],
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct MetaV2_0 {
    pub data_len: u64,
    pub index_len: u64,
    pub bloom_len: u64,
    pub items: u64,
    pub compression: Compression,
    // updating this field is done as the last step.
    // it's presence indicates that the file is good.
    pub finished: bool,
    pub checksum: u32,
    pub bloom: BloomV2_0,
}

/// Find the key in the chunk by scanning sequentially.
///
/// This assumes the chunk was fetched from disk and has V1 ondisk format.
///
/// Returns the start and end index of the value.
///
/// TODO: this probably belongs in "ondisk" for version V1.
pub fn find_value_offset_v2(buf: &[u8], key: &[u8]) -> Result<Option<(usize, usize)>> {
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
