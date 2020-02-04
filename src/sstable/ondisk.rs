use serde::{Deserialize, Serialize};

pub const MAGIC: &[u8] = b"\x80LSM";
pub type KeyLength = u16;
pub type ValueLength = u32;
pub type OffsetLength = u64;

const KEY_LENGTH_SIZE: usize = core::mem::size_of::<KeyLength>();
const VALUE_LENGTH_SIZE: usize = core::mem::size_of::<ValueLength>();
const OFFSET_SIZE: usize = core::mem::size_of::<OffsetLength>();

use super::error::Error;
use super::result::Result;
use super::utils::deserialize_from_eof_is_ok;
use super::types::Compression;
use std::io::{Read, Write};
use std::convert::TryFrom;

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
        KEY_LENGTH_SIZE + VALUE_LENGTH_SIZE
    }
    pub fn serialize_into<W: Write>(&self, w: W) -> Result<()> {
        Ok(bincode::serialize_into(w, self)?)
    }
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct KVOffset {
    pub key_length: KeyLength,
    pub offset: OffsetLength,
}

impl KVOffset {
    pub fn new(k: usize, offset: OffsetLength) -> Result<Self> {
        Ok(Self {
            key_length: KeyLength::try_from(k).map_err(|_| Error::KeyTooLong(k))?,
            offset,
        })
    }
    pub const fn encoded_size() -> usize {
        KEY_LENGTH_SIZE + OFFSET_SIZE
    }
    pub fn deserialize_from_eof_is_ok<R: Read>(r: R) -> Result<Option<Self>> {
        Ok(deserialize_from_eof_is_ok(r)?)
    }
    pub fn serialize_into<W: Write>(&self, w: W) -> Result<()> {
        Ok(bincode::serialize_into(w, self)?)
    }
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct MetaV1_0 {
    pub data_len: u64,
    pub index_len: u64,
    pub items: u64,
    pub compression: Compression,
    // updating this field is done as the last step.
    // it's presence indicates that the file is good.
    pub finished: bool,
    pub checksum: u32,
}
