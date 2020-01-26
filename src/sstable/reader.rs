use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::Path;

use bincode;
use memmap;

use memchr;

use super::*;

trait InnerReader {
    fn get(&self, key: &str) -> Result<Option<&[u8]>>;
}

enum MetaData {
    V1_0(MetaV1_0),
}

struct MetaResult {
    version: Version,
    meta: MetaData,
    offset: usize,
}

fn read_metadata(file: &mut File) -> Result<MetaResult> {
    file.seek(SeekFrom::Start(0))?;
    let mut reader = posreader::PosReader::new(BufReader::new(file), 0);
    let mut buf = [0; MAGIC.len()];
    if reader.read(&mut buf)? != MAGIC.len() {
        return Err(Error::InvalidData("not an sstable"));
    }
    if buf != MAGIC {
        return Err(Error::InvalidData("not an sstable"));
    }
    let version: Version = bincode::deserialize_from(&mut reader)?;
    let meta = match version {
        VERSION_10 => {
            let meta: MetaV1_0 = bincode::deserialize_from(&mut reader)?;
            MetaData::V1_0(meta)
        }
        _ => return Err(Error::UnsupportedVersion(version)),
    };

    let offset = reader.current_offset();
    let file = reader.into_inner().into_inner();
    file.seek(SeekFrom::Start(offset as u64))?;

    Ok(MetaResult {
        version: version,
        meta: meta,
        offset: offset,
    })
}

struct MmapSSTableReaderV1_0 {
    meta: MetaV1_0,
    mmap: memmap::Mmap,
    data_start: u64,
    index_start: u64,
    // it's not &'static in reality, but it's bound to mmap's lifetime.
    // It will NOT work with compression.
    index: BTreeMap<&'static str, usize>,
}

impl MmapSSTableReaderV1_0 {
    fn new(meta: MetaV1_0, data_start: u64, mut file: File) -> Result<Self> {
        let mmap = unsafe { memmap::MmapOptions::new().map(&mut file) }?;

        let mut index = BTreeMap::new();

        let index_start = data_start + (meta.data_len as u64);

        let mut index_data = &mmap[(index_start as usize)..];
        if index_data.len() != meta.index_len {
            return Err(Error::InvalidData("invalid index length"));
        }

        while index_data.len() > 0 {
            let string_end = memchr::memchr(b'\0', index_data);
            let zerobyte = match string_end {
                Some(idx) => idx,
                None => return Err(Error::InvalidData("corrupt index")),
            };
            let key = std::str::from_utf8(&index_data[..zerobyte])?;
            // Make it &'static
            let key: &'static str = unsafe { &*(key as *const str) };
            let value_length_encoded_size = bincode::serialized_size(&Length(0))? as usize;
            index_data = &index_data[zerobyte + 1..];
            let value: Length = bincode::deserialize(&index_data[..value_length_encoded_size])?;
            index_data = &index_data[value_length_encoded_size..];
            index.insert(key, value.0 as usize);
        }

        // dbg!(&index);

        Ok(MmapSSTableReaderV1_0 {
            meta: meta,
            mmap: mmap,
            data_start: data_start,
            index_start: index_start,
            index: index,
        })
    }
}

impl InnerReader for MmapSSTableReaderV1_0 {
    fn get(&self, key: &str) -> Result<Option<&[u8]>> {
        use std::ops::Bound;

        let offset = {
            let mut iter_left = self
                .index
                .range::<&str, _>((Bound::Unbounded, Bound::Included(key)));
            let closest_left = iter_left.next_back();
            match closest_left {
                Some((_, offset)) => *offset,
                None => return Ok(None),
            }
        };

        let right_bound = {
            let mut iter_right = self
                .index
                .range::<&str, _>((Bound::Excluded(key), Bound::Unbounded));
            let closest_right = iter_right.next_back();
            match closest_right {
                Some((_, offset)) => *offset,
                None => self.index_start as usize,
            }
        };

        // let mut data = &self.mmap[self.data_start as usize..self.index_start as usize];
        let mut data = &self.mmap[offset..right_bound];

        let value_length_encoded_size = bincode::serialized_size(&Length(0))? as usize;

        while data.len() > 0 {
            let key_end = match memchr::memchr(b'\0', data) {
                Some(idx) => idx as usize,
                None => return Err(Error::InvalidData("corrupt or buggy sstable")),
            };
            let start_key = std::str::from_utf8(&data[..key_end])?;
            data = &data[key_end + 1..];
            let value_length = bincode::deserialize::<Length>(data)?.0 as usize;
            // dbg!((start_key, value_length));
            data = &data[value_length_encoded_size..];
            let value = &data[..value_length];
            if value.len() != value_length {
                return Err(Error::InvalidData("corrupt or buggy sstable"));
            }
            if key == start_key {
                return Ok(Some(value));
            }
            data = &data[value_length..];
        }
        return Ok(None);
    }
}

pub struct SSTableReader {
    inner: Box<dyn InnerReader>,
}

impl SSTableReader {
    pub fn new<P: AsRef<Path>>(filename: P) -> Result<Self> {
        let mut file = File::open(filename)?;
        let meta = read_metadata(&mut file)?;
        let data_start = meta.offset as u64;
        let meta = match meta.meta {
            MetaData::V1_0(meta) => meta,
        };
        let inner: Box<dyn InnerReader> = match meta.compression {
            Compression::None => {
                Box::new(MmapSSTableReaderV1_0::new(meta, data_start, file)?)
            },
            Compression::Zlib => {
                unimplemented!("zlib not implemented")
            }
        };
        Ok(SSTableReader{
            inner: inner,
        })
    }
    pub fn get(&self, key: &str) -> Result<Option<&[u8]>> {
        self.inner.get(key)
    }
}