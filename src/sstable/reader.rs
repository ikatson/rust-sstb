use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::Path;

use bincode;
use memmap;

use memchr;

use super::*;

trait InnerReader {
    fn get(&mut self, key: &str) -> Result<Option<GetResult>>;
}

#[derive(Debug, PartialEq)]
pub enum GetResult<'a> {
    Ref(&'a [u8]),
    Owned(Vec<u8>),
}

impl<'a> GetResult<'a> {
    pub fn as_bytes(&self) -> &[u8] {
        use GetResult::*;
        match self {
            Ref(b) => b,
            Owned(b) => b,
        }
    }
}

impl<'a> AsRef<[u8]> for GetResult<'a> {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

enum MetaData {
    V1_0(MetaV1_0),
}

struct MetaResult {
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
        meta: meta,
        offset: offset,
    })
}

struct MmapSSTableReaderV1_0 {
    mmap: memmap::Mmap,
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
            let string_end = memchr::memchr(0, index_data);
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

        Ok(MmapSSTableReaderV1_0 {
            mmap: mmap,
            index_start: index_start,
            index: index,
        })
    }
}

impl InnerReader for MmapSSTableReaderV1_0 {
    fn get(&mut self, key: &str) -> Result<Option<GetResult>> {
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
            data = &data[value_length_encoded_size..];
            let value = &data[..value_length];
            if value.len() != value_length {
                return Err(Error::InvalidData("corrupt or buggy sstable"));
            }
            if key == start_key {
                return Ok(Some(GetResult::Ref(value)));
            }
            data = &data[value_length..];
        }
        return Ok(None);
    }
}

struct ZlibReaderV1_0 {
    file: File,
    meta: MetaV1_0,
    data_start: u64,
    index: BTreeMap<String, u64>,
}

impl ZlibReaderV1_0 {
    fn new(meta: MetaV1_0, data_start: u64, mut file: File) -> Result<Self> {
        let index_start = data_start + (meta.data_len as u64);

        file.seek(SeekFrom::Start(index_start))?;

        let file_buf_reader = BufReader::new(file);
        let decoder = flate2::read::ZlibDecoder::new(file_buf_reader);
        let mut buf_decoder = BufReader::new(decoder);
        let mut buf = Vec::new();
        let mut index = BTreeMap::new();

        loop {
            let size = buf_decoder.read_until(0, &mut buf)?;
            if size == 0 {
                // Index is read fully.
                break;
            }
            if buf[size - 1] != 0 {
                return Err(Error::InvalidData("corrupt file, no zero"));
            }
            let key = std::str::from_utf8(&buf[..size - 1])?.to_owned();
            let length = bincode::deserialize_from::<_, Length>(&mut buf_decoder)?.0;
            // let mut value = Vec::with_capacity(length as usize);
            // buf_decoder.read_exact(&mut value)?;
            index.insert(key, length);
        }

        // TODO: check that the index size matches metadata
        Ok(ZlibReaderV1_0 {
            file: buf_decoder.into_inner().into_inner().into_inner(),
            data_start: data_start,
            meta: meta,
            index: index,
        })
    }
}

impl InnerReader for ZlibReaderV1_0 {
    fn get(&mut self, key: &str) -> Result<Option<GetResult>> {
        use std::ops::Bound;

        let offset = {
            let mut iter_left = self
                .index
                .range::<str, _>((Bound::Unbounded, Bound::Included(key)));
            let closest_left = iter_left.next_back();
            match closest_left {
                Some((_, offset)) => *offset,
                None => return Ok(None),
            }
        };

        let index_start = self.data_start + self.meta.data_len as u64;

        let right_bound = {
            let mut iter_right = self
                .index
                .range::<str, _>((Bound::Excluded(key), Bound::Unbounded));
            let closest_right = iter_right.next_back();
            match closest_right {
                Some((_, offset)) => *offset,
                None => index_start,
            }
        };

        self.file.seek(SeekFrom::Start(offset))?;

        let reader = BufReader::new(&mut self.file).take(right_bound - offset);
        let zreader = flate2::read::ZlibDecoder::new(reader);
        let mut zreader = BufReader::new(zreader);
        let mut buf = Vec::with_capacity(4096);
        loop {
            buf.truncate(0);
            let size = zreader.read_until(0, &mut buf)?;
            if size == 0 {
                return Ok(None);
            }
            if buf[size - 1] != 0 {
                return Err(Error::InvalidData("stream ended before reading the key"));
            }
            let bytes: &[u8] = &buf[..size - 1];
            if bytes > key.as_bytes() {
                return Ok(None);
            } else {
                let length = bincode::deserialize_from::<_, Length>(&mut zreader)?.0;
                if bytes == key.as_bytes() {
                    // this is "read_to_end" equivalent without zeroing.
                    let value = {
                        let zreader = &mut zreader;
                        let mut buf = Vec::with_capacity(length as usize);
                        let l = zreader.take(length).read_to_end(&mut buf)?;
                        if l < length as usize {
                            return Err(Error::InvalidData("truncated file"));
                        }
                        buf
                    };
                    return Ok(Some(GetResult::Owned(value)));
                }

                // just waste the data
                let mut waste_buf = [0; 8192];
                let mut remaining = length as usize;
                while remaining > 0 {
                    let l = zreader.read(&mut waste_buf[..remaining])?;
                    if l == 0 {
                        return Err(Error::InvalidData("unexpected EOF while reading the file"));
                    }
                    remaining -= l;
                }
            }
        }
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
        // dbg!(&meta, data_start);
        let inner: Box<dyn InnerReader> = match meta.compression {
            Compression::None => Box::new(MmapSSTableReaderV1_0::new(meta, data_start, file)?),
            Compression::Zlib => Box::new(ZlibReaderV1_0::new(meta, data_start, file)?),
        };
        Ok(SSTableReader { inner: inner })
    }
    pub fn get(&mut self, key: &str) -> Result<Option<GetResult>> {
        self.inner.get(key)
    }
}
