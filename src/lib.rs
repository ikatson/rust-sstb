#![warn(
    clippy::all,
    clippy::perf,
    clippy::cast_lossless,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,

    // clippy::restriction,
    // clippy::pedantic,
    // clippy::nursery,
    // clippy::cargo,
)]

//! An experimental an educational attempt to write a Rust thread-safe sstables library.
//!
//! This was created as a learning excercise, to learn Rust, get some fair share of low-level
//! programming, optimization, to learn how sstables work and how to make them faster.
//!
//! By no means this is is complete or has any real-world usage.
//!
//! However inside are some working implementations that pass the tests, are thread-safe,
//! and even run smooth in benchmarks.
//!
//! The API is not stabilized, the disk format is not stabilized, there are no compatibility
//! guarantees or any other guarantees about this library.
//!
//! Use at your own risk.
//!
//! ## How to use
//!
//! For writing SSTables, refer to [writer documentation](./sstable/writer/index.html)
//!
//! For reading SSTables, refer to [reader documentation](./sstable/reader/index.html)
//!
//! ## Quickstart
//!
//! This example will write then read the sstable with all default options.
//!
//! For more efficient reading code, refer to [reader documentation](./sstable/reader/index.html).
//!
//! ```
//! use sstb::*;
//! use std::collections::BTreeMap;
//!
//! let filename = "/tmp/example-sstable";
//! let mut map = BTreeMap::new();
//! map.insert(b"foo", b"some foo");
//! map.insert(b"bar", b"some bar");
//!
//! write_btree_map(&map, filename, None).unwrap();
//!
//! let mut reader =
//!   SSTableReader::new_with_options(filename, &ReadOptions::default())
//!   .unwrap();

//! assert_eq!(reader.get(b"foo").unwrap(), Some(b"some foo" as &[u8]));
//! assert_eq!(reader.get(b"bar").unwrap(), Some(b"some bar" as &[u8]));
//! assert_eq!(reader.get(b"foobar").unwrap(), None);
//! ```

pub mod sstable;
pub mod utils;

pub use sstable::*;

#[cfg(test)]
mod tests {}
