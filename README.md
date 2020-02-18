# SSTB

[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](https://github.com/ikatson/rust-sstb)
[![Cargo](https://img.shields.io/crates/v/sstb.svg)](https://crates.io/crates/sstb)
[![Documentation](https://docs.rs/sstb/badge.svg)](https://docs.rs/sstb)

An experimental an educational attempt to write a Rust thread-safe sstables library.

See the [documentation](https://docs.rs/sstb) for more details and background.

## How to use
For writing SSTables, refer to [writer documentation](https://docs.rs/sstb/0.2.1-alpha/sstb/sstable/writer/index.html)

For reading SSTables, refer to [reader documentation](https://docs.rs/sstb/0.2.1-alpha/sstb/sstable/reader/index.html)

## Quickstart

This example will write then read the sstable with all default options with a single-threaded.

For more efficient, concurrent reading code, refer to [reader documentation](https://docs.rs/sstb/0.2.1-alpha/sstb/sstable/reader/index.html).

```rust
use sstb::*;
use std::collections::BTreeMap;

let filename = "/tmp/example-sstable";
let mut map = BTreeMap::new();
map.insert(b"foo", b"some foo");
map.insert(b"bar", b"some bar");

write_btree_map(&map, filename, None).unwrap();

// This example does not use multiple threads, so it's ok to use
// SSTableReader instead of ConcurrentSSTableReader.
let mut reader =
  SSTableReader::new_with_options(filename, &ReadOptions::default())
  .unwrap();
assert_eq!(reader.get(b"foo").unwrap(), Some(b"some foo" as &[u8]));
assert_eq!(reader.get(b"bar").unwrap(), Some(b"some bar" as &[u8]));
assert_eq!(reader.get(b"foobar").unwrap(), None);
```