# SSTB

[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](https://github.com/ikatson/rust-sstb)
[![Cargo](https://img.shields.io/crates/v/sstb.svg)](https://crates.io/crates/sstb)
[![Documentation](https://docs.rs/sstb/badge.svg)](https://docs.rs/sstb)

An experimental an educational attempt to write a Rust thread-safe sstables library.

See the [documentation](https://docs.rs/sstb) for more details and background.


# TODO ([x] means done)
- [ ] Prettify and publish benchmark results in the readme. For now one can "cargo bench" and look at the reports.
- [x] cache=none does not work. It uses unbounded cache as default which is incorrect.
- [-] open-source
  - [x] write README with badges
  - [ ] Travis tests etc
- [ ] backtraces in errors
- [ ] range queries
- [x] bloom filters on disk
  - they slowed things down by 25% though! but it works
- [ ] writing "flush_every"'s default should depend on the default compression.
- [ ] read cache size configurable both for page cache and for uncompressed cache
- [ ] read cache size should be in bytes, not blocks
- [ ] cache cannot be explicitly disabled in some places
- [ ] add length to encoded bits
- [ ] indexes as separate files
  in this case don't need to maintain the index in memory while writing
- [ ] remove as much as possible unsafe and unwrap
  - [ ] Mmap can be put into an Arc, to remove unsafe static buffer casts. This should not matter at runtime.
- [ ] the index can store the number of items and uncompressed length (in case the file is compressed)
  - the uncompressed length can be used when allocating memory for uncompressed chunks
  - the number of items in the chunk can be used for HashMap capacity IF we get back the "Block" structure which helps not scan the whole table every time.
  - there's a space tradeoff here, so maybe it's all not worth it
- [ ] consider getting back the "Block" trait and its implementations
  - this will help not scan through the chunk on each get()
  - however, there are costs
    - need to allocate HashMaps for lookups
      - if length is not known, might even reallocate
    - messes up the concurrency as the hashmap becomes the contention point
      - an RWLock might help for the majority of the cases
  - even if all this is implemented, it's totally not guaranteed that it's going to be faster in the end.
- [x] u16 keys and u32 values, not u64, for saving space
- [x] mmap with no compression is already multi-threaded, but the API does not
  reflect that
- [x] zlib bounded and unbounded performs the same in benchmarks
- [x] analyze all casts from u64 to usize
  - [x] clippy actually has a lint for it in pedantic
- [x] multi-threading
- [x] compression is all over the place
- [x] files and imports are all over the place, reorganize
- [x] fail if keys or values are too long (> u32_max)
- [x] byte keys
  - [x] also "memchr" is very slow, better to use offsets
