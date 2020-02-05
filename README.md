# TODO
- [ ] Mmap can be put into an Rc or an Arc, to remove unsafe static buffer casts.
- [x] u32 keys and values, not u64
- [ ] bloom filters on disk
- [ ] writing "flush_every"'s default should depend on the default compression.
- [ ] read cache size configurable both for page cache and for uncompressed cache
- [ ] read cache size should be in bytes, not blocks
- [ ] range queries
- [ ] cache cannot be explicitly disabled in some places
- [ ] add length to encoded bits
- [x] mmap with no compression is already multi-threaded, but the API does not
  reflect that
- [x] zlib bounded and unbounded performs the same in benchmarks
- [ ] remove as much as possible unsafe and unwrap
- [x] analyze all casts from u64 to usize
  - [x] clippy actually has a lint for it in pedantic
- [x] multi-threading
- [x] compression is all over the place
- [ ] files and imports are all over the place, reorganize
- [x] fail if keys or values are too long (> u32_max)
- [x] byte keys
  - [x] also "memchr" is very slow, better to use offsets
- [ ] indexes as separate files
  in this case don't need to maintain in memory while writing