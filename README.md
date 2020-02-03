# TODO
- [x] u32 keys and values, not u64
- [ ] writing "flush_every"'s default should depend on the default compression.
- [ ] range queries
- [ ] add length to encoded bits
- [ ] mmap with no compression is already multi-threaded, but the API does not
  reflect that
- [ ] zlib bounded and unbounded performs the same in benchmarks
- [ ] remove as much as possible unsafe and unwrap
- [ ] analyze all casts from u64 to usize
- [ ] multi-threading
- [ ] compression is all over the place
- [ ] files and imports are all over the place, reorganize
- [x] fail if keys or values are too long (> u32_max)
- [x] byte keys
  - [x] also "memchr" is very slow, better to use offsets
- [ ] indexes as separate files
  in this case don't need to maintain in memory while writing