# TODO
- [x] u32 keys and values, not u64
- [ ] range queries
- [ ] fail if keys or values are too long (> u32_max)
- [x] byte keys
  - [x] also "memchr" is very slow, better to use offsets
- [ ] indexes as separate files
  in this case don't need to maintain in memory while writing