# TODO
- [x] u32 keys and values, not u64
- [ ] range queries
- [ ] zlib bounded and unbounded performs the same in benchmarks
- [ ] remove all unsafe and unwrap
- [ ] analyze all casts from u64 to usize
- [ ] generic file reading
  - too much duplication
  - variants we have today
    - mmap file read
      - uncompressed
        - caching
          - store seen keys in a hashmap
        - not caching
          - scan every time
      - compressed
        - caching
          - store seen keys in a hashmap
        - not caching
          - read every time
    all these things should be a read option or a write option
    code should be the same??? can it work with both Read APIs and direct memory access??
    - cache original file (ONLY mmap supported now)
    - cache uncompressed blocks
      - what is a block? a 4096 byte region or a part between index values?

    - ALL "dyn" stuff can be removed if the caller creates the objects in the code by himself??
      - hmmm....

    - multi-threading: blocks can be Arc'ed and Weak'ed.
    - compression: 2 caches?
      - first for compressed blocks (makes sense only without mmap)
      - second for uncompressed blocks (makes sense only when compression is on)
- [x] fail if keys or values are too long (> u32_max)
- [x] byte keys
  - [x] also "memchr" is very slow, better to use offsets
- [ ] indexes as separate files
  in this case don't need to maintain in memory while writing