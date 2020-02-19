const FIRST: u8 = b'a';
const LAST: u8 = b'z';

// Todo use the crate's result instead.
use crate::sstable::{Error, Result};

/// An iterator-like object that yields byte slices in sorted order.
///
/// ```rust
/// use sstb::utils::SortedBytesIterator;
/// let mut iter = SortedBytesIterator::new_first_last(3, b'a', b'c', 0).unwrap();

/// let expected = [
///     b"aaa",
///     b"aab",
///     b"aac",
///     b"aba",
///     b"abb",
///     b"abc",
///     b"aca",
///     b"acb",
///     b"acc",
///     b"baa",
///     b"bab",
///     b"bac",
///     b"bba",
///     b"bbb",
///     b"bbc",
///     b"bca",
///     b"bcb",
///     b"bcc",
///     b"caa",
///     b"cab",
///     b"cac",
///     b"cba",
///     b"cbb",
///     b"cbc",
///     b"cca",
///     b"ccb",
///     b"ccc",
/// ];
/// for expected_value in expected.into_iter() {
///     assert_eq!(iter.next(), Some(*expected_value as &[u8]));
/// }
///
/// assert_eq!(iter.next(), None);
/// assert_eq!(iter.next(), None);
/// ```
pub struct SortedBytesIterator {
    buf: Vec<u8>,
    // points to the element being made larger.
    current: usize,
    first: u8,
    last: u8,
    counter: usize,
    limit: usize,
}

impl Clone for SortedBytesIterator {
    fn clone(&self) -> Self {
        let length = self.buf.len();
        Self {
            buf: vec![self.first; length],
            current: length,
            first: self.first,
            last: self.last,
            counter: 0,
            limit: self.limit,
        }
    }
}

impl SortedBytesIterator {
    /// Create a `SortedBytesIterator` that will yield bytestrings of length `limit` with
    /// characters between 'a' and 'z', e.g. if the `limit` is 3, then it will yield
    /// from "aaa" to "zzz"
    pub fn new(length: usize, limit: usize) -> Result<Self> {
        Self::new_first_last(length, FIRST, LAST, limit)
    }

    /// Reset the state of the iterator to initial.
    pub fn reset(&mut self) {
        for v in self.buf.iter_mut() {
            *v = self.first;
        }
        self.current = self.buf.len();
        self.counter = 0;
    }

    /// Customize the bytes being returned.
    pub fn new_first_last(length: usize, first: u8, last: u8, limit: usize) -> Result<Self> {
        if length == 0 {
            return Err(Error::ProgrammingError("length should be greater than 0"));
        }
        if last <= first {
            return Err(Error::ProgrammingError("expected last > first"));
        }
        let buf = vec![first; length];
        Ok(Self {
            buf,
            current: length,
            first,
            last,
            counter: 0,
            limit,
        })
    }

    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<&[u8]> {
        let buflen = self.buf.len();
        if self.limit > 0 && self.counter == self.limit {
            return None;
        }
        if self.current == buflen {
            self.current = buflen - 1;
        } else {
            loop {
                let val = unsafe { self.buf.get_unchecked_mut(self.current) };
                if *val < self.last {
                    *val += 1;
                    for v in self.buf.iter_mut().skip(self.current + 1) {
                        *v = self.first
                    }
                    self.current = buflen - 1;
                    break;
                } else {
                    match self.current {
                        0 => return None,
                        _ => self.current -= 1,
                    }
                }
            }
        }
        self.counter += 1;
        Some(&self.buf)
    }
}
