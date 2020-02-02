const FIRST: u8 = b'a';
const LAST: u8 = b'z';

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
        Self::new_first_last(self.buf.len(), self.first, self.last, self.limit)
    }
}

impl SortedBytesIterator {
    pub fn new(length: usize, limit: usize) -> Self {
        Self::new_first_last(length, FIRST, LAST, limit)
    }
    pub fn reset(&mut self) {
        for v in self.buf.iter_mut() {
            *v = self.first;
        }
        self.current = self.buf.len();
        self.counter = 0;
    }
    pub fn new_first_last(length: usize, first: u8, last: u8, limit: usize) -> Self {
        assert!(length > 0);
        assert!(last > first);
        let buf = core::iter::repeat(first).take(length).collect();
        Self {
            buf,
            current: length,
            first,
            last,
            counter: 0,
            limit,
        }
    }

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
        return Some(&self.buf);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_sequence() {
        let mut iter = SortedBytesIterator::new_first_last(3, b'a', b'c', 0);

        assert_eq!(iter.next(), Some(b"aaa" as &[u8]));
        assert_eq!(iter.next(), Some(b"aab" as &[u8]));
        assert_eq!(iter.next(), Some(b"aac" as &[u8]));
        assert_eq!(iter.next(), Some(b"aba" as &[u8]));
        assert_eq!(iter.next(), Some(b"abb" as &[u8]));
        assert_eq!(iter.next(), Some(b"abc" as &[u8]));
        assert_eq!(iter.next(), Some(b"aca" as &[u8]));
        assert_eq!(iter.next(), Some(b"acb" as &[u8]));
        assert_eq!(iter.next(), Some(b"acc" as &[u8]));
        assert_eq!(iter.next(), Some(b"baa" as &[u8]));
        assert_eq!(iter.next(), Some(b"bab" as &[u8]));
        assert_eq!(iter.next(), Some(b"bac" as &[u8]));
        assert_eq!(iter.next(), Some(b"bba" as &[u8]));
        assert_eq!(iter.next(), Some(b"bbb" as &[u8]));
        assert_eq!(iter.next(), Some(b"bbc" as &[u8]));
        assert_eq!(iter.next(), Some(b"bca" as &[u8]));
        assert_eq!(iter.next(), Some(b"bcb" as &[u8]));
        assert_eq!(iter.next(), Some(b"bcc" as &[u8]));
        assert_eq!(iter.next(), Some(b"caa" as &[u8]));
        assert_eq!(iter.next(), Some(b"cab" as &[u8]));
        assert_eq!(iter.next(), Some(b"cac" as &[u8]));
        assert_eq!(iter.next(), Some(b"cba" as &[u8]));
        assert_eq!(iter.next(), Some(b"cbb" as &[u8]));
        assert_eq!(iter.next(), Some(b"cbc" as &[u8]));
        assert_eq!(iter.next(), Some(b"cca" as &[u8]));
        assert_eq!(iter.next(), Some(b"ccb" as &[u8]));
        assert_eq!(iter.next(), Some(b"ccc" as &[u8]));

        assert_eq!(iter.next(), None);
        assert_eq!(iter.next(), None);
    }
}
