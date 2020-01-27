const FIRST: u8 = b'a';
const LAST: u8 = b'z';

pub struct SortedStringIterator {
    buf: Vec<u8>,
    // points to the element being made larger.
    current: usize,
    first: u8,
    last: u8,
}

impl SortedStringIterator {
    pub fn new(length: usize) -> Self {
        Self::new_first_last(length, FIRST, LAST)
    }
    pub fn new_first_last(length: usize, first: u8, last: u8) -> Self {
        assert!(length > 0);
        assert!(last > first);
        let buf = core::iter::repeat(FIRST).take(length).collect();
        Self {
            buf: buf,
            current: length,
            first: first,
            last: last,
        }
    }

    pub fn next(&mut self) -> Option<&str> {
        let buflen = self.buf.len();
        if self.current == buflen {
            self.current = buflen - 1;
        } else {
            loop {
                let val = unsafe { self.buf.get_unchecked_mut(self.current) };
                if *val < self.last {
                    *val += 1;
                    for v in self.buf.iter_mut().skip(self.current+1) {
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
        return Some(unsafe { std::str::from_utf8_unchecked(&self.buf) });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_sequence() {
        let mut iter = SortedStringIterator::new_first_last(3, b'a', b'c');

        assert_eq!(iter.next(), Some("aaa"));
        assert_eq!(iter.next(), Some("aab"));
        assert_eq!(iter.next(), Some("aac"));
        assert_eq!(iter.next(), Some("aba"));
        assert_eq!(iter.next(), Some("abb"));
        assert_eq!(iter.next(), Some("abc"));
        assert_eq!(iter.next(), Some("aca"));
        assert_eq!(iter.next(), Some("acb"));
        assert_eq!(iter.next(), Some("acc"));
        assert_eq!(iter.next(), Some("baa"));
        assert_eq!(iter.next(), Some("bab"));
        assert_eq!(iter.next(), Some("bac"));
        assert_eq!(iter.next(), Some("bba"));
        assert_eq!(iter.next(), Some("bbb"));
        assert_eq!(iter.next(), Some("bbc"));
        assert_eq!(iter.next(), Some("bca"));
        assert_eq!(iter.next(), Some("bcb"));
        assert_eq!(iter.next(), Some("bcc"));
        assert_eq!(iter.next(), Some("caa"));
        assert_eq!(iter.next(), Some("cab"));
        assert_eq!(iter.next(), Some("cac"));
        assert_eq!(iter.next(), Some("cba"));
        assert_eq!(iter.next(), Some("cbb"));
        assert_eq!(iter.next(), Some("cbc"));
        assert_eq!(iter.next(), Some("cca"));
        assert_eq!(iter.next(), Some("ccb"));
        assert_eq!(iter.next(), Some("ccc"));

        assert_eq!(iter.next(), None);
        assert_eq!(iter.next(), None);
    }
}
