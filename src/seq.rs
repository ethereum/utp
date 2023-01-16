/// A range bounded inclusively below and above that supports wrapping arithmetic.
///
/// If `end < start`, then the range contains all values `x` such that `start <= x <= u16::MAX` and
/// `0 <= x <= end`.
#[derive(Clone, Debug)]
pub struct CircularRangeInclusive {
    start: u16,
    end: u16,
    exhausted: bool,
}

impl CircularRangeInclusive {
    /// Returns a new range.
    pub fn new(start: u16, end: u16) -> Self {
        Self {
            start,
            end,
            exhausted: false,
        }
    }

    /// Returns the start of the range (inclusive).
    pub fn start(&self) -> u16 {
        self.start
    }

    /// Returns the end of the range (inclusive).
    pub fn end(&self) -> u16 {
        self.end
    }

    /// Returns `true` if `item` is contained in the range.
    pub fn contains(&self, item: u16) -> bool {
        if self.end >= self.start {
            item >= self.start && item <= self.end
        } else if item >= self.start {
            true
        } else {
            item <= self.end
        }
    }
}

impl std::iter::Iterator for CircularRangeInclusive {
    type Item = u16;

    fn next(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            None
        } else if self.start == self.end {
            self.exhausted = true;
            Some(self.end)
        } else {
            let step = self.start.wrapping_add(1);
            Some(std::mem::replace(&mut self.start, step))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use quickcheck::{quickcheck, TestResult};

    #[test]
    fn contains_start() {
        fn prop(start: u16, end: u16) -> TestResult {
            let range = CircularRangeInclusive::new(start, end);
            TestResult::from_bool(range.contains(start))
        }
        quickcheck(prop as fn(u16, u16) -> TestResult);
    }

    #[test]
    fn contains_end() {
        fn prop(start: u16, end: u16) -> TestResult {
            let range = CircularRangeInclusive::new(start, end);
            TestResult::from_bool(range.contains(end))
        }
        quickcheck(prop as fn(u16, u16) -> TestResult);
    }

    #[test]
    fn iterator() {
        fn prop(start: u16, end: u16) -> TestResult {
            let range = CircularRangeInclusive::new(start, end);

            let mut len: usize = 0;
            let mut expected_idx = start;
            for idx in range {
                assert_eq!(idx, expected_idx);
                expected_idx = expected_idx.wrapping_add(1);
                len += 1;
            }

            let expected_len = if start <= end {
                usize::from(end - start) + 1
            } else {
                usize::from(u16::MAX - start) + usize::from(end) + 2
            };
            assert_eq!(len, expected_len);

            TestResult::passed()
        }
        quickcheck(prop as fn(u16, u16) -> TestResult);
    }

    #[test]
    fn iterator_single() {
        fn prop(x: u16) -> TestResult {
            let mut range = CircularRangeInclusive::new(x, x);
            assert_eq!(range.next(), Some(x));
            assert!(range.next().is_none());

            TestResult::passed()
        }
        quickcheck(prop as fn(u16) -> TestResult);
    }
}
