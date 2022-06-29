use roaring::*;
use std::ops::Range;

//-----------------------------------------

pub struct RunIter {
    len: u32,
    current: u32,
    bits: RoaringBitmap,
}

impl RunIter {
    pub fn new(bits: RoaringBitmap, len: u32) -> Self {
        Self {
            len,
            current: 0,
            bits,
        }
    }
}

impl Iterator for RunIter {
    type Item = (bool, Range<u32>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.current == self.len {
            None
        } else {
            let b = self.bits.contains(self.current);
            let start = self.current;
            self.current += 1;
            while self.current < self.len && self.bits.contains(self.current) == b {
                self.current += 1;
            }
            Some((b, start..self.current))
        }
    }
}

//-----------------------------------------

#[cfg(test)]
mod run_iter_tests {
    use super::*;

    struct Test {
        bits: Vec<bool>,
        expected: Vec<(bool, Range<u32>)>,
    }

    #[test]
    fn test_run_iter() {
        let tests = vec![
            Test {
                bits: vec![],
                expected: vec![],
            },
            Test {
                bits: vec![false, false, false],
                expected: vec![(false, 0..3)],
            },
            Test {
                bits: vec![false, true, true, false, false, false, true],
                expected: vec![(false, 0..1), (true, 1..3), (false, 3..6), (true, 6..7)],
            },
            Test {
                bits: vec![false, true, true, false, false, false, true, false, false],
                expected: vec![
                    (false, 0..1),
                    (true, 1..3),
                    (false, 3..6),
                    (true, 6..7),
                    (false, 7..9),
                ],
            },
        ];

        for t in tests {
            let mut bits = RoaringBitmap::new();
            for (i, b) in t.bits.iter().enumerate() {
                if *b {
                    bits.insert(i as u32);
                }
            }

            let it = RunIter::new(bits, t.bits.len() as u32);
            let actual: Vec<(bool, Range<u32>)> = it.collect();
            assert_eq!(actual, t.expected);
        }
    }
}

//-----------------------------------------
