use fixedbitset::FixedBitSet;
use std::ops::Range;

pub struct RangedBitsetIter<'a> {
    bitset: &'a FixedBitSet,
    range: Range<usize>,
    current: usize,
}

impl<'a> RangedBitsetIter<'a> {
    pub fn new(bitset: &'a FixedBitSet, range: Range<usize>) -> Self {
        let current = range.start;
        Self {
            bitset,
            range,
            current,
        }
    }
}

impl<'a> Iterator for RangedBitsetIter<'a> {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        while self.current < self.range.end {
            let index = self.current;
            self.current += 1;
            if self.bitset.contains(index) {
                return Some(index as u64);
            }
        }
        None
    }
}

// This impl is safe because FixedBitset is already Sync and Send
unsafe impl<'a> Send for RangedBitsetIter<'a> {}
unsafe impl<'a> Sync for RangedBitsetIter<'a> {}
