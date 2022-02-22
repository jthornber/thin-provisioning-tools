use anyhow::Result;
use fixedbitset::FixedBitSet;
use num_traits::Bounded;
use std::boxed::Box;
use std::convert::{TryFrom, TryInto};
use std::sync::{Arc, Mutex};

//------------------------------------------

pub trait SpaceMap {
    fn get_nr_blocks(&self) -> Result<u64>;
    fn get_nr_allocated(&self) -> Result<u64>;
    fn get(&self, b: u64) -> Result<u32>;

    /// Returns the old ref count
    fn set(&mut self, b: u64, v: u32) -> Result<u32>;

    fn inc(&mut self, begin: u64, len: u64) -> Result<()>;

    /// Returns true if the block is now free
    fn dec(&mut self, b: u64) -> Result<bool> {
        let old = self.get(b)?;
        assert!(old > 0);
        self.set(b, old - 1)?;

        Ok(old == 1)
    }

    /// Finds a block with a zero reference count. Increments the count.
    /// Returns Ok(None) if no free block (ENOSPC)
    /// Returns Err on fatal error
    fn alloc(&mut self) -> Result<Option<u64>>;

    /// Finds a free block within the range
    fn find_free(&mut self, begin: u64, end: u64) -> Result<Option<u64>>;

    /// Returns the position where allocation starts
    fn get_alloc_begin(&self) -> Result<u64>;
}

pub type ASpaceMap = Arc<Mutex<dyn SpaceMap + Sync + Send>>;

//------------------------------------------

pub struct CoreSpaceMap<T> {
    nr_allocated: u64,
    alloc_begin: u64,
    counts: Vec<T>,
}

impl<V> CoreSpaceMap<V>
where
    V: Copy + Default + std::ops::AddAssign + From<u8>,
{
    pub fn new(nr_entries: u64) -> CoreSpaceMap<V> {
        CoreSpaceMap {
            nr_allocated: 0,
            alloc_begin: 0,
            counts: vec![V::default(); nr_entries as usize],
        }
    }
}

impl<V> SpaceMap for CoreSpaceMap<V>
where
    V: Copy
        + Default
        + Eq
        + std::ops::AddAssign
        + From<u8>
        + Into<u32>
        + Bounded
        + TryFrom<u32>
        + std::cmp::PartialOrd,
    <V as TryFrom<u32>>::Error: std::fmt::Debug,
{
    fn get_nr_blocks(&self) -> Result<u64> {
        Ok(self.counts.len() as u64)
    }

    fn get_nr_allocated(&self) -> Result<u64> {
        Ok(self.nr_allocated)
    }

    fn get(&self, b: u64) -> Result<u32> {
        Ok(self.counts[b as usize].into())
    }

    fn set(&mut self, b: u64, v: u32) -> Result<u32> {
        let old = self.counts[b as usize];
        assert!(v <= V::max_value().into());
        self.counts[b as usize] = v.try_into().unwrap(); // FIXME: do not panic

        if old == V::from(0u8) && v != 0 {
            self.nr_allocated += 1;
        } else if old != V::from(0u8) && v == 0 {
            self.nr_allocated -= 1;
        }

        Ok(old.into())
    }

    fn inc(&mut self, begin: u64, len: u64) -> Result<()> {
        for b in begin..(begin + len) {
            let c = &mut self.counts[b as usize];
            assert!(*c < V::max_value());
            if *c == V::from(0u8) {
                // FIXME: can we get a ref to save dereferencing counts twice?
                self.nr_allocated += 1;
                *c = V::from(1u8);
            } else {
                *c += V::from(1u8);
            }
        }
        Ok(())
    }

    fn alloc(&mut self) -> Result<Option<u64>> {
        let mut b = self.find_free(self.alloc_begin, self.counts.len() as u64)?;
        if b.is_none() {
            b = self.find_free(0, self.alloc_begin)?;
            if b.is_none() {
                return Ok(None);
            }
        }

        self.counts[b.unwrap() as usize] = V::from(1u8);
        self.nr_allocated += 1;
        self.alloc_begin = b.unwrap() + 1;

        Ok(b)
    }

    fn find_free(&mut self, begin: u64, end: u64) -> Result<Option<u64>> {
        for b in begin..end {
            if self.counts[b as usize] == V::from(0u8) {
                return Ok(Some(b));
            }
        }
        Ok(None)
    }

    fn get_alloc_begin(&self) -> Result<u64> {
        Ok(self.alloc_begin as u64)
    }
}

pub fn core_sm(nr_entries: u64, max_count: u32) -> Arc<Mutex<dyn SpaceMap + Send + Sync>> {
    if max_count <= u8::MAX as u32 {
        Arc::new(Mutex::new(CoreSpaceMap::<u8>::new(nr_entries)))
    } else if max_count <= u16::MAX as u32 {
        Arc::new(Mutex::new(CoreSpaceMap::<u16>::new(nr_entries)))
    } else {
        Arc::new(Mutex::new(CoreSpaceMap::<u32>::new(nr_entries)))
    }
}

pub fn core_sm_without_mutex(nr_entries: u64, max_count: u32) -> Box<dyn SpaceMap> {
    if max_count <= u8::MAX as u32 {
        Box::new(CoreSpaceMap::<u8>::new(nr_entries))
    } else if max_count <= u16::MAX as u32 {
        Box::new(CoreSpaceMap::<u16>::new(nr_entries))
    } else {
        Box::new(CoreSpaceMap::<u32>::new(nr_entries))
    }
}

//------------------------------------------

// This in core space map can only count to one, useful when walking
// btrees when we want to avoid visiting a node more than once, but
// aren't interested in counting how many times we've visited.
pub struct RestrictedSpaceMap {
    nr_allocated: u64,
    alloc_begin: usize,
    counts: FixedBitSet,
}

impl RestrictedSpaceMap {
    pub fn new(nr_entries: u64) -> RestrictedSpaceMap {
        RestrictedSpaceMap {
            nr_allocated: 0,
            counts: FixedBitSet::with_capacity(nr_entries as usize),
            alloc_begin: 0,
        }
    }
}

impl SpaceMap for RestrictedSpaceMap {
    fn get_nr_blocks(&self) -> Result<u64> {
        Ok(self.counts.len() as u64)
    }

    fn get_nr_allocated(&self) -> Result<u64> {
        Ok(self.nr_allocated)
    }

    fn get(&self, b: u64) -> Result<u32> {
        if self.counts.contains(b as usize) {
            Ok(1)
        } else {
            Ok(0)
        }
    }

    fn set(&mut self, b: u64, v: u32) -> Result<u32> {
        let old = self.counts.contains(b as usize);

        if v > 0 {
            if !old {
                self.nr_allocated += 1;
            }
            self.counts.insert(b as usize);
        } else {
            if old {
                self.nr_allocated -= 1;
            }
            self.counts.set(b as usize, false);
        }

        Ok(if old { 1 } else { 0 })
    }

    fn inc(&mut self, begin: u64, len: u64) -> Result<()> {
        for b in begin..(begin + len) {
            if !self.counts.contains(b as usize) {
                self.nr_allocated += 1;
                self.counts.insert(b as usize);
            }
        }
        Ok(())
    }

    fn alloc(&mut self) -> Result<Option<u64>> {
        let mut b = self.find_free(self.alloc_begin as u64, self.counts.len() as u64)?;
        if b.is_none() {
            b = self.find_free(0, self.alloc_begin as u64)?;
            if b.is_none() {
                return Ok(None);
            }
        }

        self.counts.insert(b.unwrap() as usize);
        self.nr_allocated += 1;
        self.alloc_begin = b.unwrap() as usize + 1;

        Ok(b)
    }

    fn find_free(&mut self, begin: u64, end: u64) -> Result<Option<u64>> {
        for b in begin..end {
            if !self.counts.contains(b as usize) {
                return Ok(Some(b));
            }
        }
        Ok(None)
    }

    fn get_alloc_begin(&self) -> Result<u64> {
        Ok(self.alloc_begin as u64)
    }
}

//------------------------------------------

// This in core space map can only count to two.
// It's useful when we want to know which blocks
// are shared as a result of btree visiting, and
// aren't interested in the actual reference counts.
pub struct RestrictedTwoSpaceMap {
    nr_allocated: u64,
    alloc_begin: usize,
    counts: FixedBitSet,
}

impl RestrictedTwoSpaceMap {
    pub fn new(nr_entries: u64) -> RestrictedTwoSpaceMap {
        RestrictedTwoSpaceMap {
            nr_allocated: 0,
            counts: FixedBitSet::with_capacity((nr_entries << 1) as usize),
            alloc_begin: 0,
        }
    }
}

impl SpaceMap for RestrictedTwoSpaceMap {
    fn get_nr_blocks(&self) -> Result<u64> {
        Ok((self.counts.len() >> 1) as u64)
    }

    fn get_nr_allocated(&self) -> Result<u64> {
        Ok(self.nr_allocated)
    }

    fn get(&self, b: u64) -> Result<u32> {
        let idx = (b << 1) as usize;
        if self.counts.contains(idx) {
            Ok(1)
        } else if self.counts.contains(idx + 1) {
            Ok(2)
        } else {
            Ok(0)
        }
    }

    fn set(&mut self, b: u64, v: u32) -> Result<u32> {
        let old = self.get(b)?;

        if v > 0 {
            if old == 0 {
                self.nr_allocated += 1;
            }
            let idx = (b << 1) as usize;
            if v == 1 {
                self.counts.insert(idx);
                self.counts.set(idx + 1, false);
            } else if v == 2 {
                self.counts.set(idx, false);
                self.counts.insert(idx + 1);
            }
        } else {
            if old == 1 {
                self.nr_allocated -= 1;
            }
            let idx = (b << 1) as usize;
            self.counts.set(idx, false);
            self.counts.set(idx + 1, false);
        }

        Ok(old)
    }

    fn inc(&mut self, begin: u64, len: u64) -> Result<()> {
        for b in begin..(begin + len) {
            let idx = (b << 1) as usize;

            // already hit the upper bound
            if self.counts.contains(idx + 1) {
                continue;
            }

            if self.counts.contains(idx) {
                self.counts.set(idx, false);
                self.counts.insert(idx + 1);
            } else {
                self.nr_allocated += 1;
                self.counts.insert(idx);
            }
        }
        Ok(())
    }

    fn alloc(&mut self) -> Result<Option<u64>> {
        let mut b = self.find_free(self.alloc_begin as u64, self.counts.len() as u64)?;
        if b.is_none() {
            b = self.find_free(0, self.alloc_begin as u64)?;
            if b.is_none() {
                return Ok(None);
            }
        }

        self.counts.insert((b.unwrap() << 1) as usize);
        self.nr_allocated += 1;
        self.alloc_begin = b.unwrap() as usize + 1;

        Ok(b)
    }

    fn find_free(&mut self, begin: u64, end: u64) -> Result<Option<u64>> {
        for b in begin..end {
            let idx = (b << 1) as usize;
            if !self.counts.contains(idx) && !self.counts.contains(idx + 1) {
                return Ok(Some(b));
            }
        }
        Ok(None)
    }

    fn get_alloc_begin(&self) -> Result<u64> {
        Ok(self.alloc_begin as u64)
    }
}

//------------------------------------------

// This null space map does nothing. Could be used
// when we want to visit shared nodes in btrees.
pub struct NoopSpaceMap {
    nr_blocks: u64,
}

impl NoopSpaceMap {
    pub fn new(nr_blocks: u64) -> NoopSpaceMap {
        NoopSpaceMap { nr_blocks }
    }
}

impl SpaceMap for NoopSpaceMap {
    fn get_nr_blocks(&self) -> Result<u64> {
        Ok(self.nr_blocks)
    }

    fn get_nr_allocated(&self) -> Result<u64> {
        Ok(0)
    }

    fn get(&self, _b: u64) -> Result<u32> {
        Ok(0)
    }

    fn set(&mut self, _b: u64, _v: u32) -> Result<u32> {
        Ok(0)
    }

    fn inc(&mut self, _begin: u64, _len: u64) -> Result<()> {
        Ok(())
    }

    fn alloc(&mut self) -> Result<Option<u64>> {
        Ok(Some(0))
    }

    fn find_free(&mut self, _begin: u64, _end: u64) -> Result<Option<u64>> {
        Ok(Some(0))
    }

    fn get_alloc_begin(&self) -> Result<u64> {
        Ok(0)
    }
}

//------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;

    fn test_get_nr_blocks(sm: &dyn SpaceMap, expected: u64) {
        assert_eq!(sm.get_nr_blocks().unwrap(), expected);
    }

    fn test_get_nr_allocated(sm: &mut dyn SpaceMap) {
        assert_eq!(sm.get_nr_allocated().unwrap(), 0);

        let nr_blocks = sm.get_nr_blocks().unwrap();
        for i in 0..nr_blocks {
            assert!(matches!(sm.alloc(), Ok(Some(_))));
            assert_eq!(sm.get_nr_allocated().unwrap(), i + 1);
        }

        for i in (0..nr_blocks).rev() {
            assert_eq!(sm.set(i, 0).unwrap(), 1);
            assert_eq!(sm.get_nr_allocated().unwrap(), i);
        }
    }

    fn test_runs_out_of_space(sm: &mut dyn SpaceMap) {
        let nr_blocks = sm.get_nr_blocks().unwrap();
        for _i in 0..nr_blocks {
            assert!(matches!(sm.alloc(), Ok(Some(_))));
        }
        assert!(matches!(sm.alloc(), Ok(None)));
    }

    fn test_inc_and_dec(sm: &mut dyn SpaceMap) {
        let nr_blocks = sm.get_nr_blocks().unwrap();
        let b = rand::thread_rng().gen_range(0..nr_blocks);

        for i in 0..255 {
            assert_eq!(sm.get(b).unwrap(), i);
            assert!(sm.inc(b, 1).is_ok());
        }

        for i in (2..=255).rev() {
            assert_eq!(sm.get(b).unwrap(), i);
            assert_eq!(sm.dec(b).unwrap(), false);
        }

        assert_eq!(sm.get(b).unwrap(), 1);
        assert_eq!(sm.dec(b).unwrap(), true);
        assert_eq!(sm.get(b).unwrap(), 0);
    }

    fn test_not_allocated_twice(sm: &mut dyn SpaceMap) {
        let mb = sm.alloc().unwrap();
        assert!(mb.is_some());
        let first = mb.unwrap();

        loop {
            match sm.alloc() {
                Ok(Some(b)) => {
                    assert_ne!(b, first);
                }
                Ok(None) => break,
                Err(e) => panic!("{}", e),
            }
        }
    }

    fn test_set_affects_nr_allocated(sm: &mut dyn SpaceMap) {
        let nr_blocks = sm.get_nr_blocks().unwrap();

        for i in 0..nr_blocks {
            assert_eq!(sm.set(i, 1).unwrap(), 0);
            assert_eq!(sm.get_nr_allocated().unwrap(), i + 1);
        }

        for i in 0..nr_blocks {
            assert_eq!(sm.set(i, 0).unwrap(), 1);
            assert_eq!(sm.get_nr_allocated().unwrap(), nr_blocks - i - 1);
        }
    }

    // 1. Verify the space map allocates blocks in round-robin fashion
    // 2. Verify that freeing a block doesn't change the allocation address
    fn test_wraparound_allocation(sm: &mut dyn SpaceMap) {
        let nr_blocks = sm.get_nr_blocks().unwrap();
        const BATCH_SIZE: u64 = 1000;
        assert!(nr_blocks > BATCH_SIZE);

        let loop_count = crate::math::div_up(nr_blocks, BATCH_SIZE);
        let mut batch_begin = 0;

        for _ in 0..loop_count {
            for i in batch_begin..(batch_begin + BATCH_SIZE) {
                let expected = i % nr_blocks;
                let actual = sm.alloc().unwrap();
                assert_eq!(expected, actual.unwrap());
            }

            for i in batch_begin..(batch_begin + BATCH_SIZE) {
                assert_eq!(sm.set(i % nr_blocks, 0).unwrap(), 1);
            }

            batch_begin += BATCH_SIZE;
        }
    }

    mod core_sm_u8 {
        use super::*;
        const NR_BLOCKS: u64 = 65536;

        #[test]
        fn get_nr_blocks() {
            let sm = CoreSpaceMap::<u8>::new(NR_BLOCKS);
            tests::test_get_nr_blocks(&sm, NR_BLOCKS);
        }

        #[test]
        fn get_nr_allocated() {
            let mut sm = CoreSpaceMap::<u8>::new(NR_BLOCKS);
            tests::test_get_nr_allocated(&mut sm);
        }

        #[test]
        fn runs_out_of_space() {
            let mut sm = CoreSpaceMap::<u8>::new(NR_BLOCKS);
            tests::test_runs_out_of_space(&mut sm);
        }

        #[test]
        fn inc_and_dec() {
            let mut sm = CoreSpaceMap::<u8>::new(NR_BLOCKS);
            tests::test_inc_and_dec(&mut sm);
        }

        #[test]
        fn not_allocated_twice() {
            let mut sm = CoreSpaceMap::<u8>::new(NR_BLOCKS);
            tests::test_not_allocated_twice(&mut sm);
        }

        #[test]
        fn set_affects_nr_allocated() {
            let mut sm = CoreSpaceMap::<u8>::new(NR_BLOCKS);
            tests::test_set_affects_nr_allocated(&mut sm);
        }

        #[test]
        fn wraparound_allocation() {
            let mut sm = CoreSpaceMap::<u8>::new(NR_BLOCKS);
            tests::test_wraparound_allocation(&mut sm);
        }
    }
}

//------------------------------------------
