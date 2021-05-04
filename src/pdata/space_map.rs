use anyhow::Result;
use fixedbitset::FixedBitSet;
use std::boxed::Box;
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

    /// Finds a block with a zero reference count.  Increments the
    /// count.
    fn alloc(&mut self) -> Result<Option<u64>>;
}

pub type ASpaceMap = Arc<Mutex<dyn SpaceMap + Sync + Send>>;

//------------------------------------------

pub struct CoreSpaceMap<T> {
    nr_allocated: u64,
    first_free: u64,
    counts: Vec<T>,
}

impl<V> CoreSpaceMap<V>
where
    V: Copy + Default + std::ops::AddAssign + From<u8>,
{
    pub fn new(nr_entries: u64) -> CoreSpaceMap<V> {
        CoreSpaceMap {
            nr_allocated: 0,
            first_free: 0,
            counts: vec![V::default(); nr_entries as usize],
        }
    }
}

impl<V> SpaceMap for CoreSpaceMap<V>
where
    V: Copy + Default + Eq + std::ops::AddAssign + From<u8> + Into<u32>,
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
        assert!(v < 0xff); // FIXME: we can't assume this
        self.counts[b as usize] = V::from(v as u8);

        if old == V::from(0u8) && v != 0 {
            self.nr_allocated += 1;
        } else if old != V::from(0u8) && v == 0 {
            self.nr_allocated -= 1;
            if b < self.first_free {
                self.first_free = b;
            }
        }

        Ok(old.into())
    }

    fn inc(&mut self, begin: u64, len: u64) -> Result<()> {
        for b in begin..(begin + len) {
            if self.counts[b as usize] == V::from(0u8) {
                // FIXME: can we get a ref to save dereferencing counts twice?
                self.nr_allocated += 1;
                self.counts[b as usize] = V::from(1u8);
            } else {
                self.counts[b as usize] += V::from(1u8);
            }
        }
        Ok(())
    }

    fn alloc(&mut self) -> Result<Option<u64>> {
        for b in self.first_free..(self.counts.len() as u64) {
            if self.counts[b as usize] == V::from(0u8) {
                self.counts[b as usize] = V::from(1u8);
                self.first_free = b + 1;
                return Ok(Some(b));
            }
        }

        self.first_free = self.counts.len() as u64;
        Ok(None)
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
    first_free: usize,
    counts: FixedBitSet,
}

impl RestrictedSpaceMap {
    pub fn new(nr_entries: u64) -> RestrictedSpaceMap {
        RestrictedSpaceMap {
            nr_allocated: 0,
            counts: FixedBitSet::with_capacity(nr_entries as usize),
            first_free: 0,
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
                if b < self.first_free as u64 {
                    self.first_free = b as usize;
                }
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
        for b in self.first_free..self.counts.len() {
            if !self.counts.contains(b) {
                self.counts.insert(b);
                self.first_free = b + 1;
                return Ok(Some(b as u64));
            }
        }

        self.first_free = self.counts.len();
        Ok(None)
    }
}

//------------------------------------------
