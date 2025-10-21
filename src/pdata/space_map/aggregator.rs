use anyhow::Result;
use fixedbitset::*;
use std::sync::Mutex;

use crate::pdata::space_map::base::*;

//--------------------------------

const REGION_SIZE: usize = 1024;

enum Rep {
    NoCounts,
    Bits(FixedBitSet),
    U8s(Vec<u8>),
    U16s(Vec<u16>),
    U32s(Vec<u32>),
}

use Rep::*;

impl Rep {
    fn upgrade(&self) -> Self {
        match self {
            NoCounts => Bits(FixedBitSet::with_capacity(REGION_SIZE)),
            Bits(bits) => {
                let mut bytes = vec![0; REGION_SIZE];
                for b in bits.ones() {
                    bytes[b] = 1;
                }
                U8s(bytes)
            }
            U8s(counts) => U16s(counts.iter().map(|n| *n as u16).collect()),
            U16s(counts) => U32s(counts.iter().map(|n| *n as u32).collect()),
            U32s(_) => {
                panic!("huge reference count");
            }
        }
    }
}

struct Region {
    rep: Rep,
    nr_allocated: u32,
}

impl Default for Region {
    fn default() -> Self {
        Self {
            rep: NoCounts,
            nr_allocated: 0,
        }
    }
}

enum IncResult {
    Success,

    // Contains the index of the first unprocessed element of blocks
    NeedUpgrade(usize),
}

impl Region {
    fn increment_(&mut self, blocks: &[u64]) -> IncResult {
        assert!(!blocks.is_empty());

        match &mut self.rep {
            NoCounts => {
                return IncResult::NeedUpgrade(0);
            }
            Bits(bits) => {
                for (i, &block) in blocks.iter().enumerate() {
                    let b = block % REGION_SIZE as u64;
                    if bits.contains(b as usize) {
                        return IncResult::NeedUpgrade(i);
                    }
                    bits.insert(b as usize);
                    self.nr_allocated += 1;
                }
            }
            U8s(counts) => {
                for (i, &block) in blocks.iter().enumerate() {
                    let b = block % REGION_SIZE as u64;
                    if counts[b as usize] == u8::MAX {
                        return IncResult::NeedUpgrade(i);
                    }
                    if counts[b as usize] == 0 {
                        self.nr_allocated += 1;
                    }
                    counts[b as usize] += 1;
                }
            }
            U16s(counts) => {
                for (i, &block) in blocks.iter().enumerate() {
                    let b = block % REGION_SIZE as u64;
                    if counts[b as usize] == u16::MAX {
                        return IncResult::NeedUpgrade(i);
                    }
                    if counts[b as usize] == 0 {
                        self.nr_allocated += 1;
                    }
                    counts[b as usize] += 1;
                }
            }
            U32s(counts) => {
                for b in blocks {
                    let b = b % REGION_SIZE as u64;
                    if counts[b as usize] == 0 {
                        self.nr_allocated += 1;
                    }
                    counts[b as usize] += 1;
                }
            }
        }

        IncResult::Success
    }

    fn increment(&mut self, blocks: &[u64]) {
        use IncResult::*;

        let mut idx = 0;
        loop {
            match self.increment_(&blocks[idx..]) {
                Success => break,
                NeedUpgrade(i) => {
                    self.rep = self.rep.upgrade();
                    idx += i;
                }
            }
        }
    }

    fn test_and_inc_(
        &mut self,
        blocks: &[u64],
        results: &mut FixedBitSet,
        offset: usize,
    ) -> IncResult {
        match &mut self.rep {
            NoCounts => IncResult::NeedUpgrade(0),
            Bits(bits) => {
                for (i, &block) in blocks.iter().enumerate() {
                    let b = block as usize % REGION_SIZE;
                    results.set(offset + i, bits.contains(b));
                    if bits.contains(b) {
                        return IncResult::NeedUpgrade(i);
                    }
                    bits.insert(b);
                    self.nr_allocated += 1;
                }
                IncResult::Success
            }
            U8s(counts) => {
                for (i, &block) in blocks.iter().enumerate() {
                    let b = block as usize % REGION_SIZE;
                    results.set(offset + i, counts[b] > 0);
                    if counts[b] == u8::MAX {
                        return IncResult::NeedUpgrade(i);
                    }
                    if counts[b] == 0 {
                        self.nr_allocated += 1;
                    }
                    counts[b] += 1;
                }
                IncResult::Success
            }
            U16s(counts) => {
                for (i, &block) in blocks.iter().enumerate() {
                    let b = block as usize % REGION_SIZE;
                    results.set(offset + i, counts[b] > 0);
                    if counts[b] == u16::MAX {
                        return IncResult::NeedUpgrade(i);
                    }
                    if counts[b] == 0 {
                        self.nr_allocated += 1;
                    }
                    counts[b] += 1;
                }
                IncResult::Success
            }
            U32s(counts) => {
                for (i, &block) in blocks.iter().enumerate() {
                    let b = block as usize % REGION_SIZE;
                    results.set(offset + i, counts[b] > 0);
                    if counts[b] == 0 {
                        self.nr_allocated += 1;
                    }
                    counts[b] = counts[b].saturating_add(1);
                }
                IncResult::Success
            }
        }
    }

    pub fn test_and_inc(&mut self, blocks: &[u64], results: &mut FixedBitSet, offset: usize) {
        let mut processed = 0;

        while processed < blocks.len() {
            match self.test_and_inc_(&blocks[processed..], results, offset + processed) {
                IncResult::Success => break,
                IncResult::NeedUpgrade(i) => {
                    self.rep = self.rep.upgrade();
                    processed += i;
                }
            }
        }
    }

    /// Retrieves the reference counts for a range of blocks.
    ///
    /// This method allows you to look up the reference counts for a contiguous range of blocks,
    /// starting from the specified `begin` block. It fills the provided `results` slice with
    /// the reference counts and returns the number of blocks actually processed.
    ///
    /// # Arguments
    ///
    /// * `begin` - The starting block number to look up.
    /// * `results` - A mutable slice to store the retrieved reference counts. The length of this
    ///   slice determines the maximum number of blocks to look up.
    ///
    /// # Returns
    ///
    /// Returns the number of blocks actually processed. This may be less than `results.len()` if
    /// the range extends beyond the end of the Aggregator's entries.
    ///
    /// # Examples
    ///
    /// ```
    /// use thinp::pdata::space_map::aggregator::Aggregator;
    ///
    /// let aggregator = Aggregator::new(1000);
    /// // ... (assume some increments have been performed)
    ///
    /// let mut results = vec![0; 10];
    /// let blocks_read = aggregator.lookup(500, &mut results);
    /// println!("Read {} blocks, first few counts: {:?}", blocks_read, &results[..3]);
    /// ```
    ///
    /// # Notes
    ///
    /// - If `begin` is beyond the last entry in the Aggregator, this method will return 0 and
    ///   leave `results` unchanged.
    /// - The method will read up to `results.len()` blocks, but may read fewer if the end of
    ///   the Aggregator's range is reached.
    /// - This method is thread-safe and can be called concurrently with `increment` operations.
    fn lookup(&self, block: u64, results: &mut [u32]) -> u64 {
        let b = block % REGION_SIZE as u64;
        let e = (b as usize + results.len()).min(REGION_SIZE);

        match &self.rep {
            NoCounts => {
                // All zeroes
                for i in (b as usize)..e {
                    results[i - b as usize] = 0;
                }
            }
            Bits(bits) => {
                for i in (b as usize)..e {
                    results[i - b as usize] = if bits.contains(i) { 1 } else { 0 };
                }
            }
            U8s(counts) => {
                for i in (b as usize)..e {
                    results[i - b as usize] = counts[i] as u32;
                }
            }
            U16s(counts) => {
                for i in (b as usize)..e {
                    results[i - b as usize] = counts[i] as u32;
                }
            }
            U32s(counts) => {
                for i in (b as usize)..e {
                    results[i - b as usize] = counts[i];
                }
            }
        }
        e as u64 - b
    }

    fn set_(&mut self, pairs: &[(u64, u32)]) -> IncResult {
        for (i, &(block, ref_count)) in pairs.iter().enumerate() {
            let b = block % REGION_SIZE as u64;
            match &mut self.rep {
                NoCounts => {
                    if ref_count > 0 {
                        return IncResult::NeedUpgrade(i);
                    }
                }
                Bits(bits) => {
                    if ref_count > 1 {
                        return IncResult::NeedUpgrade(i);
                    }
                    let rc_before = if bits.contains(b as usize) { 1 } else { 0 };
                    bits.set(b as usize, ref_count == 1);

                    if rc_before == 0 && ref_count != 0 {
                        self.nr_allocated += 1;
                    } else if rc_before != 0 && ref_count == 0 {
                        self.nr_allocated -= 1;
                    }
                }
                U8s(counts) => {
                    if ref_count > u8::MAX as u32 {
                        return IncResult::NeedUpgrade(i);
                    }
                    let rc_before = counts[b as usize] as u32;
                    counts[b as usize] = ref_count as u8;

                    if rc_before == 0 && ref_count != 0 {
                        self.nr_allocated += 1;
                    } else if rc_before != 0 && ref_count == 0 {
                        self.nr_allocated -= 1;
                    }
                }
                U16s(counts) => {
                    if ref_count > u16::MAX as u32 {
                        return IncResult::NeedUpgrade(i);
                    }
                    let rc_before = counts[b as usize] as u32;
                    counts[b as usize] = ref_count as u16;

                    if rc_before == 0 && ref_count != 0 {
                        self.nr_allocated += 1;
                    } else if rc_before != 0 && ref_count == 0 {
                        self.nr_allocated -= 1;
                    }
                }
                U32s(counts) => {
                    let rc_before = counts[b as usize];
                    counts[b as usize] = ref_count;

                    if rc_before == 0 && ref_count != 0 {
                        self.nr_allocated += 1;
                    } else if rc_before != 0 && ref_count == 0 {
                        self.nr_allocated -= 1;
                    }
                }
            }
        }
        IncResult::Success
    }

    /// Name clash with RefCount.set()
    pub fn set(&mut self, pairs: &[(u64, u32)]) {
        let mut processed = 0;
        while processed < pairs.len() {
            match self.set_(&pairs[processed..]) {
                IncResult::Success => break,
                IncResult::NeedUpgrade(i) => {
                    self.rep = self.rep.upgrade();
                    processed += i;
                }
            }
        }
    }

    fn rep_size(&self) -> usize {
        match &self.rep {
            NoCounts => 0,
            Bits(_) => REGION_SIZE / 8,
            U8s(_) => REGION_SIZE,
            U16s(_) => REGION_SIZE * 2,
            U32s(_) => REGION_SIZE * 4,
        }
    }
}

/// Aggregates reference count increments across multiple regions.
///
/// The `Aggregator` is designed to efficiently collect batches of reference count
/// increments. It is thread-safe and allows multiple threads to update counts
/// concurrently by locking at the region level.
///
/// # Examples
///
/// ```rust
/// use thinp::pdata::space_map::aggregator::Aggregator;
///
/// // Initialize an aggregator with 1024 entries
/// let aggregator = Aggregator::new(1024);
///
/// // Increment reference counts for a sorted list of blocks
/// let blocks = vec![1, 2, 3, 4, 5];
/// aggregator.increment(&blocks);
/// ```
///
/// # Performance Tips
///
/// - **Sorted Blocks:** To minimize locking overhead, it is recommended to pass a
///   sorted list of blocks to the `increment` method. This ensures that blocks
///   belonging to the same region are grouped together, reducing the number of
///   mutex locks required.
///
/// - **Batch Processing:** Aggregating increments in larger batches can improve
///   cache locality and reduce synchronization costs.
pub struct Aggregator {
    nr_entries: usize,
    regions: Vec<Mutex<Region>>,
    nr_allocated: Mutex<u64>,
}

impl Aggregator {
    /// Creates a new `Aggregator` with the specified number of entries.
    ///
    /// The number of regions is determined based on the `REGION_SIZE`. Each
    /// region is protected by its own `Mutex` to allow concurrent updates.
    ///
    /// # Arguments
    ///
    /// * `nr_entries` - The total number of entries to manage. This determines
    ///   the number of regions by dividing `nr_entries` by
    ///   `REGION_SIZE`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use thinp::pdata::space_map::aggregator::Aggregator;
    ///
    /// let aggregator = Aggregator::new(1024);
    /// ```
    pub fn new(nr_entries: usize) -> Self {
        let nr_regions = nr_entries.div_ceil(REGION_SIZE);
        let regions = (0..nr_regions)
            .map(|_| Mutex::new(Region::default()))
            .collect();
        Self {
            nr_entries,
            regions,
            nr_allocated: Mutex::new(0),
        }
    }

    /// Increments the reference counts for the specified blocks.
    ///
    /// This method processes a batch of blocks, incrementing their reference
    /// counts accordingly. To optimize performance and minimize locking overhead,
    /// it is recommended to pass a **sorted** list of blocks. Sorting ensures that
    /// blocks belonging to the same region are processed together, reducing the
    /// number of mutex locks required.
    ///
    /// # Arguments
    ///
    /// * `blocks` - A slice of block identifiers to increment. The blocks should
    ///   be sorted in ascending order to maximize performance.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use thinp::pdata::space_map::aggregator::Aggregator;
    ///
    /// let aggregator = Aggregator::new(1024);
    /// let mut blocks = vec![5, 2, 3, 1, 4];
    /// blocks.sort(); // Ensure the blocks are sorted
    /// aggregator.increment(&blocks);
    /// ```
    ///
    /// # Performance Tips
    ///
    /// - **Sorted Input:** Always sort the `blocks` before passing them to this
    ///   method. Unsigned sorted blocks can lead to increased locking as multiple
    ///   regions may be accessed more frequently.
    ///
    /// - **Batch Size:** Larger batches can improve throughput by reducing the
    ///   frequency of mutex acquisitions.
    pub fn increment(&self, blocks: &[u64]) {
        if blocks.is_empty() {
            return;
        }

        // We increment one region at a time to minimise the number of
        // times the mutex is taken.
        let mut start_idx = 0;
        let mut start_region = blocks[start_idx] / REGION_SIZE as u64;
        let mut nr_allocated = 0u64;

        for i in 1..blocks.len() {
            let current_region = blocks[i] / REGION_SIZE as u64;
            if current_region != start_region {
                nr_allocated += self.process_region_increment(start_region, &blocks[start_idx..i]);
                start_idx = i;
                start_region = current_region;
            }
        }

        nr_allocated += self.process_region_increment(start_region, &blocks[start_idx..]);
        self.inc_allocated(nr_allocated);
    }

    pub fn inc_single(&self, block: u64) {
        self.increment(&[block]);
    }

    pub fn get_nr_blocks(&self) -> usize {
        self.nr_entries
    }

    pub fn lookup(&self, begin: u64, results: &mut [u32]) -> u64 {
        if begin >= self.nr_entries as u64 {
            return 0;
        }

        // Adjust the results slice to take into account the nr_entries
        let len = results.len();
        let results = &mut results[0..len.min(self.nr_entries - begin as usize)];

        let mut b = begin;
        let end = (begin as usize + results.len()).min(self.nr_entries);
        let mut nr_read = 0;
        while b < end as u64 {
            let region_idx = b as usize / REGION_SIZE;
            let region = self.regions[region_idx].lock().unwrap();
            let len = region.lookup(b, &mut results[nr_read as usize..]);
            b += len;
            nr_read += len;
        }

        nr_read
    }

    pub fn test_and_inc(&self, blocks: &[u64]) -> FixedBitSet {
        let mut results = FixedBitSet::with_capacity(blocks.len());

        if blocks.is_empty() {
            return results;
        }

        let mut start_idx = 0;
        let mut start_region = blocks[start_idx] / REGION_SIZE as u64;
        let mut nr_allocated = 0u64;

        for i in 1..blocks.len() {
            let current_region = blocks[i] / REGION_SIZE as u64;
            if current_region != start_region {
                nr_allocated += self.process_region_test_and_inc(
                    start_region,
                    &blocks[start_idx..i],
                    &mut results,
                    start_idx,
                );
                start_idx = i;
                start_region = current_region;
            }
        }

        nr_allocated += self.process_region_test_and_inc(
            start_region,
            &blocks[start_idx..],
            &mut results,
            start_idx,
        );
        self.inc_allocated(nr_allocated);

        results
    }

    pub fn set_batch(&self, pairs: &[(u64, u32)]) {
        if pairs.is_empty() {
            return;
        }

        let mut start_idx = 0;
        let mut start_region = pairs[0].0 / REGION_SIZE as u64;

        // Use two unsigned counters to prevent integer overflow with a signed counter
        let mut nr_allocated = 0u64;
        let mut nr_freed = 0u64;

        for i in 1..pairs.len() {
            let current_region = pairs[i].0 / REGION_SIZE as u64;
            if current_region != start_region {
                let (allocated, freed) =
                    self.process_region_set(start_region, &pairs[start_idx..i]);
                nr_allocated += allocated;
                nr_freed += freed;

                start_idx = i;
                start_region = current_region;
            }
        }

        let (allocated, freed) = self.process_region_set(start_region, &pairs[start_idx..]);
        nr_allocated += allocated;
        nr_freed += freed;

        // Apply net allocation change with single lock
        // If nr_allocated == nr_freed, do nothing (optimal)
        if nr_allocated > nr_freed {
            self.inc_allocated(nr_allocated - nr_freed);
        } else if nr_allocated < nr_freed {
            self.dec_allocated(nr_freed - nr_allocated);
        }
    }

    /// Compares the reference counts with another aggregator and calls the provided
    /// callback for any block with differing counts.
    ///
    /// The callback is invoked with (global block number, self count, other count).
    ///
    /// Panics if the two aggregators do not manage the same number of entries.
    pub fn diff<F>(&self, other: &Aggregator, mut f: F)
    where
        F: FnMut(u64, u32, u32),
    {
        assert_eq!(self.nr_entries, other.nr_entries, "Mismatched nr_entries");

        let total_entries = self.nr_entries;
        let nr_regions = self.regions.len().min(other.regions.len());

        // FIXME: we could be more efficient here and just gather counts from one side
        // and have a region method to do the comparison.
        let mut self_counts = vec![0u32; REGION_SIZE];
        let mut other_counts = vec![0u32; REGION_SIZE];

        for region_idx in 0..nr_regions {
            let region_base = region_idx * REGION_SIZE;
            let region_limit = ((region_idx + 1) * REGION_SIZE).min(total_entries);
            let len = region_limit - region_base;

            let self_region = self.regions[region_idx].lock().unwrap();
            let other_region = other.regions[region_idx].lock().unwrap();

            // Read counts from both regions.
            let _ = self_region.lookup(region_base as u64, &mut self_counts[0..len]);
            let _ = other_region.lookup(region_base as u64, &mut other_counts[0..len]);

            for i in 0..len {
                if self_counts[i] != other_counts[i] {
                    f((region_base + i) as u64, self_counts[i], other_counts[i]);
                }
            }
        }
    }

    pub fn rep_size(&self) -> usize {
        let mut total = 0;
        for i in 0..self.regions.len() {
            let region = self.regions[i].lock().unwrap();
            total += std::mem::size_of::<Mutex<Region>>();
            total += region.rep_size();
        }
        total
    }

    pub fn get_nr_regions(&self) -> usize {
        self.regions.len()
    }

    fn process_region_increment(&self, region_idx: u64, blocks: &[u64]) -> u64 {
        if region_idx >= self.regions.len() as u64 {
            return 0;
        }

        let mut region = self.regions[region_idx as usize].lock().unwrap();
        let allocated_before = region.nr_allocated;
        region.increment(blocks);
        (region.nr_allocated - allocated_before) as u64
    }

    fn process_region_test_and_inc(
        &self,
        region_idx: u64,
        blocks: &[u64],
        results: &mut FixedBitSet,
        results_offset: usize,
    ) -> u64 {
        if region_idx >= self.regions.len() as u64 {
            return 0;
        }

        let mut region = self.regions[region_idx as usize].lock().unwrap();
        let allocated_before = region.nr_allocated;
        region.test_and_inc(blocks, results, results_offset);
        (region.nr_allocated - allocated_before) as u64
    }

    fn process_region_set(&self, region_idx: u64, pairs: &[(u64, u32)]) -> (u64, u64) {
        if region_idx >= self.regions.len() as u64 {
            return (0, 0);
        }

        let mut region = self.regions[region_idx as usize].lock().unwrap();
        let allocated_before = region.nr_allocated;
        region.set(pairs);

        if region.nr_allocated >= allocated_before {
            ((region.nr_allocated - allocated_before) as u64, 0)
        } else {
            (0, (allocated_before - region.nr_allocated) as u64)
        }
    }

    fn inc_allocated(&self, count: u64) {
        let mut nr_allocated = self.nr_allocated.lock().unwrap();
        *nr_allocated = nr_allocated.saturating_add(count);
    }

    fn dec_allocated(&self, count: u64) {
        let mut nr_allocated = self.nr_allocated.lock().unwrap();
        *nr_allocated = nr_allocated.saturating_sub(count);
    }
}

impl RefCount for Aggregator {
    fn get_nr_blocks(&self) -> Result<u64> {
        Ok(self.get_nr_blocks() as u64)
    }

    fn get(&self, b: u64) -> Result<u32> {
        // So slow
        let mut r = [0];
        self.lookup(b, &mut r[0..]);
        Ok(r[0])
    }

    /// Returns the old ref count
    fn set(&mut self, b: u64, v: u32) -> Result<u32> {
        let r = self.get(b)?;
        self.set_batch(&[(b, v)]);
        Ok(r)
    }

    fn inc(&mut self, begin: u64, len: u64) -> Result<()> {
        for b in begin..(begin + len) {
            self.inc_single(b);
        }
        Ok(())
    }
}

// FIXME: only doing this as a stop gap solution
impl SpaceMap for Aggregator {
    fn get_nr_allocated(&self) -> Result<u64> {
        let nr_allocated = self.nr_allocated.lock().unwrap();
        Ok(*nr_allocated)
    }

    /// Finds a block with a zero reference count. Increments the count.
    /// Returns Ok(None) if no free block (ENOSPC)
    /// Returns Err on fatal error
    fn alloc(&mut self) -> Result<Option<u64>> {
        todo!();
    }

    /// Finds a free block within the range
    fn find_free(&mut self, _begin: u64, _end: u64) -> Result<Option<u64>> {
        todo!();
    }

    /// Returns the position where allocation starts
    fn get_alloc_begin(&self) -> Result<u64> {
        todo!();
    }
}

//--------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    fn from_rep(rep: Rep) -> Region {
        let nr_allocated = match &rep {
            NoCounts => 0,
            Bits(bits) => bits.count_ones(..),
            U8s(counts) => counts.iter().filter(|&x| *x != 0).count(),
            U16s(counts) => counts.iter().filter(|&x| *x != 0).count(),
            U32s(counts) => counts.iter().filter(|&x| *x != 0).count(),
        } as u32;

        Region { rep, nr_allocated }
    }

    #[test]
    fn test_initial_no_counts() {
        let mut region = Region::default();
        let blocks = [1, 2, 3];
        let result = region.increment_(&blocks);
        assert!(matches!(result, IncResult::NeedUpgrade(0)));
    }

    #[test]
    fn test_u8_increment() {
        let mut region = from_rep(U8s(vec![0; REGION_SIZE]));
        let blocks = [10, 20, 10];
        let result = region.increment_(&blocks);
        assert!(matches!(result, IncResult::Success));
        if let U8s(counts) = &region.rep {
            assert_eq!(counts[10], 2);
            assert_eq!(counts[20], 1);
        } else {
            panic!("Expected U8s representation");
        }
    }

    #[test]
    fn test_u8_overflow_upgrade() {
        let counts = vec![u8::MAX; REGION_SIZE];
        let mut region = from_rep(U8s(counts));
        let blocks = [5];
        let result = region.increment_(&blocks);
        assert!(matches!(result, IncResult::NeedUpgrade(0)));
    }

    #[test]
    fn test_full_upgrade_path() {
        let mut region = Region::default();
        // Initial increment should request upgrade
        let result = region.increment_(&[1]);
        assert!(matches!(result, IncResult::NeedUpgrade(0)));
        // Perform the upgrade to Bits
        region.rep = region.rep.upgrade();
        // Simulate handling Bits
        if let Bits(bits) = &mut region.rep {
            bits.insert(1);
        }
        // Next increment should request upgrade from Bits to U8s
        let result = region.increment_(&[1]);
        assert!(matches!(result, IncResult::NeedUpgrade(0)));
    }

    #[test]
    fn test_aggregator_increment_single_region() {
        let aggregator = Aggregator::new(REGION_SIZE);
        let blocks = vec![1, 2, 1];
        aggregator.increment(&blocks);

        let region = aggregator.regions[0].lock().unwrap();
        if let U8s(counts) = &region.rep {
            assert_eq!(counts[1], 2);
            assert_eq!(counts[2], 1);
        } else {
            panic!("Expected U8s representation");
        }
    }

    #[test]
    fn test_aggregator_increment_multiple_regions() {
        let nr_entries = REGION_SIZE * 2;
        let aggregator = Aggregator::new(nr_entries);
        let blocks = vec![1, REGION_SIZE as u64 + 1, 1, REGION_SIZE as u64 + 2];
        aggregator.increment(&blocks);

        // Check first region
        let region0 = aggregator.regions[0].lock().unwrap();
        if let U8s(counts) = &region0.rep {
            assert_eq!(counts[1], 2);
            assert_eq!(counts[0], 0);
        } else {
            panic!("Expected U8s representation");
        }

        // Check second region
        let region1 = aggregator.regions[1].lock().unwrap();
        if let Bits(bits) = &region1.rep {
            assert!(bits.contains(1));
            assert!(bits.contains(2));
        } else {
            panic!("Expected U8s representation");
        }
    }

    #[test]
    fn test_concurrent_increments() {
        let nr_entries = REGION_SIZE * 10;
        let aggregator = Arc::new(Aggregator::new(nr_entries));
        let threads: Vec<_> = (0..10)
            .map(|i| {
                let agg = Arc::clone(&aggregator);
                thread::spawn(move || {
                    let blocks = vec![REGION_SIZE as u64 * i + 1; 100];
                    agg.increment(&blocks);
                })
            })
            .collect();

        for t in threads {
            t.join().unwrap();
        }

        for i in 0..10 {
            let region = aggregator.regions[i].lock().unwrap();
            if let U8s(counts) = &region.rep {
                assert_eq!(counts[1], 100);
            } else {
                panic!("Expected U8s representation");
            }
        }
    }

    #[test]
    fn test_region_lookup_no_counts() {
        let region = Region::default();
        let mut results = vec![0; 10];
        let blocks_read = region.lookup(5, &mut results);
        assert_eq!(blocks_read, 10);
        assert_eq!(results, vec![0; 10]);
    }

    #[test]
    fn test_region_lookup_bits() {
        let mut region = from_rep(Bits(FixedBitSet::with_capacity(REGION_SIZE)));
        if let Bits(bits) = &mut region.rep {
            bits.insert(7);
            bits.insert(9);
        }
        let mut results = vec![0; 5];
        let blocks_read = region.lookup(6, &mut results);
        assert_eq!(blocks_read, 5);
        assert_eq!(results, vec![0, 1, 0, 1, 0]);
    }

    #[test]
    fn test_region_lookup_u8s() {
        let mut counts = vec![0; REGION_SIZE];
        counts[3] = 2;
        counts[4] = 1;
        let region = from_rep(U8s(counts));
        let mut results = vec![0; 5];
        let blocks_read = region.lookup(2, &mut results);
        assert_eq!(blocks_read, 5);
        assert_eq!(results, vec![0, 2, 1, 0, 0]);
    }

    #[test]
    fn test_region_lookup_u16s() {
        let mut counts = vec![0; REGION_SIZE];
        counts[1] = 300;
        counts[2] = 500;
        let region = from_rep(U16s(counts));
        let mut results = vec![0; 3];
        let blocks_read = region.lookup(1, &mut results);
        assert_eq!(blocks_read, 3);
        assert_eq!(results, vec![300, 500, 0]);
    }

    #[test]
    fn test_region_lookup_u32s() {
        let mut counts = vec![0; REGION_SIZE];
        counts[0] = 1_000_000;
        counts[1] = 2_000_000;
        let region = from_rep(U32s(counts));
        let mut results = vec![0; 3];
        let blocks_read = region.lookup(0, &mut results);
        assert_eq!(blocks_read, 3);
        assert_eq!(results, vec![1_000_000, 2_000_000, 0]);
    }

    #[test]
    fn test_aggregator_lookup_single_region() {
        let aggregator = Aggregator::new(REGION_SIZE);
        aggregator.increment(&[1, 2, 1, 3]);
        let mut results = vec![0; 5];
        let blocks_read = aggregator.lookup(0, &mut results);
        assert_eq!(blocks_read, 5);
        assert_eq!(results, vec![0, 2, 1, 1, 0]);
    }

    #[test]
    fn test_aggregator_lookup_multiple_regions() {
        let aggregator = Aggregator::new(REGION_SIZE * 2);
        aggregator.increment(&[1, REGION_SIZE as u64 + 1, 1, REGION_SIZE as u64 + 2]);
        let mut results = vec![0; REGION_SIZE + 5];
        let blocks_read = aggregator.lookup(0, &mut results);
        assert_eq!(blocks_read, REGION_SIZE as u64 + 5);
        assert_eq!(results[1], 2);
        assert_eq!(results[REGION_SIZE + 1], 1);
        assert_eq!(results[REGION_SIZE + 2], 1);
    }

    #[test]
    fn test_aggregator_lookup_out_of_bounds() {
        let aggregator = Aggregator::new(REGION_SIZE);
        let mut results = vec![0; 5];
        let blocks_read = aggregator.lookup(REGION_SIZE as u64, &mut results);
        assert_eq!(blocks_read, 0);
        assert_eq!(results, vec![0; 5]);
    }

    #[test]
    fn test_aggregator_lookup_partial_read() {
        let aggregator = Aggregator::new(REGION_SIZE + 5);
        aggregator.increment(&[
            REGION_SIZE as u64,
            REGION_SIZE as u64 + 1,
            REGION_SIZE as u64 + 2,
        ]);
        let mut results = vec![0; 10];
        let blocks_read = aggregator.lookup(REGION_SIZE as u64 - 2, &mut results);
        assert_eq!(blocks_read, 7);
        assert_eq!(results[2..5], vec![1, 1, 1]);
    }

    #[test]
    fn test_region_test_and_inc_no_counts() {
        let mut region = Region::default();
        let blocks = vec![1, 2, 3];
        let mut results = FixedBitSet::with_capacity(3);
        region.test_and_inc(&blocks, &mut results, 0);

        assert_eq!(results.len(), 3);
        assert!(!results.contains(0));
        assert!(!results.contains(1));
        assert!(!results.contains(2));

        if let Bits(bits) = &region.rep {
            assert!(bits.contains(1));
            assert!(bits.contains(2));
            assert!(bits.contains(3));
        } else {
            panic!("Expected Bits representation after upgrade");
        }
    }

    #[test]
    fn test_region_test_and_inc_bits() {
        let mut region = from_rep(Bits(FixedBitSet::with_capacity(REGION_SIZE)));
        if let Bits(bits) = &mut region.rep {
            bits.insert(2);
        }
        let blocks = vec![1, 2, 3];
        let mut results = FixedBitSet::with_capacity(3);
        region.test_and_inc(&blocks, &mut results, 0);

        assert_eq!(results.len(), 3);
        assert!(!results.contains(0));
        assert!(results.contains(1));
        assert!(!results.contains(2));

        if let U8s(counts) = &region.rep {
            assert_eq!(counts[1], 1);
            assert_eq!(counts[2], 2);
            assert_eq!(counts[3], 1);
        } else {
            panic!("Expected U8s representation after upgrade from Bits");
        }
    }

    #[test]
    fn test_region_test_and_inc_u8s() {
        let mut counts = vec![0; REGION_SIZE];
        counts[1] = 1;
        counts[2] = 255;
        let mut region = from_rep(U8s(counts));
        let blocks = vec![1, 2, 3];
        let mut results = FixedBitSet::with_capacity(3);
        region.test_and_inc(&blocks, &mut results, 0);

        assert_eq!(results.len(), 3);
        assert!(results.contains(0));
        assert!(results.contains(1));
        assert!(!results.contains(2));

        if let U16s(counts) = &region.rep {
            assert_eq!(counts[1], 2);
            assert_eq!(counts[2], 256);
            assert_eq!(counts[3], 1);
        } else {
            panic!("Expected U16s representation after upgrade");
        }
    }

    #[test]
    fn test_aggregator_test_and_inc_single_region() {
        let aggregator = Aggregator::new(REGION_SIZE);
        let blocks = vec![1, 2, 1];
        let results = aggregator.test_and_inc(&blocks);

        assert_eq!(results.len(), 3);
        assert!(!results.contains(0));
        assert!(!results.contains(1));
        assert!(results.contains(2));

        let region = aggregator.regions[0].lock().unwrap();
        if let U8s(counts) = &region.rep {
            assert_eq!(counts[1], 2);
            assert_eq!(counts[2], 1);
        } else {
            panic!("Expected U8s representation");
        }
    }

    #[test]
    fn test_aggregator_test_and_inc_multiple_regions() {
        let nr_entries = REGION_SIZE * 2;
        let aggregator = Aggregator::new(nr_entries);
        let blocks = vec![1, REGION_SIZE as u64 + 1, 1, REGION_SIZE as u64 + 2];
        let results = aggregator.test_and_inc(&blocks);

        assert_eq!(results.len(), 4);
        assert!(!results.contains(0));
        assert!(!results.contains(1));
        assert!(results.contains(2));
        assert!(!results.contains(3));

        let region0 = aggregator.regions[0].lock().unwrap();
        if let U8s(counts) = &region0.rep {
            assert_eq!(counts[1], 2);
        } else {
            panic!("Expected U8s representation for region 0");
        }

        let region1 = aggregator.regions[1].lock().unwrap();
        if let Bits(bits) = &region1.rep {
            assert!(bits.contains(1));
            assert!(bits.contains(2));
        } else {
            panic!("Expected Bits representation for region 1");
        }
    }

    #[test]
    fn test_aggregator_test_and_inc_empty_blocks() {
        let aggregator = Aggregator::new(REGION_SIZE);
        let blocks = vec![];
        let results = aggregator.test_and_inc(&blocks);

        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_region_set_no_counts() {
        let mut region = Region::default();
        let pairs = vec![(1, 0), (2, 1), (3, 2)];
        region.set(&pairs);

        if let U8s(counts) = &region.rep {
            assert_eq!(counts[1], 0);
            assert_eq!(counts[2], 1);
            assert_eq!(counts[3], 2);
        } else {
            panic!("Expected U8s representation");
        }
    }

    #[test]
    fn test_region_set_bits() {
        let mut region = from_rep(Bits(FixedBitSet::with_capacity(REGION_SIZE)));
        let pairs = vec![(1, 0), (2, 1), (3, 2)];
        region.set(&pairs);

        if let U8s(counts) = &region.rep {
            assert_eq!(counts[1], 0);
            assert_eq!(counts[2], 1);
            assert_eq!(counts[3], 2);
        } else {
            panic!("Expected U8s representation after upgrade");
        }
    }

    #[test]
    fn test_region_set_u8s() {
        let mut region = from_rep(U8s(vec![0; REGION_SIZE]));
        let pairs = vec![(1, 255), (2, 256), (3, 1)];
        region.set(&pairs);

        if let U16s(counts) = &region.rep {
            assert_eq!(counts[1], 255);
            assert_eq!(counts[2], 256);
            assert_eq!(counts[3], 1);
        } else {
            panic!("Expected U16s representation after upgrade");
        }
    }

    #[test]
    fn test_region_set_u16s() {
        let mut region = from_rep(U16s(vec![0; REGION_SIZE]));
        let pairs = vec![(1, 65535), (2, 65536), (3, 1)];
        region.set(&pairs);

        if let U32s(counts) = &region.rep {
            assert_eq!(counts[1], 65535);
            assert_eq!(counts[2], 65536);
            assert_eq!(counts[3], 1);
        } else {
            panic!("Expected U32s representation after upgrade");
        }
    }

    #[test]
    fn test_region_set_u32s() {
        let mut region = from_rep(U32s(vec![0; REGION_SIZE]));
        let pairs = vec![(1, 1000000), (2, 2000000), (3, 3000000)];
        region.set(&pairs);

        if let U32s(counts) = &region.rep {
            assert_eq!(counts[1], 1000000);
            assert_eq!(counts[2], 2000000);
            assert_eq!(counts[3], 3000000);
        } else {
            panic!("Expected U32s representation");
        }
    }

    #[test]
    fn test_aggregator_set_single_region() {
        let aggregator = Aggregator::new(REGION_SIZE);
        let pairs = vec![(1, 1), (2, 2), (3, 3)];
        aggregator.set_batch(&pairs);

        let region = aggregator.regions[0].lock().unwrap();
        if let U8s(counts) = &region.rep {
            assert_eq!(counts[1], 1);
            assert_eq!(counts[2], 2);
            assert_eq!(counts[3], 3);
        } else {
            panic!("Expected U8s representation");
        }
    }

    #[test]
    fn test_aggregator_set_multiple_regions() {
        let nr_entries = REGION_SIZE * 2;
        let aggregator = Aggregator::new(nr_entries);
        let pairs = vec![
            (1, 1),
            (REGION_SIZE as u64 + 1, 2),
            (2, 3),
            (REGION_SIZE as u64 + 2, 4),
        ];
        aggregator.set_batch(&pairs);

        // Check first region
        let region0 = aggregator.regions[0].lock().unwrap();
        if let U8s(counts) = &region0.rep {
            assert_eq!(counts[1], 1);
            assert_eq!(counts[2], 3);
        } else {
            panic!("Expected U8s representation for region 0");
        }

        // Check second region
        let region1 = aggregator.regions[1].lock().unwrap();
        if let U8s(counts) = &region1.rep {
            assert_eq!(counts[1], 2);
            assert_eq!(counts[2], 4);
        } else {
            panic!("Expected U8s representation for region 1");
        }
    }

    #[test]
    fn test_aggregator_set_empty_pairs() {
        let aggregator = Aggregator::new(REGION_SIZE);
        let pairs: Vec<(u64, u32)> = vec![];
        aggregator.set_batch(&pairs);

        // Verify that no changes were made
        for region in &aggregator.regions {
            let region = region.lock().unwrap();
            assert!(matches!(region.rep, NoCounts));
        }
    }

    #[test]
    fn test_aggregator_set_upgrade_path() {
        let aggregator = Aggregator::new(REGION_SIZE);

        // Set initial values
        aggregator.set_batch(&[(1, 1), (2, 2)]);

        // Upgrade to U8s
        aggregator.set_batch(&[(3, 3)]);

        // Upgrade to U16s
        aggregator.set_batch(&[(4, 256)]);

        // Upgrade to U32s
        aggregator.set_batch(&[(5, 65536)]);

        let region = aggregator.regions[0].lock().unwrap();
        if let U32s(counts) = &region.rep {
            assert_eq!(counts[1], 1);
            assert_eq!(counts[2], 2);
            assert_eq!(counts[3], 3);
            assert_eq!(counts[4], 256);
            assert_eq!(counts[5], 65536);
        } else {
            panic!("Expected U32s representation");
        }
    }

    #[test]
    fn test_aggregator_allocation_tracking_with_set() {
        let aggregator = Aggregator::new(REGION_SIZE);

        // Start with 0 allocated blocks
        assert_eq!(aggregator.get_nr_allocated().unwrap(), 0);

        // Set some blocks to non-zero values (should increase allocation count)
        aggregator.set_batch(&[(1, 1), (2, 2), (3, 3)]);
        assert_eq!(aggregator.get_nr_allocated().unwrap(), 3);

        // Set some of those blocks to zero (should decrease allocation count)
        aggregator.set_batch(&[(1, 0), (3, 0)]);
        assert_eq!(aggregator.get_nr_allocated().unwrap(), 1);

        // Set some new blocks and update existing ones
        aggregator.set_batch(&[(4, 5), (2, 10), (5, 1)]);
        assert_eq!(aggregator.get_nr_allocated().unwrap(), 3);

        // Set all to zero
        aggregator.set_batch(&[(2, 0), (4, 0), (5, 0)]);
        assert_eq!(aggregator.get_nr_allocated().unwrap(), 0);
    }

    #[test]
    fn test_aggregator_allocation_tracking_mixed_operations() {
        let aggregator = Aggregator::new(REGION_SIZE);

        // Start with increments
        aggregator.increment(&[1, 2, 3]);
        assert_eq!(aggregator.get_nr_allocated().unwrap(), 3);

        // Use set_batch to change some values
        aggregator.set_batch(&[(1, 0), (4, 5)]); // Remove 1, add 4
        assert_eq!(aggregator.get_nr_allocated().unwrap(), 3);

        // Use test_and_inc
        let _results = aggregator.test_and_inc(&[5, 6]);
        assert_eq!(aggregator.get_nr_allocated().unwrap(), 5);

        // Set all allocated blocks to zero
        aggregator.set_batch(&[(2, 0), (3, 0), (4, 0), (5, 0), (6, 0)]);
        assert_eq!(aggregator.get_nr_allocated().unwrap(), 0);
    }
}

//--------------------------------
