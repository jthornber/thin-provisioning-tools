use fixedbitset::*;
use std::intrinsics::unlikely;
use std::sync::Mutex;

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
}

impl Default for Region {
    fn default() -> Self {
        Self { rep: NoCounts }
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
                for i in 0..blocks.len() {
                    let b = blocks[i] % REGION_SIZE as u64;
                    if bits.contains(b as usize) {
                        return IncResult::NeedUpgrade(i);
                    }
                    bits.insert(b as usize);
                }
            }
            U8s(counts) => {
                for i in 0..blocks.len() {
                    let b = blocks[i] % REGION_SIZE as u64;
                    if unlikely(counts[b as usize] == u8::MAX) {
                        return IncResult::NeedUpgrade(i);
                    }
                    counts[b as usize] += 1;
                }
            }
            U16s(counts) => {
                for i in 0..blocks.len() {
                    let b = blocks[i] % REGION_SIZE as u64;
                    if counts[b as usize] == u16::MAX {
                        return IncResult::NeedUpgrade(i);
                    }
                    counts[b as usize] += 1;
                }
            }
            U32s(counts) => {
                for b in blocks {
                    let b = b % REGION_SIZE as u64;
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
    ///               slice determines the maximum number of blocks to look up.
    ///
    /// # Returns
    ///
    /// Returns the number of blocks actually processed. This may be less than `results.len()` if
    /// the range extends beyond the end of the Aggregator's entries.
    ///
    /// # Examples
    ///
    /// ```
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
                    results[i - b as usize] = counts[i] as u32;
                }
            }
        }
        return e as u64 - b;
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
/// use your_crate::Aggregator;
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
    ///                  the number of regions by dividing `nr_entries` by
    ///                  `REGION_SIZE`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use your_crate::Aggregator;
    ///
    /// let aggregator = Aggregator::new(1024);
    /// ```
    pub fn new(nr_entries: usize) -> Self {
        let nr_regions = (nr_entries + REGION_SIZE - 1) / REGION_SIZE;
        let regions = (0..nr_regions)
            .map(|_| Mutex::new(Region::default()))
            .collect();
        Self {
            nr_entries,
            regions,
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
    ///              be sorted in ascending order to maximize performance.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use your_crate::Aggregator;
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
        for i in 1..blocks.len() {
            let current_region = blocks[i] / REGION_SIZE as u64;
            if current_region != start_region {
                let mut region = self.regions[start_region as usize].lock().unwrap();
                region.increment(&blocks[start_idx..i]);
                start_idx = i;
                start_region = current_region;
            }
        }

        let mut region = self.regions[start_region as usize].lock().unwrap();
        region.increment(&blocks[start_idx..]);
    }

    pub fn inc_single(&self, block: u64) {
        self.increment(&[block]);
    }

    pub fn get_nr_blocks(&self) -> usize {
        self.nr_entries
    }

    pub fn lookup(&mut self, begin: u64, results: &mut [u32]) -> u64 {
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

    pub fn rep_size(&self) -> usize {
        eprintln!("{} regions", self.regions.len());
        eprintln!("Region size = {}", std::mem::size_of::<Region>());
        let mut total = 0;
        for i in 0..self.regions.len() {
            let region = self.regions[i].lock().unwrap();
            total += std::mem::size_of::<Mutex<Region>>();
            total += region.rep_size();
        }
        total
    }
}

//--------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_initial_no_counts() {
        let mut region = Region::default();
        let blocks = [1, 2, 3];
        let result = region.increment_(&blocks);
        assert!(matches!(result, IncResult::NeedUpgrade(0)));
    }

    #[test]
    fn test_u8_increment() {
        let mut region = Region {
            rep: U8s(vec![0; REGION_SIZE]),
        };
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
        let mut region = Region { rep: U8s(counts) };
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
        let mut region = Region {
            rep: Bits(FixedBitSet::with_capacity(REGION_SIZE)),
        };
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
        let region = Region { rep: U8s(counts) };
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
        let region = Region { rep: U16s(counts) };
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
        let region = Region { rep: U32s(counts) };
        let mut results = vec![0; 3];
        let blocks_read = region.lookup(0, &mut results);
        assert_eq!(blocks_read, 3);
        assert_eq!(results, vec![1_000_000, 2_000_000, 0]);
    }

    #[test]
    fn test_aggregator_lookup_single_region() {
        let mut aggregator = Aggregator::new(REGION_SIZE);
        aggregator.increment(&[1, 2, 1, 3]);
        let mut results = vec![0; 5];
        let blocks_read = aggregator.lookup(0, &mut results);
        assert_eq!(blocks_read, 5);
        assert_eq!(results, vec![0, 2, 1, 1, 0]);
    }

    #[test]
    fn test_aggregator_lookup_multiple_regions() {
        let mut aggregator = Aggregator::new(REGION_SIZE * 2);
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
        let mut aggregator = Aggregator::new(REGION_SIZE);
        let mut results = vec![0; 5];
        let blocks_read = aggregator.lookup(REGION_SIZE as u64, &mut results);
        assert_eq!(blocks_read, 0);
        assert_eq!(results, vec![0; 5]);
    }

    #[test]
    fn test_aggregator_lookup_partial_read() {
        let mut aggregator = Aggregator::new(REGION_SIZE + 5);
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
}

//--------------------------------
