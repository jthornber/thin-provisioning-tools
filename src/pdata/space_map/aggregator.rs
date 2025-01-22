use fixedbitset::*;
use std::sync::Mutex;

//--------------------------------

const REGION_SIZE: usize = 256;

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
            NoCounts => Bits(FixedBitSet::with_capacity(256)),
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
                    if counts[b as usize] == u8::MAX {
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

    pub fn get_nr_entries(&self) -> usize {
        self.nr_entries
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
}

//--------------------------------
