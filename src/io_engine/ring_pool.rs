use io_uring::IoUring;
use std::collections::VecDeque;
use std::io::Result;
use std::sync::{Condvar, Mutex};
use std::vec::Vec;

//--------------------------------

/// Manages a pool of io_uring instances for concurrent IO operations
pub struct RingPool {
    available_rings: Mutex<VecDeque<usize>>,
    rings: Vec<Mutex<IoUring>>,
    condvar: Condvar,
}

impl RingPool {
    /// Creates a new pool with the specified number of io_uring instances
    pub fn new(count: usize, queue_depth: u32) -> Result<Self> {
        let mut rings = Vec::with_capacity(count);
        let mut available_rings = VecDeque::with_capacity(count);

        for i in 0..count {
            let ring = IoUring::new(queue_depth)?;
            rings.push(Mutex::new(ring));
            available_rings.push_back(i);
        }

        Ok(Self {
            available_rings: Mutex::new(available_rings),
            rings,
            condvar: Condvar::new(),
        })
    }

    /// Executes an operation with a borrowed io_uring instance
    pub fn with_ring<F, T>(&self, f: F) -> T
    where
        F: FnOnce(&mut IoUring) -> T,
    {
        // Get an available ring index
        let ring_idx = {
            let mut available = self.available_rings.lock().unwrap();
            while available.is_empty() {
                available = self.condvar.wait(available).unwrap();
            }
            available.pop_front().unwrap()
        };

        // Use the ring
        let result = {
            let mut ring = self.rings[ring_idx].lock().unwrap();
            f(&mut ring)
        };

        // Return the ring to the pool
        {
            let mut available = self.available_rings.lock().unwrap();
            available.push_back(ring_idx);
            self.condvar.notify_one();
        }

        result
    }

    /// Returns the number of rings in the pool
    pub fn len(&self) -> usize {
        self.rings.len()
    }

    /// Returns whether the pool is empty
    pub fn is_empty(&self) -> bool {
        self.rings.is_empty()
    }
}

//--------------------------------
