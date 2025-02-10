use std::alloc::{alloc, dealloc, Layout};
use std::vec::Vec;

//--------------------------------

/// Represents a block of IO memory with its location and data pointer.
///
/// # Fields
/// * `loc` - The logical block address or location identifier
/// * `data` - Raw pointer to the block's data in memory
#[derive(Clone)]
pub struct IOBlock {
    pub loc: u64,
    pub data: *mut u8,
}

// SAFETY: Block contains a raw pointer, but we ensure it's used safely.
// The data pointer is always allocated and managed by the BufferPool,
// and we maintain exclusive access to the memory it points to.
unsafe impl Send for IOBlock {}

//--------------------------------

/// A memory pool that manages a fixed number of IO blocks.
///
/// The BufferPool allocates a contiguous chunk of memory and divides it into
/// fixed-size blocks that can be borrowed and returned. This helps reduce
/// memory fragmentation and provides efficient memory reuse for IO operations.
///
/// # Fields
/// * `nr_blocks` - Number of blocks in the pool
/// * `block_size` - Size of each block in bytes
/// * `buffer` - Raw pointer to the allocated memory region
/// * `blocks` - Vector of available blocks for use
pub struct BufferPool {
    nr_blocks: usize,
    block_size: usize,
    buffer: *mut u8,
    blocks: Vec<IOBlock>,
}

impl BufferPool {
    /// Creates the memory layout for the buffer pool.
    ///
    /// # Arguments
    /// * `nr_blocks` - Number of blocks to allocate
    /// * `block_size` - Size of each block in bytes
    ///
    /// # Returns
    /// A Layout describing the memory requirements for the pool
    fn layout(nr_blocks: usize, block_size: usize) -> Layout {
        Layout::from_size_align(block_size * nr_blocks, block_size).unwrap()
    }

    /// Creates a new BufferPool with the specified number and size of blocks.
    ///
    /// # Arguments
    /// * `nr_blocks` - Number of blocks to allocate
    /// * `block_size` - Size of each block in bytes
    ///
    /// # Panics
    /// Panics if memory allocation fails
    pub fn new(nr_blocks: usize, block_size: usize) -> Self {
        let layout = Self::layout(nr_blocks, block_size);
        let buffer = unsafe { alloc(layout) };

        if buffer.is_null() {
            panic!("Failed to allocate memory for BufferPool");
        }

        let mut blocks = Vec::with_capacity(nr_blocks);
        for i in 0..nr_blocks {
            blocks.push(IOBlock {
                loc: 0,
                data: unsafe { buffer.add(i * block_size) },
            });
        }

        BufferPool {
            nr_blocks,
            block_size,
            buffer,
            blocks,
        }
    }

    pub fn get_block_size(&self) -> usize {
        self.block_size
    }

    pub fn get_nr_blocks(&self) -> usize {
        self.nr_blocks
    }

    /// Retrieves an available block from the pool and assigns it a location.
    ///
    /// # Arguments
    /// * `loc` - Location identifier to assign to the block
    ///
    /// # Returns
    /// Some(IOBlock) if a block is available, None if the pool is empty
    pub fn get(&mut self, loc: u64) -> Option<IOBlock> {
        let b = self.blocks.pop();
        b.map(|b| IOBlock { loc, data: b.data })
    }

    /// Returns a block back to the pool for reuse.
    ///
    /// # Arguments
    /// * `block` - The IOBlock to return to the pool
    pub fn put(&mut self, block: IOBlock) {
        self.blocks.push(block);
    }

    /// Checks if the pool has any available blocks.
    ///
    /// # Returns
    /// true if no blocks are available, false otherwise
    pub fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }
}

/// Ensures proper cleanup of allocated memory when the BufferPool is dropped.
impl Drop for BufferPool {
    fn drop(&mut self) {
        let layout = Self::layout(self.nr_blocks, self.block_size);
        unsafe { dealloc(self.buffer, layout) };
    }
}
