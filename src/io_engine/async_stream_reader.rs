/// This module provides asynchronous block-oriented reading capabilities using io_uring.
/// It manages two distinct block sizes for different purposes:
///
/// 1. IO Block Size: Used by the BufferPool for background IO operations
///    - These are larger blocks managed internally by the BufferPool
///    - Optimized for efficient IO operations with the storage device
///    - Used in the actual read_io_blocks_() method for physical reads
///
/// 2. Logical Block Size: Used by the StreamReader::read_blocks() interface
///    - These are typically smaller blocks that represent the logical view of the data
///    - Callers of read_blocks() work with these logical block sizes
///    - The AsyncStreamReader internally maps these logical blocks to IO blocks
///
/// The relationship between these block sizes is handled transparently by the reader,
/// which may batch multiple logical blocks into a single IO operation for efficiency.
use io_uring::{opcode, types, IoUring};
use libc::iovec;
use std::io;
use std::os::fd::RawFd;
use std::vec::Vec;
use std::collections::HashMap;

use crate::io_engine::base::*;
use crate::io_engine::buffer_pool::{BufferPool, IOBlock};

//--------------------------------

const QUEUE_DEPTH: u32 = 256;
/// Maximum number of IO blocks that can be read in a single io_uring operation
const MAX_BLOCKS_PER_READ: usize = 64;

//--------------------------------

/// Represents a single IO operation's data and buffer management
struct IoData {
    /// Array of IO vectors for scatter/gather operations
    iov: Vec<iovec>,
    /// The actual IO blocks being used for this operation
    blocks: Vec<IOBlock>,
}

/// AsyncStreamReader provides asynchronous block-oriented reading capabilities using io_uring.
/// It manages two different block sizes:
///
/// - The IO block size (block_size): Used for actual IO operations through the BufferPool
///   This is typically larger to optimize IO performance with the storage device.
///
/// - The logical block size: Used by callers of read_blocks()
///   This is the block size that the higher-level code works with, and may be smaller
///   than the IO block size.
///
/// The reader automatically handles the mapping between these two block sizes, potentially
/// batching multiple logical blocks into single IO operations for better performance.
pub struct AsyncStreamReader {
    fd: RawFd,
    /// The size of IO blocks used for physical reads
    block_size: usize,
    /// Pool of IO blocks for buffering data
    blocks: BufferPool,
    /// The io_uring instance for async IO operations
    ring: IoUring,
}

unsafe impl Send for AsyncStreamReader {}
unsafe impl Sync for AsyncStreamReader {}

impl AsyncStreamReader {
    /// Creates a new AsyncStreamReader with the specified file descriptor, block size, and buffer size.
    /// 
    /// # Arguments
    /// * `fd` - Raw file descriptor for the input file
    /// * `block_size` - Size of IO blocks used for physical reads
    /// * `buffer_size_mb` - Size of the buffer pool in megabytes
    /// 
    /// # Returns
    /// * `io::Result<Self>` - A new AsyncStreamReader instance or an IO error
    pub fn new(fd: RawFd, block_size: usize, buffer_size_mb: usize) -> io::Result<Self> {
        let buffer_size = buffer_size_mb * 1024 * 1024;
        let num_buffer_blocks = buffer_size / block_size;

        // Initialize buffer pool
        let blocks = BufferPool::new(num_buffer_blocks as usize, block_size);

        // Initialize io_uring
        let ring = IoUring::new(QUEUE_DEPTH)?;

        Ok(Self {
            fd,
            block_size,
            blocks,
            ring,
        })
    }

    /// Builds an IoData structure for a sequence of contiguous blocks starting at the specified block.
    /// 
    /// # Arguments
    /// * `start_block` - The starting block number
    /// * `remaining_blocks` - Slice of remaining block numbers to process
    /// 
    /// # Returns
    /// * A tuple containing:
    ///   - An optional boxed IoData structure
    ///   - The number of blocks prepared
    fn prepare_io_request(&mut self, start_block: u64, remaining_blocks: &[u64]) -> (Option<Box<IoData>>, u64) {
        let mut blocks_this_read = 0;
        let mut io_data = IoData {
            iov: Vec::with_capacity(MAX_BLOCKS_PER_READ),
            blocks: Vec::with_capacity(MAX_BLOCKS_PER_READ),
        };

        while blocks_this_read < MAX_BLOCKS_PER_READ as u64
            && blocks_this_read < remaining_blocks.len() as u64
            && remaining_blocks[blocks_this_read as usize] == start_block + blocks_this_read
        {
            if let Some(block) = self.blocks.get(start_block + blocks_this_read) {
                let iov_base = block.data as *mut _;
                let iov_len = self.block_size as usize;
                io_data.iov.push(iovec { iov_base, iov_len });
                io_data.blocks.push(block);
                blocks_this_read += 1;
            } else {
                break;
            }
        }

        if blocks_this_read > 0 {
            (Some(Box::new(io_data)), blocks_this_read)
        } else {
            (None, 0)
        }
    }

    /// Submits a read request to io_uring for the given IoData.
    /// 
    /// # Arguments
    /// * `io_data` - The IoData structure containing the IO vectors and blocks to read
    /// * `offset` - The offset in bytes where to start reading
    /// 
    /// # Returns
    /// * `io::Result<()>` - Success or an IO error
    fn submit_read_request(&mut self, io_data: Box<IoData>, offset: u64) -> io::Result<()> {
        let read_op = opcode::Readv::new(
            types::Fd(self.fd),
            io_data.iov.as_ptr(),
            io_data.iov.len() as u32,
        )
        .offset(offset)
        .build()
        .user_data(Box::into_raw(io_data) as u64);

        unsafe {
            self.ring
                .submission()
                .push(&read_op)
                .expect("submission queue is full");
        }
        Ok(())
    }

    /// Processes completion events from io_uring and invokes callbacks for completed reads.
    /// 
    /// # Arguments
    /// * `inflight` - Number of currently in-flight IO operations
    /// * `total_bytes_read` - Running total of bytes read
    /// * `callback` - Function to call with the results of each completed read
    /// 
    /// # Returns
    /// * `io::Result<()>` - Success or an IO error
    fn process_completions<F>(&mut self, inflight: &mut u64, total_bytes_read: &mut u64, callback: &mut F) -> io::Result<()>
    where
        F: FnMut(u64, Result<&[u8], io::Error>),
    {
        self.ring.completion().for_each(|cqe| {
            let mut io_data = unsafe { Box::from_raw(cqe.user_data() as *mut IoData) };
            if cqe.result() < 0 {
                for b in &io_data.blocks {
                    callback(b.loc, Err(io::Error::from_raw_os_error(-cqe.result())));
                }
            } else {
                *total_bytes_read += cqe.result() as u64;

                for b in &io_data.blocks {
                    callback(
                        b.loc,
                        Ok(unsafe { std::slice::from_raw_parts(b.data, self.block_size) }),
                    );
                }
            }

            for block in io_data.blocks.drain(..) {
                self.blocks.put(block);
            }

            *inflight = inflight.checked_sub(1).unwrap_or(0);
        });
        Ok(())
    }

    /// Reads a sequence of IO blocks asynchronously using io_uring.
    /// 
    /// # Arguments
    /// * `block_indices` - Slice of block indices to read
    /// * `callback` - Function to call with the results of each read
    /// 
    /// # Returns
    /// * `io::Result<()>` - Success or an IO error
    fn read_io_blocks_<F>(&mut self, block_indices: &[u64], mut callback: F) -> io::Result<()>
    where
        F: FnMut(u64, Result<&[u8], io::Error>),
    {
        let blocks_to_read = block_indices.len() as u64;
        let mut blocks_read = 0u64;
        let mut inflight = 0u64;
        let mut total_bytes_read = 0u64;

        while blocks_read < blocks_to_read || inflight > 0 {
            // Submit as many I/O requests as possible
            while blocks_read < blocks_to_read && inflight < QUEUE_DEPTH as u64 && !self.blocks.is_empty() {
                let start_block = block_indices[blocks_read as usize];
                let offset = start_block * self.block_size as u64;
                
                // Prepare the next IO request
                let (io_data, blocks_this_read) = self.prepare_io_request(
                    start_block,
                    &block_indices[blocks_read as usize..]
                );

                if let Some(io_data) = io_data {
                    // Submit the read request
                    self.submit_read_request(io_data, offset)?;
                    blocks_read += blocks_this_read;
                    inflight += 1;
                } else {
                    break;
                }
            }

            // Submit and wait for completions
            self.ring.submit_and_wait(1)?;

            // Process completions
            self.process_completions(&mut inflight, &mut total_bytes_read, &mut callback)?;
        }

        Ok(())
    }

    /// Maps smaller logical blocks to larger IO blocks for efficient reading.
    /// 
    /// # Arguments
    /// * `blocks` - Iterator over logical block numbers
    /// * `blocks_per_io` - Number of logical blocks that fit in one IO block
    /// 
    /// # Returns
    /// * `HashMap<u64, u64>` - Mapping from logical block numbers to IO block numbers
    fn map_small_blocks_to_io(blocks: &mut dyn Iterator<Item = u64>, blocks_per_io: usize) -> HashMap<u64, u64> {
        let mut io_block_map: HashMap<u64, u64> = HashMap::new();
        let mut current_io_idx: Option<u64> = None;
        let mut current_bitmask: u64 = 0;

        for small_idx in blocks {
            let io_idx = small_idx / blocks_per_io as u64;
            let bit = small_idx % blocks_per_io as u64;

            if Some(io_idx) != current_io_idx {
                if let Some(idx) = current_io_idx {
                    io_block_map.insert(idx, current_bitmask);
                }
                current_io_idx = Some(io_idx);
                current_bitmask = 0;
            }
            current_bitmask |= 1 << bit;
        }

        // Push the last accumulated io_idx and bitmask
        if let Some(idx) = current_io_idx {
            io_block_map.insert(idx, current_bitmask);
        }

        io_block_map
    }

    /// Processes the result of reading an IO block and splits it into logical blocks.
    /// 
    /// # Arguments
    /// * `io_idx` - Index of the IO block
    /// * `result` - Result containing the read data or an error
    /// * `blocks_per_io` - Number of logical blocks per IO block
    /// * `io_block_map` - Mapping from logical to IO block numbers
    /// * `handler` - Handler to receive the processed results
    fn process_io_block_result(
        io_idx: u64,
        result: Result<&[u8], io::Error>,
        blocks_per_io: usize,
        io_block_map: &HashMap<u64, u64>,
        handler: &mut dyn ReadHandler,
    ) {
        // Find the bitmask for the current io_idx
        if let Some(bitmask) = io_block_map.get(&io_idx) {
            let mut bm = *bitmask;
            while bm != 0 {
                let tz = bm.trailing_zeros();
                if tz >= blocks_per_io as u32 {
                    break;
                }
                bm &= !(1 << tz);
                let small_idx = io_idx * blocks_per_io as u64 + tz as u64;
                
                match &result {
                    Ok(data) => {
                        let offset = (tz as usize) * BLOCK_SIZE;
                        let small_data = &data[offset..offset + BLOCK_SIZE];
                        handler.handle(small_idx, Ok(small_data));
                    }
                    Err(_) => {
                        handler.handle(small_idx, Err(io::Error::last_os_error()));
                    }
                }
            }
        }
    }

}

impl StreamReader for AsyncStreamReader {
    /// Utility function to read smaller blocks using larger IO blocks.  Indices must be sorted into
    /// ascending order.
    /// 
    /// # Arguments
    /// * `blocks` - Iterator over logical block numbers to read
    /// * `handler` - Handler to receive the read results
    /// 
    /// # Returns
    /// * `io::Result<()>` - Success or an IO error
    fn read_blocks(
        &mut self,
        blocks: &mut dyn Iterator<Item = u64>,
        handler: &mut dyn ReadHandler,
    ) -> io::Result<()> {
        let io_block_size = self.block_size;
        assert!(BLOCK_SIZE <= io_block_size);
        assert!(
            io_block_size % BLOCK_SIZE == 0,
            "IO block size must be a multiple of small block size"
        );
        let blocks_per_io = io_block_size / BLOCK_SIZE;
        assert!(blocks_per_io <= 64); // so we can fit the bitset in a u64

        // Map small blocks to IO blocks
        let io_block_map = Self::map_small_blocks_to_io(blocks, blocks_per_io);

        // Collect unique IO block indices
        let mut io_indices: Vec<u64> = io_block_map.keys().cloned().collect();
        io_indices.sort_unstable();

        // Read IO blocks
        self.read_io_blocks_(&io_indices, |io_idx, result| {
            Self::process_io_block_result(io_idx, result, blocks_per_io, &io_block_map, handler);
        })?;

        handler.complete();
        Ok(())
    }
}

//--------------------------------
