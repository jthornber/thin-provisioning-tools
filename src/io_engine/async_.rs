use io_uring::{opcode, types, IoUring};
use libc::iovec;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{self, Result};
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::vec::Vec;

use crate::io_engine::buffer_pool::{BufferPool, IOBlock};
use crate::io_engine::ring_pool::*;
use crate::io_engine::*;

//--------------------------------

/// Number of rings each engine creates.  Should be related to the
/// number of threads that will be concurrently using the engine.
const NR_RINGS: usize = 16;
// FIXME: reduce this if it makes no difference to performance.
const QUEUE_DEPTH: u32 = 256;
/// Maximum number of IO blocks that can be read in a single io_uring operation
const MAX_BLOCKS_PER_READ: usize = 64;
/// Minimum block size in bytes (4KB)
const MIN_BLOCK_SIZE: usize = 4 * 1024;
/// Maximum block size in bytes (16MB)
const MAX_BLOCK_SIZE: usize = 16 * 1024 * 1024;

//--------------------------------

// A couple of utility functions to make it easier to construct io errors
fn io_err<T>(msg: &str) -> io::Result<T> {
    Err(io::Error::other(msg))
}

fn to_io_result<T, E>(r: std::result::Result<T, E>, msg: &str) -> io::Result<T> {
    r.map_err(|_| io::Error::other(msg))
}

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
pub struct AsyncReader<'a> {
    fd: RawFd,

    /// Pool of IO blocks for buffering data
    io_blocks: &'a mut BufferPool,

    /// The io_uring instance for async IO operations
    ring: &'a mut IoUring,
}

impl<'a> AsyncReader<'a> {
    pub fn new(
        fd: RawFd,
        io_blocks: &'a mut BufferPool,
        ring: &'a mut IoUring,
    ) -> io::Result<Self> {
        let block_size = io_blocks.get_block_size();

        // Validate input parameters
        if !(MIN_BLOCK_SIZE..=MAX_BLOCK_SIZE).contains(&block_size) {
            return io_err(&format!(
                "block_size must be between {} and {} bytes",
                MIN_BLOCK_SIZE, MAX_BLOCK_SIZE
            ));
        }

        Ok(Self {
            fd,
            io_blocks,
            ring,
        })
    }

    fn prepare_io_request(
        &mut self,
        start_block: u64,
        remaining_blocks: &[u64],
    ) -> (Option<Box<IoData>>, u64) {
        let mut blocks_this_read = 0;
        let mut io_data = IoData {
            iov: Vec::with_capacity(MAX_BLOCKS_PER_READ),
            blocks: Vec::with_capacity(MAX_BLOCKS_PER_READ),
        };

        while blocks_this_read < MAX_BLOCKS_PER_READ as u64
            && blocks_this_read < remaining_blocks.len() as u64
            && remaining_blocks[blocks_this_read as usize] == start_block + blocks_this_read
        {
            if let Some(block) = self.io_blocks.get(start_block + blocks_this_read) {
                let iov_base = block.data as *mut _;
                let iov_len = self.io_blocks.get_block_size();
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

    fn submit_read_request(&mut self, io_data: Box<IoData>, offset: u64) -> io::Result<()> {
        let read_op = opcode::Readv::new(
            types::Fd(self.fd),
            io_data.iov.as_ptr(),
            io_data.iov.len() as u32,
        )
        .offset(offset)
        .build()
        .user_data(Box::into_raw(io_data) as u64);

        to_io_result(
            unsafe { self.ring.submission().push(&read_op) },
            "submission queue is full",
        )
    }

    fn process_completions<F>(
        &mut self,
        inflight: &mut u64,
        total_bytes_read: &mut u64,
        callback: &mut F,
    ) -> Result<()>
    where
        F: FnMut(u64, Result<&[u8]>),
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
                        Ok(unsafe {
                            std::slice::from_raw_parts(b.data, self.io_blocks.get_block_size())
                        }),
                    );
                }
            }

            for block in io_data.blocks.drain(..) {
                self.io_blocks.put(block);
            }

            *inflight = inflight.checked_sub(1).unwrap_or(0);
        });

        Ok(())
    }

    fn read_io_blocks_<F>(&mut self, block_indices: &[u64], mut callback: F) -> Result<()>
    where
        F: FnMut(u64, Result<&[u8]>),
    {
        let blocks_to_read = block_indices.len() as u64;
        let mut blocks_read = 0u64;
        let mut inflight = 0u64;
        let mut total_bytes_read = 0u64;

        while blocks_read < blocks_to_read || inflight > 0 {
            // Submit as many I/O requests as possible
            while blocks_read < blocks_to_read
                && inflight < QUEUE_DEPTH as u64
                && !self.io_blocks.is_empty()
            {
                let start_block = block_indices[blocks_read as usize];
                let offset = start_block * self.io_blocks.get_block_size() as u64;

                // Prepare the next IO request
                let (io_data, blocks_this_read) =
                    self.prepare_io_request(start_block, &block_indices[blocks_read as usize..]);

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

    /// Utility function to read smaller blocks using larger IO blocks.  Indices must be sorted into
    /// ascending order.
    ///
    /// # Arguments
    /// * `blocks` - Iterator over logical block numbers to read
    /// * `handler` - Handler to receive the read results
    ///
    /// # Returns
    /// * `io::Result<()>` - Success or an IO error
    fn stream_blocks(
        &mut self,
        blocks: &mut dyn Iterator<Item = u64>,
        handler: &mut dyn ReadHandler,
    ) -> io::Result<()> {
        let io_block_size = self.io_blocks.get_block_size();
        assert!(BLOCK_SIZE <= io_block_size);
        assert!(
            io_block_size.is_multiple_of(BLOCK_SIZE),
            "IO block size must be a multiple of small block size"
        );
        let blocks_per_io = io_block_size / BLOCK_SIZE;
        assert!(blocks_per_io <= 64); // so we can fit the bitset in a u64

        // Map small blocks to IO blocks
        let io_block_map = utils::map_small_blocks_to_io(blocks, blocks_per_io);

        // Collect unique IO block indices
        let mut io_indices: Vec<u64> = io_block_map.keys().cloned().collect();
        io_indices.sort_unstable();

        // Read IO blocks
        self.read_io_blocks_(&io_indices, |io_idx, result| {
            utils::process_io_block_result(io_idx, result, blocks_per_io, &io_block_map, handler);
        })?;

        handler.complete();
        Ok(())
    }
}

//--------------------------------

pub struct AsyncIoEngine {
    input: File,
    nr_blocks: u64,
    rings: RingPool,
}

unsafe impl Sync for AsyncIoEngine {}
unsafe impl Send for AsyncIoEngine {}

impl AsyncIoEngine {
    pub fn new_with<P: AsRef<Path>>(path: P, writable: bool, excl: bool) -> Result<Self> {
        let nr_blocks = get_nr_blocks(path.as_ref())?;

        let mut flags = libc::O_DIRECT;
        if excl {
            flags |= libc::O_EXCL;
        }
        let input = OpenOptions::new()
            .read(true)
            .write(writable)
            .custom_flags(flags)
            .open(path)?;

        let rings = RingPool::new(NR_RINGS, QUEUE_DEPTH)?;

        Ok(Self {
            input,
            nr_blocks,
            rings,
        })
    }

    pub fn new<P: AsRef<Path>>(path: P, writable: bool) -> Result<Self> {
        Self::new_with(path, writable, true)
    }

    fn get_fd(&self) -> RawFd {
        self.input.as_raw_fd()
    }

    fn exec_op(&self, op: &io_uring::squeue::Entry) -> Result<io_uring::cqueue::Entry> {
        self.rings.with_ring(|ring| {
            // Submit operation
            let mut sq = ring.submission();
            let _e = to_io_result(unsafe { sq.push(op) }, "submission queue is full");
            drop(sq);

            // Wait for completion
            ring.submit_and_wait(1)?;

            // Get completion result
            let mut cq = ring.completion();
            if let Some(ret) = cq.next() {
                Ok(ret)
            } else {
                io_err("completion queue is empty")
            }
        })
    }
}

impl IoEngine for AsyncIoEngine {
    fn get_nr_blocks(&self) -> u64 {
        self.nr_blocks
    }

    // FIXME: remove this
    fn get_batch_size(&self) -> usize {
        QUEUE_DEPTH as usize
    }

    fn read(&self, b: u64) -> Result<Block> {
        let block = Block::new(b);
        let loc = b * BLOCK_SIZE as u64;

        // Prepare read operation
        let read_op = opcode::Read::new(
            types::Fd(self.get_fd()),
            unsafe { block.get_raw_ptr() },
            BLOCK_SIZE as u32,
        )
        .offset(loc)
        .build();

        let ret = self.exec_op(&read_op)?;

        if ret.result() < 0 {
            return Err(io::Error::from_raw_os_error(-ret.result()));
        }

        if ret.result() as usize != BLOCK_SIZE {
            return io_err("short read");
        }

        Ok(block)
    }

    fn read_many(&self, blocks: &[u64]) -> Result<Vec<Result<Block>>> {
        self.rings.with_ring(|ring| {
            let mut results = Vec::with_capacity(blocks.len());
            for _ in 0..blocks.len() {
                results.push(io_err("read not completed"));
            }

            let mut inflight = 0;
            let mut completed = 0;
            let mut block_data = Vec::with_capacity(blocks.len());

            // Allocate blocks for all reads
            for &block_idx in blocks {
                block_data.push(Some(Block::new(block_idx)));
            }

            // Process all blocks
            while completed < blocks.len() {
                // Submit as many operations as possible
                while completed + inflight < blocks.len() && inflight < QUEUE_DEPTH as usize {
                    let idx = completed + inflight;
                    if let Some(block) = &block_data[idx] {
                        let block_idx = blocks[idx];
                        let offset = block_idx * BLOCK_SIZE as u64;

                        // Prepare read operation
                        let read_op = opcode::Read::new(
                            types::Fd(self.get_fd()),
                            unsafe { block.get_raw_ptr() },
                            BLOCK_SIZE as u32,
                        )
                        .offset(offset)
                        .build()
                        .user_data(idx as u64);

                        // Push to submission queue
                        if unsafe { ring.submission().push(&read_op) }.is_err() {
                            // If queue is full, break and process some completions
                            break;
                        }

                        inflight += 1;
                    } else {
                        // This shouldn't happen, but just in case
                        completed += 1;
                    }
                }

                // Submit operations and wait for at least one completion
                ring.submit_and_wait(1)?;

                // Process completions
                ring.completion().for_each(|cqe| {
                    let idx = cqe.user_data() as usize;

                    if cqe.result() < 0 {
                        // Error occurred
                        results[idx] = Err(io::Error::from_raw_os_error(-cqe.result()));
                    } else if cqe.result() as usize != BLOCK_SIZE {
                        // Short read
                        results[idx] = io_err("short read");
                    } else {
                        // Successful read - take the block from block_data
                        results[idx] = Ok(block_data[idx].take().unwrap());
                    }

                    inflight -= 1;
                    completed += 1;
                });
            }

            Ok(results)
        })
    }

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
        &self,
        io_block_pool: &mut BufferPool,
        blocks: &mut dyn Iterator<Item = u64>,
        handler: &mut dyn ReadHandler,
    ) -> io::Result<()> {
        self.rings.with_ring(|ring| {
            let mut reader = AsyncReader::new(self.get_fd(), io_block_pool, ring)?;
            reader.stream_blocks(blocks, handler)
        })
    }

    fn write(&self, block: &Block) -> Result<()> {
        let loc = block.loc * BLOCK_SIZE as u64;

        // Prepare write operation
        let write_op = opcode::Write::new(
            types::Fd(self.get_fd()),
            unsafe { block.get_raw_ptr() },
            BLOCK_SIZE as u32,
        )
        .offset(loc)
        .build();

        let ret = self.exec_op(&write_op)?;

        if ret.result() < 0 {
            return Err(io::Error::from_raw_os_error(-ret.result()));
        }

        if ret.result() as usize != BLOCK_SIZE {
            return io_err("short write");
        }

        Ok(())
    }

    fn write_many(&self, blocks: &[Block]) -> Result<Vec<Result<()>>> {
        self.rings.with_ring(|ring| {
            let mut results = Vec::with_capacity(blocks.len());
            for _ in 0..blocks.len() {
                results.push(io_err("write not completed"));
            }

            let mut inflight = 0;
            let mut completed = 0;

            // Process all blocks
            while completed < blocks.len() {
                // Submit as many operations as possible
                while completed + inflight < blocks.len() && inflight < QUEUE_DEPTH as usize {
                    let idx = completed + inflight;
                    let block = &blocks[idx];
                    let offset = block.loc * BLOCK_SIZE as u64;

                    // Prepare write operation
                    let write_op = opcode::Write::new(
                        types::Fd(self.get_fd()),
                        unsafe { block.get_raw_ptr() },
                        BLOCK_SIZE as u32,
                    )
                    .offset(offset)
                    .build()
                    .user_data(idx as u64);

                    // Push to submission queue
                    if unsafe { ring.submission().push(&write_op) }.is_err() {
                        // If queue is full, break and process some completions
                        break;
                    }

                    inflight += 1;
                }

                // Submit operations and wait for at least one completion
                ring.submit_and_wait(1)?;

                // Process completions
                ring.completion().for_each(|cqe| {
                    let idx = cqe.user_data() as usize;

                    if cqe.result() < 0 {
                        // Error occurred
                        results[idx] = Err(io::Error::from_raw_os_error(-cqe.result()));
                    } else if cqe.result() as usize != BLOCK_SIZE {
                        // Short write
                        results[idx] = io_err("short write");
                    } else {
                        // Successful write
                        results[idx] = Ok(());
                    }

                    inflight -= 1;
                    completed += 1;
                });
            }

            Ok(results)
        })
    }
}

//------------------------------------------
