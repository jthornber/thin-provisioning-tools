use io_uring::{opcode, types, IoUring};
use libc::iovec;
use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{self, Result};
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::sync::Mutex;
use std::vec::Vec;

use crate::io_engine::buffer_pool::{BufferPool, IOBlock};
use crate::io_engine::*;

//--------------------------------

// FIXME: reduce this if it makes no difference to performance.

const QUEUE_DEPTH: u32 = 256;
/// Maximum number of IO blocks that can be read in a single io_uring operation
const MAX_BLOCKS_PER_READ: usize = 64;
/// Minimum block size in bytes (4KB)
const MIN_BLOCK_SIZE: usize = 4 * 1024;
/// Maximum block size in bytes (16MB)
const MAX_BLOCK_SIZE: usize = 16 * 1024 * 1024;
/// Minimum buffer size in megabytes
const MIN_BUFFER_SIZE_MB: usize = 1;
/// Maximum buffer size in megabytes (1GB)
const MAX_BUFFER_SIZE_MB: usize = 1024;

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
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "block_size must be between {} and {} bytes",
                    MIN_BLOCK_SIZE, MAX_BLOCK_SIZE
                ),
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
                let iov_len = self.io_blocks.get_block_size() as usize;
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

        unsafe {
            self.ring
                .submission()
                .push(&read_op)
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "submission queue is full"))?;
        }
        Ok(())
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

    /// Maps smaller logical blocks to larger IO blocks for efficient reading.
    ///
    /// # Arguments
    /// * `blocks` - Iterator over logical block numbers
    /// * `blocks_per_io` - Number of logical blocks that fit in one IO block
    ///
    /// # Returns
    /// * `HashMap<u64, u64>` - Mapping from logical block numbers to IO block numbers
    fn map_small_blocks_to_io(
        blocks: &mut dyn Iterator<Item = u64>,
        blocks_per_io: usize,
    ) -> HashMap<u64, u64> {
        // Pre-allocate with a reasonable capacity
        let mut io_block_map = HashMap::with_capacity(1024);
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
        result: Result<&[u8]>,
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

struct BlockReadHandler {
    loc_to_index: HashMap<u64, usize>,
    results: Vec<Result<Block>>,
}

impl BlockReadHandler {
    fn new(blocks: &[u64]) -> Self {
        let mut loc_to_index = HashMap::new();
        for (i, b) in blocks.iter().enumerate() {
            loc_to_index.insert(*b, i);
        }

        let mut results = Vec::with_capacity(blocks.len());
        for _ in 0..blocks.len() {
            results.push(Err(io::Error::new(io::ErrorKind::Other, "missing read")));
        }

        Self {
            loc_to_index,
            results,
        }
    }

    fn get_results(self) -> Vec<Result<Block>> {
        self.results
    }
}

impl ReadHandler for BlockReadHandler {
    fn handle(&mut self, loc: u64, data: std::io::Result<&[u8]>) {
        let maybe_index = self.loc_to_index.get(&loc);

        if maybe_index.is_none() {
            // FIXME: handle more gracefully
            panic!("unexpected block read");
        }
        let index = maybe_index.unwrap();

        match data {
            Ok(data) => {
                let b = Block::new(loc);
                b.get_data().copy_from_slice(data);
                self.results[*index] = Ok(b);
            }
            Err(e) => {
                self.results[*index] = Err(e);
            }
        }
    }

    fn complete(&mut self) {
        // no op
    }
}

//--------------------------------

pub struct AsyncIoEngineInner {
    input: File,
    nr_blocks: u64,
    ring: IoUring,

    // We maintain a small pool of BLOCK_SIZE buffers for the older read_many() interface.
    small_pool: BufferPool,
}

unsafe impl Sync for AsyncIoEngineInner {}
unsafe impl Send for AsyncIoEngineInner {}

impl AsyncIoEngineInner {
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

        let ring = IoUring::new(QUEUE_DEPTH)?;

        let small_pool = BufferPool::new(QUEUE_DEPTH as usize, BLOCK_SIZE);

        Ok(Self {
            input,
            nr_blocks,
            ring,
            small_pool,
        })
    }

    pub fn new<P: AsRef<Path>>(path: P, writable: bool) -> Result<Self> {
        Self::new_with(path, writable, true)
    }

    fn get_fd(&self) -> RawFd {
        self.input.as_raw_fd()
    }

    fn get_nr_blocks(&self) -> u64 {
        self.nr_blocks
    }

    // FIXME: remove this
    fn get_batch_size(&self) -> usize {
        QUEUE_DEPTH as usize
    }

    fn read(&mut self, b: u64) -> Result<Block> {
        let block = Block::new(b);
        let loc = b * BLOCK_SIZE as u64;

        // Prepare read operation
        let read_op = opcode::Read::new(
            types::Fd(self.get_fd()),
            unsafe { block.get_raw_ptr() },
            BLOCK_SIZE as u32,
        )
        .offset(loc as u64)
        .build();

        // Submit read operation
        let mut sq = self.ring.submission();
        let _read_e = unsafe { sq.push(&read_op).expect("submission queue is full") };
        drop(sq);

        // Wait for completion
        self.ring.submit_and_wait(1)?;

        // Get completion result
        let mut cq = self.ring.completion();
        let ret = cq.next().expect("completion queue is empty");

        if ret.result() < 0 {
            return Err(io::Error::from_raw_os_error(-ret.result()));
        }

        if ret.result() as usize != BLOCK_SIZE {
            return Err(io::Error::new(io::ErrorKind::Other, "short read"));
        }

        Ok(block)
    }

    fn read_many(&mut self, blocks: &[u64]) -> Result<Vec<Result<Block>>> {
        let mut reader = AsyncReader::new(self.get_fd(), &mut self.small_pool, &mut self.ring)?;
        let mut handler = BlockReadHandler::new(blocks);

        reader.stream_blocks(&mut blocks.iter().cloned(), &mut handler)?;
        Ok(handler.get_results())
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
        &mut self,
        io_block_pool: &mut BufferPool,
        blocks: &mut dyn Iterator<Item = u64>,
        handler: &mut dyn ReadHandler,
    ) -> io::Result<()> {
        let mut reader = AsyncReader::new(self.get_fd(), io_block_pool, &mut self.ring)?;
        reader.stream_blocks(blocks, handler)
    }

    fn write(&mut self, block: &Block) -> Result<()> {
        let loc = block.loc * BLOCK_SIZE as u64;

        // Prepare write operation
        let write_op = opcode::Write::new(
            types::Fd(self.get_fd()),
            unsafe { block.get_raw_ptr() },
            BLOCK_SIZE as u32,
        )
        .offset(loc as u64)
        .build();

        // Submit write operation
        let mut sq = self.ring.submission();
        unsafe {
            sq.push(&write_op).expect("submission queue is full");
        }
        drop(sq);

        // Wait for completion
        self.ring.submit_and_wait(1)?;

        // Get completion result
        let mut cq = self.ring.completion();
        let ret = cq.next().expect("completion queue is empty");

        if ret.result() < 0 {
            return Err(io::Error::from_raw_os_error(-ret.result()));
        }

        if ret.result() as usize != BLOCK_SIZE {
            return Err(io::Error::new(io::ErrorKind::Other, "short write"));
        }

        Ok(())
    }

    fn write_many(&mut self, blocks: &[Block]) -> Result<Vec<Result<()>>> {
        /*
        let mut results = Vec::with_capacity(blocks.len());
        let mut ops = Vec::with_capacity(blocks.len());

        // Prepare operations
        for (_i, block) in blocks.iter().enumerate() {
            let loc = block.loc * BLOCK_SIZE as u64;

            let write_op = opcode::Write::new(
                types::Fd(self.get_fd()),
                unsafe { block.get_raw_ptr() },
                BLOCK_SIZE as u32,
            )
            .offset(loc as u64)
            // .user_data(i as u64)
            .build();

            ops.push(write_op);
        }

        unsafe {
            // Submit all operations
            let mut sq = self.ring.submission();
            for op in ops {
                sq.push(&op).expect("submission queue is full");
            }
            drop(sq);

            // Wait for all completions
            self.ring.submit_and_wait(blocks.len() as usize)?;

            // Process completions
            let mut cq = self.ring.completion();
            for _ in 0..blocks.len() {
                let ret = cq.next().expect("completion queue is empty");

                if ret.result() < 0 {
                    results.push(Err(io::Error::from_raw_os_error(-ret.result())));
                } else if ret.result() as usize != BLOCK_SIZE {
                    results.push(Err(io::Error::new(io::ErrorKind::Other, "short write")));
                } else {
                    results.push(Ok(()));
                }
            }
        }

        Ok(results)
        */

        todo!();
    }
}

//------------------------------------------

pub struct AsyncIoEngine {
    inner: Mutex<AsyncIoEngineInner>,
}

impl AsyncIoEngine {
    pub fn new_with<P: AsRef<Path>>(path: P, writable: bool, excl: bool) -> Result<Self> {
        let inner = Mutex::new(AsyncIoEngineInner::new_with(path, writable, excl)?);
        Ok(Self { inner })
    }

    pub fn new<P: AsRef<Path>>(path: P, writable: bool) -> Result<Self> {
        let inner = Mutex::new(AsyncIoEngineInner::new_with(path, writable, true)?);
        Ok(Self { inner })
    }
}

impl IoEngine for AsyncIoEngine {
    fn get_nr_blocks(&self) -> u64 {
        let inner = self.inner.lock().unwrap();
        inner.nr_blocks
    }

    fn get_batch_size(&self) -> usize {
        QUEUE_DEPTH as usize
    }

    fn read(&self, b: u64) -> Result<Block> {
        let mut inner = self.inner.lock().unwrap();
        inner.read(b)
    }

    fn read_many(&self, blocks: &[u64]) -> Result<Vec<Result<Block>>> {
        let mut inner = self.inner.lock().unwrap();
        inner.read_many(blocks)
    }

    fn write(&self, block: &Block) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.write(block)
    }

    fn write_many(&self, blocks: &[Block]) -> Result<Vec<Result<()>>> {
        let mut inner = self.inner.lock().unwrap();
        inner.write_many(blocks)
    }

    fn read_blocks(
        &self,
        io_block_pool: &mut BufferPool,
        blocks: &mut dyn Iterator<Item = u64>,
        handler: &mut dyn ReadHandler,
    ) -> io::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.read_blocks(io_block_pool, blocks, handler)
    }
}

//------------------------------------------
