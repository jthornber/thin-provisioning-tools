use io_uring::{opcode, types, IoUring};
use libc::iovec;
use std::alloc::{alloc, dealloc, Layout};
use std::collections::HashMap;
use std::io;
use std::os::fd::RawFd;
use std::vec::Vec;

use crate::io_engine::base::*;

//--------------------------------

const QUEUE_DEPTH: u32 = 256;
const MAX_BLOCKS_PER_READ: usize = 64;

#[derive(Clone)]
struct IOBlock {
    loc: u64,
    data: *mut u8,
}

// SAFETY: Block contains a raw pointer, but we ensure it's used safely.
unsafe impl Send for IOBlock {}

//--------------------------------

struct BufferPool {
    nr_blocks: usize,
    block_size: usize,
    buffer: *mut u8,
    blocks: Vec<IOBlock>,
}

impl BufferPool {
    fn layout(nr_blocks: usize, block_size: usize) -> Layout {
        Layout::from_size_align(block_size * nr_blocks, block_size).unwrap()
    }

    fn new(nr_blocks: usize, block_size: usize) -> Self {
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

    fn get(&mut self, loc: u64) -> Option<IOBlock> {
        let b = self.blocks.pop();
        b.map(|b| IOBlock { loc, data: b.data })
    }

    fn put(&mut self, block: IOBlock) {
        self.blocks.push(block);
    }

    fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }
}

impl Drop for BufferPool {
    fn drop(&mut self) {
        let layout = Self::layout(self.nr_blocks, self.block_size);
        unsafe { dealloc(self.buffer, layout) };
    }
}

//--------------------------------

struct IoData {
    iov: Vec<iovec>,
    blocks: Vec<IOBlock>,
}

// New StreamingRead Struct
pub struct AsyncStreamReader {
    fd: RawFd,
    block_size: usize,
    blocks: BufferPool,
    ring: IoUring,
}

unsafe impl Send for AsyncStreamReader {}
unsafe impl Sync for AsyncStreamReader {}

impl AsyncStreamReader {
    pub fn new(fd: RawFd, block_size: usize, buffer_size_mb: usize) -> io::Result<Self> {
        let buffer_size = buffer_size_mb * 1024 * 1024;
        let num_buffer_blocks = buffer_size / block_size;

        // Initialize free list
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

    fn read_io_blocks_<F>(&mut self, block_indices: &[u64], mut callback: F) -> io::Result<()>
    where
        F: FnMut(u64, Result<&[u8], io::Error>),
    {
        let blocks_to_read = block_indices.len() as u64;

        let mut blocks_read = 0;
        let mut inflight = 0;
        let mut total_bytes_read = 0;

        while blocks_read < blocks_to_read || inflight > 0 {
            // Submit as many I/O requests as possible
            while blocks_read < blocks_to_read && inflight < QUEUE_DEPTH && !self.blocks.is_empty()
            {
                let start_block = block_indices[blocks_read as usize];
                let offset = start_block * self.block_size as u64;
                let mut blocks_this_read = 0;
                let mut io_data = IoData {
                    iov: Vec::with_capacity(MAX_BLOCKS_PER_READ),
                    blocks: Vec::with_capacity(MAX_BLOCKS_PER_READ),
                };

                while blocks_read + blocks_this_read < blocks_to_read
                    && blocks_this_read < MAX_BLOCKS_PER_READ as u64
                    && block_indices[(blocks_read + blocks_this_read) as usize]
                        == start_block + blocks_this_read as u64
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

                let io_data = Box::new(io_data);
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

                blocks_read += blocks_this_read;
                inflight += 1;
            }

            // Submit and wait for completions
            self.ring.submit_and_wait(1)?;

            // Process completions
            self.ring.completion().for_each(|cqe| {
                let mut io_data = unsafe { Box::from_raw(cqe.user_data() as *mut IoData) };
                if cqe.result() < 0 {
                    for b in &io_data.blocks {
                        callback(b.loc, Err(io::Error::from_raw_os_error(-cqe.result())));
                    }
                } else {
                    total_bytes_read += cqe.result() as u64;

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

                inflight = inflight.checked_sub(1).unwrap_or(0);
            });
        }

        Ok(())
    }
}

impl StreamReader for AsyncStreamReader {
    /// Utility function to read smaller blocks using larger IO blocks.  Indices must be sorted into
    /// ascending order.
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

        // Collect unique IO block indices
        let mut io_indices: Vec<u64> = io_block_map.keys().cloned().collect();
        io_indices.sort_unstable();

        // Read IO blocks
        self.read_io_blocks_(&io_indices, |io_idx, result| {
            match result {
                Ok(data) => {
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
                            let offset = (tz as usize) * BLOCK_SIZE;
                            let small_data = &data[offset..offset + BLOCK_SIZE];
                            handler.handle(small_idx, Ok(small_data));
                        }
                    }
                }
                Err(_e) => {
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
                            handler.handle(small_idx, Err(io::Error::last_os_error()));
                        }
                    }
                }
            }
        })?;

        handler.complete();
        Ok(())
    }
}

//--------------------------------
