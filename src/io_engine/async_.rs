use io_uring::{opcode, types};
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{self, Result};
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::vec::Vec;

use crate::io_engine::ring_pool::*;
use crate::io_engine::*;

//--------------------------------

/// Number of rings each engine creates.  Should be related to the
/// number of threads that will be concurrently using the engine.
const NR_RINGS: usize = 16;
// FIXME: reduce this if it makes no difference to performance.
const QUEUE_DEPTH: u32 = 256;

//--------------------------------

// A couple of utility functions to make it easier to construct io errors
fn io_err<T>(msg: &str) -> io::Result<T> {
    Err(io::Error::other(msg))
}

fn to_io_result<T, E>(r: std::result::Result<T, E>, msg: &str) -> io::Result<T> {
    r.map_err(|_| io::Error::other(msg))
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
