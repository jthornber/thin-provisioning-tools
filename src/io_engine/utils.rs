use anyhow::{anyhow, Result};
use iovec::{unix, IoVec};
use std::collections::HashMap;
use std::os::unix::fs::FileExt;

use crate::io_engine::{ReadHandler, VectoredIo};

#[cfg(test)]
mod tests;

//-------------------------------------

/// Trait for types that can perform block-aligned reads from a device.
pub trait ReadBlocks {
    /// Reads multiple blocks of data from the device starting at the specified position.
    ///
    /// # Arguments
    /// * `buffers` - Slice of mutable buffers to read into. All buffers must be the same size.
    /// * `pos` - Starting position in bytes where the read should begin.
    ///
    /// # Returns
    /// A vector of results, one for each buffer. Each result indicates whether the read for that
    /// buffer succeeded or failed.
    fn read_blocks(&self, buffers: &mut [&mut [u8]], pos: u64) -> Result<Vec<Result<()>>>;
}

/// Trait for types that can perform block-aligned writes to a device.
pub trait WriteBlocks {
    /// Writes multiple blocks of data to the device starting at the specified position.
    ///
    /// # Arguments
    /// * `buffers` - Slice of buffers containing data to write. All buffers must be the same size.
    /// * `pos` - Starting position in bytes where the write should begin.
    ///
    /// # Returns
    /// A vector of results, one for each buffer. Each result indicates whether the write for that
    /// buffer succeeded or failed.
    fn write_blocks(&self, buffers: &[&[u8]], pos: u64) -> Result<Vec<Result<()>>>;
}

//-------------------------------------

/// A block I/O implementation that uses vectored I/O operations for improved performance.
///
/// This struct wraps any type that implements the `VectoredIo` trait and provides block-aligned
/// read and write operations. It attempts to perform I/O operations on multiple blocks at once
/// using vectored I/O, falling back to processing individual blocks on failure.
pub struct VectoredBlockIo<T> {
    dev: T,
    partial_io: bool,
}

impl<T: VectoredIo> VectoredBlockIo<T> {
    pub fn with_partial(dev: T) -> VectoredBlockIo<T> {
        VectoredBlockIo {
            dev,
            partial_io: true,
        }
    }
}

impl<T: VectoredIo> From<T> for VectoredBlockIo<T> {
    fn from(dev: T) -> VectoredBlockIo<T> {
        VectoredBlockIo {
            dev,
            partial_io: false,
        }
    }
}

impl<T: VectoredIo> ReadBlocks for VectoredBlockIo<T> {
    // If io fails, the first block will be marked as errored,
    // and the io will be retried from the subsequent block.
    fn read_blocks(&self, buffers: &mut [&mut [u8]], mut pos: u64) -> Result<Vec<Result<()>>> {
        let block_size = buffers[0].len();
        let mut remaining = 0;
        let mut bufs: Vec<&mut IoVec> = Vec::with_capacity(buffers.len());
        for b in buffers.iter_mut() {
            assert_eq!(b.len(), block_size);
            remaining += b.len();
            bufs.push((*b).into());
        }
        let mut os_bufs = unix::as_os_slice_mut(&mut bufs[..]);
        let mut results = Vec::with_capacity(os_bufs.len());

        // Track partial buffers that need zero-filling:
        // (buffer_index, start_offset)
        let mut partial_bufs = Vec::new();
        let mut buffer_idx = 0;

        while remaining > 0 {
            match self.dev.read_vectored_at(os_bufs, pos) {
                Ok(0) => {
                    for _ in 0..os_bufs.len() {
                        results.push(Err(anyhow!("EOF")));
                    }
                    remaining = 0;
                }
                Ok(n) => {
                    let blocks_completed = n / block_size;
                    let partial_bytes = n % block_size;
                    let blocks_to_skip = n.div_ceil(block_size);

                    // Skip to the next iovec: for partial reads this skips to next iovec boundary,
                    // for complete reads this continues normally
                    remaining -= blocks_to_skip * block_size;
                    pos += blocks_to_skip as u64 * block_size as u64;
                    os_bufs = &mut os_bufs[blocks_to_skip..];

                    // Save the return status (complete + partial if any)
                    for _ in 0..blocks_completed {
                        results.push(Ok(()));
                    }
                    if partial_bytes > 0 {
                        if self.partial_io {
                            results.push(Ok(()));
                        } else {
                            results.push(Err(anyhow!("incomplete read")));
                        }

                        // Mark the partial block for zero-filling later
                        partial_bufs.push((buffer_idx + blocks_completed, partial_bytes));
                    }

                    buffer_idx += blocks_to_skip;
                }
                Err(_) => {
                    // Skip to the next iovec
                    remaining -= block_size;
                    pos += block_size as u64;
                    os_bufs = &mut os_bufs[1..];
                    results.push(Err(anyhow!("read failed")));
                    buffer_idx += 1;
                }
            }
        }

        // Zero-fill the partial buffers
        for (buf_idx, offset) in partial_bufs {
            buffers[buf_idx][offset..].fill(0);
        }

        Ok(results)
    }
}

impl<T: VectoredIo> WriteBlocks for VectoredBlockIo<T> {
    fn write_blocks(&self, buffers: &[&[u8]], mut pos: u64) -> Result<Vec<Result<()>>> {
        let block_size = buffers[0].len();
        let mut remaining = 0;
        let mut bufs: Vec<&IoVec> = Vec::with_capacity(buffers.len());
        for b in buffers.iter() {
            assert_eq!(b.len(), block_size);
            remaining += b.len();
            bufs.push((*b).into());
        }
        let mut os_bufs = unix::as_os_slice(&bufs[..]);
        let mut results = Vec::with_capacity(os_bufs.len());

        while remaining > 0 {
            if let Ok(n) = self.dev.write_vectored_at(os_bufs, pos) {
                remaining -= n;
                pos += n as u64;
                assert_eq!(n % block_size, 0);
                os_bufs = &os_bufs[(n / block_size)..];
                for _ in 0..(n / block_size) {
                    results.push(Ok(()));
                }
            } else {
                // Skip to the next iovec
                remaining -= block_size;
                pos += block_size as u64;
                os_bufs = &os_bufs[1..];
                results.push(Err(anyhow!("read failed")));
            }
        }

        Ok(results)
    }
}

//-------------------------------------

/// A simple block I/O implementation that performs individual block operations.
///
/// This struct wraps any type that implements the `FileExt` trait and provides block-aligned
/// read and write operations. It processes each block individually using standard file I/O
/// operations.
pub struct SimpleBlockIo<T> {
    dev: T,
}

impl<T: FileExt> From<T> for SimpleBlockIo<T> {
    fn from(dev: T) -> SimpleBlockIo<T> {
        SimpleBlockIo { dev }
    }
}

impl<T: FileExt> ReadBlocks for SimpleBlockIo<T> {
    fn read_blocks(&self, buffers: &mut [&mut [u8]], mut pos: u64) -> Result<Vec<Result<()>>> {
        let mut results = Vec::with_capacity(buffers.len());

        for buf in buffers.iter_mut() {
            let r = self
                .dev
                .read_exact_at(buf, pos)
                .map_err(|_| anyhow!("read failed"));
            results.push(r);
            pos += buf.len() as u64;
        }

        Ok(results)
    }
}

impl<T: FileExt> WriteBlocks for SimpleBlockIo<T> {
    fn write_blocks(&self, buffers: &[&[u8]], mut pos: u64) -> Result<Vec<Result<()>>> {
        let mut results = Vec::with_capacity(buffers.len());

        for buf in buffers {
            let r = self
                .dev
                .write_all_at(buf, pos)
                .map_err(|_| anyhow!("write failed"));
            results.push(r);
            pos += buf.len() as u64;
        }

        Ok(results)
    }
}

//-------------------------------------

/// Maps smaller logical blocks to larger IO blocks for efficient reading.
///
/// # Arguments
/// * `blocks` - Iterator over logical block numbers
/// * `blocks_per_io` - Number of logical blocks that fit in one IO block
///
/// # Returns
/// * `HashMap<u64, u64>` - Mapping from logical block numbers to IO block numbers
pub fn map_small_blocks_to_io(
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
pub fn process_io_block_result(
    io_idx: u64,
    result: std::io::Result<&[u8]>,
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
                    let offset = (tz as usize) * crate::io_engine::BLOCK_SIZE;
                    let small_data = &data[offset..offset + crate::io_engine::BLOCK_SIZE];
                    handler.handle(small_idx, Ok(small_data));
                }
                Err(e) => {
                    let block_error = clone_error(e);
                    handler.handle(small_idx, Err(block_error));
                }
            }
        }
    }
}

fn clone_error(origin: &std::io::Error) -> std::io::Error {
    use std::error::Error;
    let mut message = origin.to_string();
    let mut src = origin.source();
    while let Some(err) = src {
        message.push_str(&err.to_string());
        src = err.source();
    }
    std::io::Error::new(origin.kind(), message)
}

//-------------------------------------
