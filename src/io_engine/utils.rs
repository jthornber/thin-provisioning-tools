use anyhow::{anyhow, Result};
use iovec::{unix, IoVec};
use std::os::unix::fs::FileExt;

use crate::io_engine::VectoredIo;

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
}

impl<T: VectoredIo> From<T> for VectoredBlockIo<T> {
    fn from(dev: T) -> VectoredBlockIo<T> {
        VectoredBlockIo { dev }
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

        while remaining > 0 {
            match self.dev.read_vectored_at(os_bufs, pos) {
                Ok(n) => {
                    remaining -= n;
                    pos += n as u64;
                    assert_eq!(n % block_size, 0);
                    os_bufs = &mut os_bufs[(n / block_size)..];
                    for _ in 0..(n / block_size) {
                        results.push(Ok(()));
                    }
                }
                Err(_) => {
                    // Skip to the next iovec
                    remaining -= block_size;
                    pos += block_size as u64;
                    os_bufs = &mut os_bufs[1..];
                    results.push(Err(anyhow!("read failed")));
                }
            }
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
