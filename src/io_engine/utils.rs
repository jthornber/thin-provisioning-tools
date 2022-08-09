use anyhow::{anyhow, Result};
use iovec::{unix, IoVec};

use crate::io_engine::VectoredIo;

#[cfg(test)]
mod tests;

//-------------------------------------

pub trait ReadBlocks {
    // All the individual buffers are assumed to be the same size.
    fn read_blocks(&self, buffers: &mut [&mut [u8]], pos: u64) -> Result<Vec<Result<()>>>;
}

pub trait WriteBlocks {
    // All the individual buffers are assumed to be the same size.
    fn write_blocks(&self, buffers: &[&[u8]], pos: u64) -> Result<Vec<Result<()>>>;
}

//-------------------------------------

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
            if let Ok(n) = self.dev.read_vectored_at(os_bufs, pos) {
                remaining -= n as usize;
                pos += n as u64;
                assert_eq!(n as usize % block_size, 0);
                os_bufs = &mut os_bufs[(n as usize / block_size)..];
                for _ in 0..(n as usize / block_size) {
                    results.push(Ok(()));
                }
            } else {
                // Skip to the next iovec
                remaining -= block_size;
                pos += block_size as u64;
                os_bufs = &mut os_bufs[1..];
                results.push(Err(anyhow!("read failed")));
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
                remaining -= n as usize;
                pos += n as u64;
                assert_eq!(n as usize % block_size, 0);
                os_bufs = &os_bufs[(n as usize / block_size)..];
                for _ in 0..(n as usize / block_size) {
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
