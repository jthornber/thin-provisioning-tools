use std::fs::File;
use std::io;
use std::os::fd::{FromRawFd, RawFd};
use std::os::unix::fs::FileExt;
use std::vec::Vec;

use crate::io_engine::base::*;
use crate::io_engine::buffer_pool::{BufferPool, IOBlock};
use crate::io_engine::utils::{ReadBlocks, SimpleBlockIo};

//--------------------------------

const MAX_BLOCKS_PER_READ: usize = 64;

pub struct SyncStreamReader {
    fd: RawFd,
    block_size: usize,
    blocks: BufferPool,
    reader: SimpleBlockIo<File>,
}

unsafe impl Send for SyncStreamReader {}
unsafe impl Sync for SyncStreamReader {}

impl SyncStreamReader {
    pub fn new(fd: RawFd, block_size: usize, buffer_size_mb: usize) -> io::Result<Self> {
        let buffer_size = buffer_size_mb * 1024 * 1024;
        let num_buffer_blocks = buffer_size / block_size;

        // Initialize buffer pool
        let blocks = BufferPool::new(num_buffer_blocks as usize, block_size);

        // Convert RawFd to File and initialize simple block io
        // SAFETY: We're taking ownership of the fd, and it will be valid for the lifetime of SyncStreamReader
        let file = unsafe { File::from_raw_fd(fd) };
        let reader = SimpleBlockIo::from(file);

        Ok(SyncStreamReader {
            fd,
            block_size,
            blocks,
            reader,
        })
    }

    fn read_io_blocks_<F>(&mut self, block_indices: &[u64], mut callback: F) -> io::Result<()>
    where
        F: FnMut(&[u8], u64) -> io::Result<()>,
    {
        let mut bufs = Vec::with_capacity(block_indices.len());
        let mut io_blocks = Vec::with_capacity(block_indices.len());

        // Get blocks from the pool and prepare buffers
        for &index in block_indices {
            if let Some(block) = self.blocks.get(index) {
                // Clone for storing in io_blocks
                io_blocks.push(block.clone());
                // SAFETY: The buffer is valid for the lifetime of the IOBlock
                let buf = unsafe { std::slice::from_raw_parts_mut(block.data, self.block_size) };
                bufs.push(buf);
            } else {
                // Return any blocks we've already taken from the pool
                for block in io_blocks {
                    self.blocks.put(block);
                }
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "failed to get buffer from pool",
                ));
            }
        }

        // Convert bufs into slice references for read_blocks
        let mut buf_refs: Vec<&mut [u8]> = bufs.iter_mut().map(|b| &mut b[..]).collect();

        // Read the blocks
        let results = self
            .reader
            .read_blocks(&mut buf_refs, block_indices[0] * self.block_size as u64)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        // Process results and invoke callback
        for (i, result) in results.into_iter().enumerate() {
            if let Ok(()) = result {
                callback(&bufs[i], io_blocks[i].loc)?;
            }
        }

        // Return blocks to the pool
        for block in io_blocks {
            self.blocks.put(block);
        }

        Ok(())
    }
}

impl StreamReader for SyncStreamReader {
    /// Utility function to read smaller blocks using larger IO blocks. Indices must be sorted into
    /// ascending order.
    fn read_blocks(
        &mut self,
        blocks: &mut dyn Iterator<Item = u64>,
        handler: &mut dyn ReadHandler,
    ) -> io::Result<()> {
        let mut io_blocks = Vec::with_capacity(MAX_BLOCKS_PER_READ);
        let mut current_io_block = None;
        let mut current_io_block_index = 0;
        let block_size = self.block_size;

        for block_index in blocks {
            let io_block_index = block_index / (block_size as u64);

            if Some(io_block_index) != current_io_block {
                if !io_blocks.is_empty() {
                    self.read_io_blocks_(&io_blocks, |data, loc| {
                        handler.handle(loc * block_size as u64, Ok(data));
                        Ok(())
                    })?;
                    io_blocks.clear();
                }
                current_io_block = Some(io_block_index);
                current_io_block_index = io_block_index;
            }

            io_blocks.push(current_io_block_index);

            if io_blocks.len() == MAX_BLOCKS_PER_READ {
                self.read_io_blocks_(&io_blocks, |data, loc| {
                    handler.handle(loc * block_size as u64, Ok(data));
                    Ok(())
                })?;
                io_blocks.clear();
            }
        }

        if !io_blocks.is_empty() {
            self.read_io_blocks_(&io_blocks, |data, loc| {
                handler.handle(loc * block_size as u64, Ok(data));
                Ok(())
            })?;
        }

        handler.complete();
        Ok(())
    }
}
