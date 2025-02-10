use std::fs::File;
use std::io;
use std::os::fd::{FromRawFd, RawFd};
use std::os::unix::fs::FileExt;
use std::vec::Vec;

use super::base::StreamReader;
use super::stream_reader_common::{
    map_small_blocks_to_io, process_io_block_result,
    MIN_BLOCK_SIZE, MAX_BLOCK_SIZE, MIN_BUFFER_SIZE_MB, MAX_BUFFER_SIZE_MB,
};
use super::utils::{ReadBlocks, VectoredBlockIo};
use crate::io_engine::buffer_pool::{BufferPool, IOBlock};
use crate::io_engine::*;

//--------------------------------

const MAX_BLOCKS_PER_READ: usize = 64;

pub struct SyncStreamReader {
    block_size: usize,
    blocks: BufferPool,
    reader: VectoredBlockIo<File>,
}

unsafe impl Send for SyncStreamReader {}
unsafe impl Sync for SyncStreamReader {}

impl SyncStreamReader {
    pub fn new(fd: RawFd, block_size: usize, buffer_size_mb: usize) -> io::Result<Self> {
        // Validate input parameters
        if !(MIN_BLOCK_SIZE..=MAX_BLOCK_SIZE).contains(&block_size) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("block_size must be between {} and {} bytes", MIN_BLOCK_SIZE, MAX_BLOCK_SIZE)
            ));
        }
        if !(MIN_BUFFER_SIZE_MB..=MAX_BUFFER_SIZE_MB).contains(&buffer_size_mb) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("buffer_size_mb must be between {} and {} MB", MIN_BUFFER_SIZE_MB, MAX_BUFFER_SIZE_MB)
            ));
        }

        let buffer_size = buffer_size_mb * 1024 * 1024;
        let num_buffer_blocks = buffer_size / block_size;

        // Initialize buffer pool
        let blocks = BufferPool::new(num_buffer_blocks as usize, block_size);

        // Convert RawFd to File and initialize vectored block io
        // SAFETY: We're taking ownership of the fd, and it will be valid for the lifetime of SyncStreamReader
        let file = unsafe { File::from_raw_fd(fd) };
        let reader = VectoredBlockIo::from(file);

        Ok(SyncStreamReader {
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
    fn read_blocks(
        &mut self,
        blocks: &mut dyn Iterator<Item = u64>,
        handler: &mut dyn ReadHandler,
    ) -> io::Result<()> {
        let blocks_per_io = self.block_size / super::base::BLOCK_SIZE;
        let io_block_map = map_small_blocks_to_io(blocks, blocks_per_io);

        if io_block_map.is_empty() {
            return Ok(());
        }

        // Get unique IO blocks we need to read
        let mut io_blocks: Vec<_> = io_block_map.values().copied().collect();
        io_blocks.sort_unstable();
        io_blocks.dedup();

        // Read blocks in chunks to avoid excessive memory usage
        for chunk in io_blocks.chunks(MAX_BLOCKS_PER_READ) {
            self.read_io_blocks_(chunk, |data, io_idx| {
                process_io_block_result(
                    io_idx,
                    Ok(data),
                    blocks_per_io,
                    &io_block_map,
                    handler,
                );
                Ok(())
            })?;
        }

        Ok(())
    }
}
