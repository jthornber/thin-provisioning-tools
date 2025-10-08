use std::fs::File;
use std::fs::OpenOptions;
use std::io::{self, Result};
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::Path;

use crate::io_engine::gaps::*;
use crate::io_engine::utils::*;
use crate::io_engine::*;

#[cfg(test)]
mod tests;

//------------------------------------------

/// Minimum block size in bytes (4KB)
const MIN_BLOCK_SIZE: usize = 4 * 1024;
/// Maximum block size in bytes (16MB)
const MAX_BLOCK_SIZE: usize = 16 * 1024 * 1024;
/// Maximum number of blocks per IO
const MAX_IO_BLOCKS: usize = 64;

//------------------------------------------

struct SyncReader<'a> {
    reader: VectoredBlockIo<&'a File>,
    io_blocks: &'a mut BufferPool,
}

impl<'a> SyncReader<'a> {
    pub fn new(file: &'a File, io_blocks: &'a mut BufferPool) -> io::Result<Self> {
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

        let reader = VectoredBlockIo::with_partial(file);

        Ok(Self { reader, io_blocks })
    }

    fn read_io_blocks_<F>(&mut self, block_indices: &[u64], mut callback: F) -> io::Result<()>
    where
        F: FnMut(u64, io::Result<&[u8]>) -> io::Result<()>,
    {
        let block_size = self.io_blocks.get_block_size();
        let mut bufs = Vec::with_capacity(block_indices.len());
        let mut io_blocks = Vec::with_capacity(block_indices.len());

        // Get blocks from the pool and prepare buffers
        for &index in block_indices {
            if let Some(block) = self.io_blocks.get(index) {
                io_blocks.push(block.clone());
                // SAFETY: The buffer is valid for the lifetime of the IOBlock
                let buf = unsafe { std::slice::from_raw_parts_mut(block.data, block_size) };
                bufs.push(buf);
            } else {
                // Return any blocks we've already taken from the pool
                for block in io_blocks {
                    self.io_blocks.put(block);
                }
                return Err(io::Error::other("failed to get buffer from pool"));
            }
        }

        // Convert bufs into slice references for read_blocks
        let mut buf_refs: Vec<&mut [u8]> = bufs.iter_mut().map(|b| &mut b[..]).collect();

        // Read the blocks
        let results = self
            .reader
            .read_blocks(&mut buf_refs, block_indices[0] * block_size as u64)
            .map_err(io::Error::other)?;

        // Process results and invoke callback
        for (i, result) in results.into_iter().enumerate() {
            match result {
                Ok(_) => callback(io_blocks[i].loc, Ok(bufs[i]))?,
                Err(e) => callback(io_blocks[i].loc, Err(io::Error::other(e)))?,
            }
        }

        // Return blocks to the pool
        for block in io_blocks {
            self.io_blocks.put(block);
        }

        Ok(())
    }

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
        let blocks_per_io = io_block_size / base::BLOCK_SIZE;
        assert!(blocks_per_io <= 64); // so we can fit the bitset in a u64

        // Map small blocks to IO blocks
        let io_block_map = utils::map_small_blocks_to_io(blocks, blocks_per_io);

        if io_block_map.is_empty() {
            return Ok(());
        }

        // Get unique IO blocks we need to read
        let mut io_blocks: Vec<_> = io_block_map.keys().cloned().collect();
        io_blocks.sort_unstable();

        // Read blocks in chunks to avoid excessive memory usage
        for chunk in crate::utils::adjacent_chunks::adjacent_chunks(&io_blocks, MAX_IO_BLOCKS) {
            self.read_io_blocks_(chunk, |io_idx, result| {
                utils::process_io_block_result(
                    io_idx,
                    result,
                    blocks_per_io,
                    &io_block_map,
                    handler,
                );
                Ok(())
            })?;
        }

        handler.complete();
        Ok(())
    }
}

//------------------------------------------

pub struct SyncIoEngine {
    nr_blocks: u64,
    file: File,
}

impl SyncIoEngine {
    fn open_file<P: AsRef<Path>>(path: P, writable: bool, excl: bool) -> Result<File> {
        let file = OpenOptions::new()
            .read(true)
            .write(writable)
            .custom_flags(if excl {
                libc::O_EXCL | libc::O_DIRECT
            } else {
                libc::O_DIRECT
            })
            .open(path)?;

        Ok(file)
    }

    pub fn new<P: AsRef<Path>>(path: P, writable: bool) -> Result<Self> {
        SyncIoEngine::new_with(path, writable, true)
    }

    pub fn new_with<P: AsRef<Path>>(path: P, writable: bool, excl: bool) -> Result<Self> {
        let nr_blocks = get_nr_blocks(path.as_ref())?; // check file mode before opening it
        let file = SyncIoEngine::open_file(path.as_ref(), writable, excl)?;

        Ok(SyncIoEngine { nr_blocks, file })
    }

    fn bad_read<T>() -> Result<T> {
        Err(io::Error::other("read failed"))
    }

    fn bad_write() -> io::Error {
        io::Error::other("write failed")
    }

    fn read_many_<T: VectoredIo>(
        vio: VectoredBlockIo<T>,
        blocks: &[u64],
    ) -> Result<Vec<Result<Block>>> {
        const GAP_THRESHOLD: u64 = 8;

        if blocks.is_empty() {
            return Ok(vec![]);
        }

        // Split into runs of adjacent blocks
        let batches = generate_runs(blocks, GAP_THRESHOLD, libc::UIO_MAXIOV as u64);

        // Issue ios
        let mut bs = blocks
            .iter()
            .map(|loc| Some(Block::new(*loc)))
            .collect::<Vec<Option<Block>>>();

        let mut issued_minus_gaps = 0;

        let mut results: Vec<Result<Block>> = Vec::with_capacity(bs.len());
        let mut bs_index = 0;

        let mut gaps: Vec<Block> = Vec::with_capacity(16);

        for batch in batches {
            let mut first = None;

            // build io
            let mut buffers = Vec::with_capacity(16);
            for op in &batch {
                match op {
                    RunOp::Run(b, e) => {
                        if first.is_none() {
                            first = Some(*b);
                        }
                        for b in *b..*e {
                            assert_eq!(b, bs[issued_minus_gaps].as_ref().unwrap().loc);
                            buffers.push(bs[issued_minus_gaps].as_ref().unwrap().get_data());
                            issued_minus_gaps += 1;
                        }
                    }
                    RunOp::Gap(b, e) => {
                        // Initially I reused the same junk buffer for the gaps, since I don't intend
                        // using what is read.  But this causes issues with dm-integrity.
                        if first.is_none() {
                            first = Some(*b);
                        }
                        for loc in *b..*e {
                            let gap_buffer = Block::new(loc);
                            buffers.push(gap_buffer.get_data());
                            gaps.push(gap_buffer);
                        }
                    }
                }
            }

            assert!(first.is_some());

            // Issue io
            let run_results = vio.read_blocks(&mut buffers[..], first.unwrap() * BLOCK_SIZE as u64);

            if let Ok(run_results) = run_results {
                // select results
                let mut rindex = 0;
                for op in batch {
                    match op {
                        RunOp::Run(b, e) => {
                            for i in b..e {
                                if run_results[rindex].is_err() {
                                    results.push(Self::bad_read());
                                } else {
                                    let b = bs[bs_index].take().unwrap();
                                    assert_eq!(i, b.loc);
                                    results.push(Ok(b));
                                }
                                bs_index += 1;
                                rindex += 1;
                            }
                        }
                        RunOp::Gap(b, e) => {
                            rindex += (e - b) as usize;
                        }
                    }
                }
            } else {
                // Error everything
                for op in batch {
                    match op {
                        RunOp::Run(b, e) => {
                            for _ in b..e {
                                results.push(Self::bad_read());
                                bs_index += 1;
                            }
                        }
                        RunOp::Gap(..) => {
                            // do nothing
                        }
                    }
                }
            }
        }
        assert_eq!(results.len(), blocks.len());

        Ok(results)
    }

    fn write_many_<T: VectoredIo>(
        vio: VectoredBlockIo<T>,
        blocks: &[Block],
    ) -> Result<Vec<Result<()>>> {
        if blocks.is_empty() {
            return Ok(vec![]);
        }

        let batches = find_runs_nogap(blocks, libc::UIO_MAXIOV as usize);
        let mut issued: usize = 0;
        let mut results: Vec<Result<()>> = Vec::with_capacity(blocks.len());

        for (batch_start, batch_size) in batches {
            assert!(batch_size > 0);

            // Issue io
            let buffers: Vec<&[u8]> = blocks[issued..(issued + batch_size)]
                .iter()
                .map(|b| b.as_ref())
                .collect();
            let run_results = vio.write_blocks(&buffers, batch_start * BLOCK_SIZE as u64);
            issued += batch_size;

            if let Ok(run_results) = run_results {
                for r in run_results {
                    results.push(r.map_err(|_| Self::bad_write()));
                }
            } else {
                // Error everything
                for _ in 0..batch_size {
                    results.push(Err(Self::bad_write()));
                }
            }
        }

        Ok(results)
    }
}

impl IoEngine for SyncIoEngine {
    fn get_nr_blocks(&self) -> u64 {
        self.nr_blocks
    }

    fn get_batch_size(&self) -> usize {
        32 // could be up to libc::UIO_MAXIOV
    }

    fn read(&self, loc: u64) -> Result<Block> {
        let b = Block::new(loc);
        self.file
            .read_exact_at(b.get_data(), b.loc * BLOCK_SIZE as u64)?;
        Ok(b)
    }

    fn read_many(&self, blocks: &[u64]) -> Result<Vec<Result<Block>>> {
        Self::read_many_((&self.file).into(), blocks)
    }

    fn write(&self, b: &Block) -> Result<()> {
        self.file
            .write_all_at(b.get_data(), b.loc * BLOCK_SIZE as u64)?;
        Ok(())
    }

    fn write_many(&self, blocks: &[Block]) -> Result<Vec<Result<()>>> {
        Self::write_many_((&self.file).into(), blocks)
    }

    fn read_blocks(
        &self,
        io_block_pool: &mut BufferPool,
        blocks: &mut dyn Iterator<Item = u64>,
        handler: &mut dyn ReadHandler,
    ) -> io::Result<()> {
        let mut reader = SyncReader::new(&self.file, io_block_pool)?;
        reader.stream_blocks(blocks, handler)
    }
}

// Simplified version of generate_runs() without gaps. It should be a bit faster
// since multiple iterations are not required.
fn find_runs_nogap(blocks: &[Block], max_len: usize) -> Vec<(u64, usize)> {
    if blocks.is_empty() {
        return Vec::new();
    }

    let mut begin = blocks[0].loc;
    let mut len = 1usize;

    if blocks.len() == 1 {
        return vec![(begin, len)];
    }

    let mut runs = Vec::new();

    for b in &blocks[1..] {
        if b.loc == begin + len as u64 && len < max_len {
            len += 1;
        } else {
            runs.push((begin, len));
            begin = b.loc;
            len = 1;
        }
    }

    runs.push((begin, len));

    runs
}

//------------------------------------------
