use std::collections::BTreeSet;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{self, Result};
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::Path;

use crate::io_engine::utils::*;
use crate::io_engine::*;

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
        Err(io::Error::new(io::ErrorKind::Other, "read failed"))
    }

    fn read_many_(input: &File, blocks: &[u64]) -> Result<Vec<Result<Block>>> {
        const GAP_THRESHOLD: u64 = 8;

        let mut bs = blocks
            .iter()
            .map(|loc| Some(Block::new(*loc)))
            .collect::<Vec<Option<Block>>>();

        // Split into runs of adjacent blocks
        let mut runs: Vec<(u64, u64)> = Vec::with_capacity(16);
        let mut last: Option<(u64, u64)> = None;
        let mut gap_blocks = BTreeSet::new();
        for b in &bs {
            let b = b.as_ref().unwrap();
            if let Some((begin, end)) = last {
                if b.loc >= end {
                    let len = b.loc - end;
                    if (len > GAP_THRESHOLD) || ((end - begin) >= libc::UIO_MAXIOV as u64) {
                        runs.push((begin, end));
                        last = Some((b.loc, b.loc + 1));
                    } else if len == 0 {
                        last = Some((begin, b.loc + 1));
                    } else {
                        for g in end..b.loc {
                            gap_blocks.insert(g);
                        }
                        last = Some((begin, b.loc + 1));
                    }
                } else {
                    runs.push((begin, end));
                    last = Some((b.loc, b.loc + 1));
                }
            } else {
                last = Some((b.loc, b.loc + 1));
            }
        }

        if let Some((begin, end)) = last {
            runs.push((begin, end));
        }

        // Issue ios
        let vio: VectoredBlockIo<&File> = input.into();
        let mut issued_minus_gaps = 0;
        let mut run_results = Vec::with_capacity(runs.len());
        let gap_buffer = Block::new(0);
        for (b, e) in &runs {
            let run_len = (*e - *b) as usize;
            let mut buffers = Vec::with_capacity(run_len);
            for i in 0..run_len {
                if gap_blocks.contains(&(b + i as u64)) {
                    buffers.push(gap_buffer.get_data());
                } else {
                    assert_eq!(b + i as u64, bs[issued_minus_gaps].as_ref().unwrap().loc);
                    buffers.push(bs[issued_minus_gaps].as_ref().unwrap().get_data());
                    issued_minus_gaps += 1;
                }
            }
            run_results.push(vio.read_blocks(&mut buffers[..], b * BLOCK_SIZE as u64));
        }

        let mut results = Vec::with_capacity(bs.len());
        let mut index = 0;
        for ((b, e), r) in runs.iter().zip(run_results) {
            match r {
                Err(_) => {
                    for i in *b..*e {
                        if !gap_blocks.contains(&i) {
                            results.push(Self::bad_read());
                            index += 1;
                        }
                    }
                }
                Ok(rs) => {
                    for (i, r) in rs.iter().enumerate() {
                        if !gap_blocks.contains(&(b + i as u64)) {
                            match r {
                                Err(_) => {
                                    results.push(Self::bad_read());
                                }
                                Ok(()) => {
                                    assert_eq!(b + i as u64, bs[index].as_ref().unwrap().loc);
                                    results.push(Ok(bs[index].take().unwrap()));
                                }
                            }
                            index += 1;
                        }
                    }
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
        1
    }

    fn suggest_nr_threads(&self) -> usize {
        std::cmp::min(8, num_cpus::get())
    }

    fn read(&self, loc: u64) -> Result<Block> {
        let b = Block::new(loc);
        self.file
            .read_exact_at(b.get_data(), b.loc * BLOCK_SIZE as u64)?;
        Ok(b)
    }

    fn read_many(&self, blocks: &[u64]) -> Result<Vec<Result<Block>>> {
        Self::read_many_(&self.file, blocks)
    }

    fn write(&self, b: &Block) -> Result<()> {
        self.file
            .write_all_at(b.get_data(), b.loc * BLOCK_SIZE as u64)?;
        Ok(())
    }

    fn write_many(&self, blocks: &[Block]) -> Result<Vec<Result<()>>> {
        let mut bs = Vec::new();
        for b in blocks {
            bs.push(self.write(b));
        }
        Ok(bs)
    }
}

//------------------------------------------
