use std::fs::File;
use std::fs::OpenOptions;
use std::io::{self, Result};
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::Path;

use crate::io_engine::gaps::*;
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

        let vio: VectoredBlockIo<&File> = input.into();
        let mut issued_minus_gaps = 0;

        // The same junk buffer is inserted for all the gaps
        let gap_buffer = Block::new(0);
        let mut results: Vec<Result<Block>> = Vec::with_capacity(bs.len());
        let mut bs_index = 0;

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
                        if first.is_none() {
                            first = Some(*b);
                        }
                        for _ in *b..*e {
                            buffers.push(gap_buffer.get_data());
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
