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
        let mut bs = blocks
            .iter()
            .map(|loc| Some(Block::new(*loc)))
            .collect::<Vec<Option<Block>>>();

        // sort by location
        // bs.sort_by(|lhs, rhs| lhs.loc.cmp(&rhs.loc));

        // Split into runs of adjacent blocks
        let mut runs: Vec<usize> = Vec::with_capacity(blocks.len());
        let mut last: Option<(u64, u64)> = None;
        for b in &bs {
            let b = b.as_ref().unwrap();
            if let Some((begin, end)) = last {
                if end != b.loc || end - begin >= libc::UIO_MAXIOV as u64 {
                    runs.push((end - begin) as usize);
                    last = Some((b.loc, b.loc + 1));
                } else {
                    last = Some((begin, b.loc + 1));
                }
            } else {
                last = Some((b.loc, b.loc + 1));
            }
        }

        if let Some((begin, end)) = last {
            runs.push((end - begin) as usize);
        }

        // Issue ios
        let vio: VectoredBlockIo<&File> = input.into();
        let mut issued = 0;
        let mut run_results = Vec::with_capacity(runs.len());
        for run_len in &runs {
            assert!(*run_len > 0);
            let mut buffers = Vec::with_capacity(*run_len);
            for i in 0..*run_len {
                buffers.push(bs[i + issued].as_ref().unwrap().get_data());
            }
            run_results.push(vio.read_blocks(
                &mut buffers[..],
                bs[issued].as_ref().unwrap().loc * BLOCK_SIZE as u64,
            ));
            issued += run_len;
        }

        let mut results = Vec::with_capacity(bs.len());
        let mut index = 0;
        for (run_len, r) in runs.iter().zip(run_results) {
            match r {
                Err(_) => {
                    for _ in 0..*run_len {
                        results.push(Self::bad_read());
                    }
                    index += *run_len;
                }
                Ok(rs) => {
                    for r in rs {
                        match r {
                            Err(_) => {
                                results.push(Self::bad_read());
                            }
                            Ok(()) => {
                                results.push(Ok(bs[index].take().unwrap()));
                            }
                        }
                        index += 1;
                    }
                }
            }
        }

        // unsort
        // FIXME: finish

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
