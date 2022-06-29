use rio::*;
use std::collections::BTreeMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{self, Result};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

use crate::io_engine::*;

//------------------------------------------

// We hang waiting for completions on spindle devices if the QUEUE_DEPTH
// is larger than this.  This doesn't give me confidence in io_uring.
const QUEUE_DEPTH: usize = 256;

pub struct AsyncIoEngine {
    input: File,
    nr_blocks: u64,
    ring: Rio,
}

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

        let mut cfg = rio::Config::default();
        cfg.depth = QUEUE_DEPTH;
        cfg.io_poll = false;
        let ring = cfg.start()?;

        Ok(Self {
            input,
            nr_blocks,
            ring,
        })
    }

    pub fn new<P: AsRef<Path>>(path: P, writable: bool) -> Result<Self> {
        Self::new_with(path, writable, true)
    }
}

//------------------------------------------

impl IoEngine for AsyncIoEngine {
    fn get_nr_blocks(&self) -> u64 {
        self.nr_blocks
    }

    fn get_batch_size(&self) -> usize {
        QUEUE_DEPTH
    }

    fn suggest_nr_threads(&self) -> usize {
        std::cmp::min(2, num_cpus::get())
    }

    fn read(&self, b: u64) -> Result<Block> {
        let b = Block::new(b);

        let loc = b.loc * BLOCK_SIZE as u64;
        let completion = self.ring.read_at(&self.input, &b, loc);

        let nr_read = completion.wait()?;
        if nr_read != BLOCK_SIZE {
            return Err(io::Error::new(io::ErrorKind::Other, "short read"));
        }

        Ok(b)
    }

    fn read_many(&self, blocks: &[u64]) -> Result<Vec<Result<Block>>> {
        // eprintln!("read_many {:?}", blocks);
        let blocks: Vec<Block> = blocks.iter().map(|b| Block::new(*b)).collect();
        let mut completions = Vec::with_capacity(blocks.len());

        for (i, b) in blocks.iter().enumerate() {
            let loc = b.loc * BLOCK_SIZE as u64;
            let completion = self.ring.read_at(&self.input, b, loc);
            completions.push((i, completion));
        }

        let mut errs = BTreeMap::new();
        for (i, completion) in completions {
            // eprintln!("waiting for completion {}", i);
            match completion.wait() {
                Err(e) => {
                    errs.insert(i, e);
                }
                Ok(nr_read) => {
                    if nr_read != BLOCK_SIZE {
                        errs.insert(i, io::Error::new(io::ErrorKind::Other, "short read"));
                    }
                }
            }
        }

        let mut results = Vec::with_capacity(blocks.len());
        for (i, b) in blocks.into_iter().enumerate() {
            if let Some(e) = errs.get_mut(&i) {
                let mut err = io::Error::new(io::ErrorKind::Other, "stub");
                std::mem::swap(&mut err, e);
                results.push(Err(err));
            } else {
                results.push(Ok(b));
            }
        }

        Ok(results)
    }

    fn write(&self, b: &Block) -> Result<()> {
        let loc = b.loc * BLOCK_SIZE as u64;
        let completion = self.ring.write_at(&self.input, &b, loc);

        let nr_written = completion.wait()?;
        if nr_written != BLOCK_SIZE {
            return Err(io::Error::new(io::ErrorKind::Other, "short write"));
        }

        Ok(())
    }

    fn write_many(&self, blocks: &[Block]) -> Result<Vec<Result<()>>> {
        let mut completions = Vec::with_capacity(blocks.len());

        for (i, b) in blocks.iter().enumerate() {
            let loc = b.loc * BLOCK_SIZE as u64;
            let completion = self.ring.read_at(&self.input, b, loc);
            completions.push((i, completion));
        }

        let mut errs = BTreeMap::new();
        for (i, completion) in completions {
            match completion.wait() {
                Err(e) => {
                    errs.insert(i, e);
                }
                Ok(nr_written) => {
                    if nr_written != BLOCK_SIZE {
                        errs.insert(i, io::Error::new(io::ErrorKind::Other, "short write"));
                    }
                }
            }
        }

        let mut results = Vec::with_capacity(blocks.len());
        for i in 0..blocks.len() {
            if let Some(e) = errs.get_mut(&i) {
                let mut err = io::Error::new(io::ErrorKind::Other, "stub");
                std::mem::swap(&mut err, e);
                results.push(Err(err));
            } else {
                results.push(Ok(()));
            }
        }

        Ok(results)
    }
}

//------------------------------------------
