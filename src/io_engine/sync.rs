use std::fs::File;
use std::fs::OpenOptions;
use std::io::Result;
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::Path;

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
        let mut bs = Vec::new();
        for b in blocks {
            bs.push(self.read(*b));
        }
        Ok(bs)
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
