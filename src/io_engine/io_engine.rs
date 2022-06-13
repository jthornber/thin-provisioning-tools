use safemem::write_bytes;
use std::alloc::{alloc, dealloc, Layout};
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{self, Result};
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::sync::{Condvar, Mutex};

use crate::file_utils;

//------------------------------------------

pub const BLOCK_SIZE: usize = 4096;
pub const SECTOR_SHIFT: usize = 9;
const ALIGN: usize = 4096;

#[derive(Debug)]
pub struct Block {
    pub loc: u64,

    // FIXME: don't make this pub
    pub data: *mut u8,
}

impl Block {
    // Creates a new block that corresponds to the given location.  The
    // memory is not initialised.
    pub fn new(loc: u64) -> Block {
        let layout = Layout::from_size_align(BLOCK_SIZE, ALIGN).unwrap();
        let ptr = unsafe { alloc(layout) };
        assert!(!ptr.is_null(), "out of memory");
        Block { loc, data: ptr }
    }

    pub fn zeroed(loc: u64) -> Block {
        let r = Self::new(loc);
        write_bytes(r.get_data(), 0);
        r
    }

    pub fn get_data<'a>(&self) -> &'a mut [u8] {
        unsafe { std::slice::from_raw_parts_mut::<'a>(self.data, BLOCK_SIZE) }
    }

    pub fn zero(&mut self) {
        unsafe {
            std::ptr::write_bytes(self.data, 0, BLOCK_SIZE);
        }
    }
}

impl Drop for Block {
    fn drop(&mut self) {
        let layout = Layout::from_size_align(BLOCK_SIZE, ALIGN).unwrap();
        unsafe {
            dealloc(self.data, layout);
        }
    }
}

unsafe impl Send for Block {}

//------------------------------------------

pub trait IoEngine {
    fn get_nr_blocks(&self) -> u64;
    fn get_batch_size(&self) -> usize;

    fn read(&self, b: u64) -> Result<Block>;
    // The whole io could fail, or individual blocks
    fn read_many(&self, blocks: &[u64]) -> Result<Vec<Result<Block>>>;

    fn write(&self, block: &Block) -> Result<()>;
    // The whole io could fail, or individual blocks
    fn write_many(&self, blocks: &[Block]) -> Result<Vec<Result<()>>>;
}

pub fn get_nr_blocks(path: &Path) -> io::Result<u64> {
    Ok(file_utils::file_size(path)? / (BLOCK_SIZE as u64))
}

//------------------------------------------

pub struct SyncIoEngine {
    nr_blocks: u64,
    file: File,
}

impl SyncIoEngine {
    fn open_file(path: &Path, writable: bool, excl: bool) -> Result<File> {
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

    pub fn new(path: &Path, writable: bool) -> Result<SyncIoEngine> {
        SyncIoEngine::new_with(path, writable, true)
    }

    pub fn new_with(path: &Path, writable: bool, excl: bool) -> Result<SyncIoEngine> {
        let nr_blocks = get_nr_blocks(path)?; // check file mode before opening it
        let file = SyncIoEngine::open_file(path, writable, excl)?;

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


