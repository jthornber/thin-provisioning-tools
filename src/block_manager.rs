use anyhow::{anyhow, Result};
use rio::{self, Completion, Rio};
use std::alloc::{alloc, dealloc, Layout};
use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::{Read, Seek};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::sync::{Arc, Mutex};

pub const BLOCK_SIZE: usize = 4096;
const ALIGN: usize = 4096;

// FIXME: introduce a cache
// FIXME: use O_DIRECT
#[derive(Debug)]
pub struct Block {
    pub loc: u64,
    data: *mut u8,
}

impl Block {
    pub fn new(loc: u64) -> Block {
        let layout = Layout::from_size_align(BLOCK_SIZE, ALIGN).unwrap();
        let ptr = unsafe { alloc(layout) };
        assert!(!ptr.is_null(), "out of memory");
        Block { loc, data: ptr }
    }

    fn get_data(&self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut::<'static>(self.data, BLOCK_SIZE) }
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

//------------------------------------------

pub trait IoEngine {
    fn get_nr_blocks(&self) -> u64;
    fn read(&mut self, blocks: &mut Vec<Block>) -> Result<()>;
}

fn get_nr_blocks(path: &Path) -> io::Result<u64> {
    let metadata = std::fs::metadata(path)?;
    Ok(metadata.len() / (BLOCK_SIZE as u64))
}

//------------------------------------------

pub struct SyncIoEngine {
    nr_blocks: u64,
    input: File,
}

impl SyncIoEngine {
    pub fn new(path: &Path) -> Result<SyncIoEngine> {
        let input = OpenOptions::new()
            .read(true)
            .write(false)
            .custom_flags(libc::O_DIRECT)
            .open(path)?;

        let ring = rio::new()?;

        Ok(SyncIoEngine {
            nr_blocks: get_nr_blocks(path)?,
            input,
        })
    }
}

impl IoEngine for SyncIoEngine {
    fn get_nr_blocks(&self) -> u64 {
        self.nr_blocks
    }

    fn read(&mut self, blocks: &mut Vec<Block>) -> Result<()> {
        for b in blocks.into_iter() {
            self.input.seek(io::SeekFrom::Start(0))?;
            self.input.read_exact(&mut b.get_data())?;
        }

        Ok(())
    }
}

//------------------------------------------

/*
pub struct AsyncIoEngine {
    ring: Rio,
    nr_blocks: u64,
    input: File,
}

impl AsyncIoEngine {
    pub fn new(path: &Path) -> Result<IoEngine> {
        let input = OpenOptions::new()
            .read(true)
            .write(false)
            .custom_flags(libc::O_DIRECT)
            .open(path)?;

        let ring = rio::new()?;

        Ok(IoEngine {
            ring,
            nr_blocks: get_nr_blocks(path)?,
            input,
        })
    }

    pub fn read(&self, blocks: &mut Vec<Block>) -> Result<()> {
        // FIXME: using a bounce buffer as a hack, since b.get_data() will not have
        // a big enough lifetime.
        let mut bounce_buffer = vec![0; blocks.len() * BLOCK_SIZE];
        let mut completions = Vec::new();

        for n in 0..blocks.len() {
            let b = &blocks[n];
            let at = b.loc * BLOCK_SIZE as u64;
            let completion = self.ring.read_at(&self.input, &slice, at);
            completions.push(completion);
        }

        for c in completions {
            let n = c.wait()?;
            if n != BLOCK_SIZE {
                return Err(anyhow!("short read"));
            }
        }

        // copy out of the bounce buffer

        Ok(())
    }
}
*/
