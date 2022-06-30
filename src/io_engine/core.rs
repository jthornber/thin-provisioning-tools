use std::alloc::{alloc, dealloc, Layout};
use std::io;
use std::vec::*;

use crate::io_engine::{Block, IoEngine, BLOCK_SIZE};

//------------------------------------------

const ALIGN: usize = 4096;

pub struct CoreIoEngine {
    nr_blocks: u64,
    data: *mut u8,
}

impl CoreIoEngine {
    pub fn new(nr_blocks: u64) -> CoreIoEngine {
        assert!(nr_blocks <= usize::MAX as u64);
        let capacity = BLOCK_SIZE * nr_blocks as usize;
        let layout = Layout::from_size_align(capacity, ALIGN).unwrap();
        let ptr = unsafe { alloc(layout) };
        assert!(!ptr.is_null(), "out of memory");
        CoreIoEngine {
            nr_blocks,
            data: ptr,
        }
    }
}

impl Drop for CoreIoEngine {
    fn drop(&mut self) {
        let capacity = BLOCK_SIZE * self.nr_blocks as usize;
        let layout = Layout::from_size_align(capacity, ALIGN).unwrap();
        unsafe {
            dealloc(self.data, layout);
        }
    }
}

unsafe impl Send for CoreIoEngine {}
unsafe impl Sync for CoreIoEngine {}

impl IoEngine for CoreIoEngine {
    fn get_nr_blocks(&self) -> u64 {
        self.nr_blocks
    }

    fn get_batch_size(&self) -> usize {
        1
    }

    fn suggest_nr_threads(&self) -> usize {
        1
    }

    fn read(&self, b: u64) -> io::Result<Block> {
        if b >= self.nr_blocks {
            return Err(io::Error::from(io::ErrorKind::InvalidInput));
        }
        let block = Block::new(b);
        unsafe {
            let off = b as isize * BLOCK_SIZE as isize;
            std::ptr::copy(
                self.data.offset(off),
                block.get_data().as_mut_ptr(),
                BLOCK_SIZE,
            );
        }
        Ok(block)
    }

    fn read_many(&self, blocks: &[u64]) -> io::Result<Vec<io::Result<Block>>> {
        let mut bs = Vec::new();
        for b in blocks {
            bs.push(self.read(*b));
        }
        Ok(bs)
    }

    fn write(&self, block: &Block) -> io::Result<()> {
        if block.loc >= self.nr_blocks {
            return Err(io::Error::from(io::ErrorKind::InvalidInput));
        }
        unsafe {
            let off = block.loc as isize * BLOCK_SIZE as isize;
            std::ptr::copy(block.get_data().as_ptr(), self.data.offset(off), BLOCK_SIZE);
        }
        Ok(())
    }

    fn write_many(&self, blocks: &[Block]) -> io::Result<Vec<io::Result<()>>> {
        let mut ret = Vec::new();
        for b in blocks {
            ret.push(self.write(b));
        }
        Ok(ret)
    }
}

//------------------------------------------

pub fn trash_block(engine: &dyn IoEngine, b: u64) {
    let block = Block::zeroed(b);
    assert!(engine.write(&block).is_ok());
}

//------------------------------------------
