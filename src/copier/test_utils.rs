use anyhow::Result;
use std::os::unix::fs::FileExt;

use crate::io_engine::base::PAGE_SIZE;
use crate::io_engine::buffer::Buffer;
use crate::random::Generator;

//------------------------------------------

pub trait BlockVisitor {
    fn visit(&mut self, blocknr: u64) -> Result<()>;
}

pub fn visit_blocks(nr_blocks: u64, v: &mut dyn BlockVisitor) -> Result<()> {
    for b in 0..nr_blocks {
        v.visit(b)?;
    }
    Ok(())
}

//------------------------------------------

pub struct Stamper<T> {
    dev: T,
    block_size: usize, // bytes
    seed: u64,
    buf: Buffer,
    offset: u64, // bytes
}

impl<T: FileExt> Stamper<T> {
    pub fn new(dev: T, seed: u64, block_size: usize) -> Self {
        let buf = Buffer::new(block_size, PAGE_SIZE);
        Self {
            dev,
            block_size,
            seed,
            buf,
            offset: 0,
        }
    }

    pub fn offset(mut self, offset: u64) -> Self {
        self.offset = offset;
        self
    }
}

impl<T: FileExt> BlockVisitor for Stamper<T> {
    fn visit(&mut self, blocknr: u64) -> Result<()> {
        let mut gen = Generator::new();
        gen.fill_buffer(self.seed ^ blocknr, self.buf.get_data())?;

        let offset = blocknr * self.block_size as u64 + self.offset;
        self.dev.write_all_at(self.buf.get_data(), offset)?;
        Ok(())
    }
}

//------------------------------------------
