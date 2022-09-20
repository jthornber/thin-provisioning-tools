use anyhow::{anyhow, Result};
use rangemap::RangeSet;
use std::sync::{Arc, Mutex};

use crate::checksum;
use crate::io_engine::*;
use crate::pdata::space_map::*;

#[cfg(test)]
mod tests;

//------------------------------------------

pub struct WriteBatcher {
    pub engine: Arc<dyn IoEngine + Send + Sync>,

    // FIXME: this doesn't need to be in a mutex
    pub sm: Arc<Mutex<dyn SpaceMap>>,

    batch_size: usize,
    queue: Vec<Block>,

    // The allocations could be a hint of potentially modified blocks.
    // The blocks in allocations doesn't necessarily have non-zero ref counts,
    // if the caller returns the allocated blocks via SpaceMap::dec().
    allocations: RangeSet<u64>,
}

impl WriteBatcher {
    pub fn new(
        engine: Arc<dyn IoEngine + Send + Sync>,
        sm: Arc<Mutex<dyn SpaceMap>>,
        batch_size: usize,
    ) -> WriteBatcher {
        WriteBatcher {
            engine,
            sm,
            batch_size,
            queue: Vec::with_capacity(batch_size),
            allocations: RangeSet::<u64>::new(),
        }
    }

    pub fn alloc(&mut self) -> Result<Block> {
        let mut sm = self.sm.lock().unwrap();
        let b = sm.alloc()?;
        if b.is_none() {
            return Err(anyhow!("out of metadata space"));
        }

        let loc = b.unwrap();
        self.allocations.insert(std::ops::Range {
            start: loc,
            end: loc + 1,
        });

        Ok(Block::new(loc))
    }

    pub fn alloc_zeroed(&mut self) -> Result<Block> {
        let mut sm = self.sm.lock().unwrap();
        let b = sm.alloc()?;
        if b.is_none() {
            return Err(anyhow!("out of metadata space"));
        }

        let loc = b.unwrap();
        self.allocations.insert(std::ops::Range {
            start: loc,
            end: loc + 1,
        });

        Ok(Block::zeroed(loc))
    }

    pub fn clear_allocations(&mut self) -> RangeSet<u64> {
        let mut tmp = RangeSet::<u64>::new();
        std::mem::swap(&mut tmp, &mut self.allocations);
        tmp
    }

    pub fn write(&mut self, b: Block, kind: checksum::BT) -> Result<()> {
        checksum::write_checksum(b.get_data(), kind)?;

        for blk in self.queue.iter().rev() {
            if blk.loc == b.loc {
                // write hit
                blk.get_data().copy_from_slice(b.get_data());
                return Ok(());
            }
        }

        if self.queue.len() == self.batch_size {
            let mut tmp = Vec::new();
            std::mem::swap(&mut tmp, &mut self.queue);
            self.flush_(tmp)?;
        }

        self.queue.push(b);
        Ok(())
    }

    pub fn read(&mut self, blocknr: u64) -> Result<Block> {
        for b in self.queue.iter().rev() {
            if b.loc == blocknr {
                let r = Block::new(b.loc);
                r.get_data().copy_from_slice(b.get_data());
                return Ok(r);
            }
        }

        self.engine
            .read(blocknr)
            .map_err(|_| anyhow!("read block error"))
    }

    fn flush_(&mut self, queue: Vec<Block>) -> Result<()> {
        self.engine.write_many(&queue)?;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        if self.queue.is_empty() {
            return Ok(());
        }
        let mut tmp = Vec::new();
        std::mem::swap(&mut tmp, &mut self.queue);
        self.flush_(tmp)?;
        Ok(())
    }
}

impl Drop for WriteBatcher {
    fn drop(&mut self) {
        assert!(self.flush().is_ok());
    }
}

//------------------------------------------
