use anyhow::{anyhow, Result};
use std::ops::DerefMut;
use std::sync::{Arc, Mutex};

use crate::checksum;
use crate::io_engine::*;
use crate::pdata::space_map::*;

//------------------------------------------

#[derive(Clone)]
pub struct WriteBatcher {
    pub engine: Arc<dyn IoEngine + Send + Sync>,

    // FIXME: this doesn't need to be in a mutex
    pub sm: Arc<Mutex<dyn SpaceMap>>,

    batch_size: usize,
    queue: Vec<Block>,

    // The reserved range keeps track of all the blocks allocated.
    // An allocated block won't be reused even though it was freed.
    // In other words, the WriteBatcher performs allocation in
    // transactional fashion, that simplifies block allocationas
    // as well as tracking.
    reserved: std::ops::Range<u64>,
}

pub fn find_free(sm: &mut dyn SpaceMap, reserved: &std::ops::Range<u64>) -> Result<u64> {
    let nr_blocks = sm.get_nr_blocks()?;
    let mut b;
    if reserved.end >= reserved.start {
        b = sm.find_free(reserved.end, nr_blocks)?;
        if b.is_none() {
            b = sm.find_free(0, reserved.start)?;
        }
    } else {
        b = sm.find_free(reserved.end, reserved.start)?;
    }

    if b.is_none() {
        return Err(anyhow!("out of metadata space"));
    }

    Ok(b.unwrap())
}

impl WriteBatcher {
    pub fn new(
        engine: Arc<dyn IoEngine + Send + Sync>,
        sm: Arc<Mutex<dyn SpaceMap>>,
        batch_size: usize,
    ) -> WriteBatcher {
        let alloc_begin = sm.lock().unwrap().get_alloc_begin().unwrap_or(0);

        WriteBatcher {
            engine,
            sm,
            batch_size,
            queue: Vec::with_capacity(batch_size),
            reserved: std::ops::Range {
                start: alloc_begin,
                end: alloc_begin,
            },
        }
    }

    pub fn alloc(&mut self) -> Result<Block> {
        let mut sm = self.sm.lock().unwrap();
        let b = find_free(sm.deref_mut(), &self.reserved)?;
        self.reserved.end = b + 1;

        sm.set(b, 1)?;

        Ok(Block::new(b))
    }

    pub fn alloc_zeroed(&mut self) -> Result<Block> {
        let mut sm = self.sm.lock().unwrap();
        let b = find_free(sm.deref_mut(), &self.reserved)?;
        self.reserved.end = b + 1;

        sm.set(b, 1)?;

        Ok(Block::zeroed(b))
    }

    pub fn get_reserved_range(&self) -> std::ops::Range<u64> {
        std::ops::Range {
            start: self.reserved.start,
            end: self.reserved.end,
        }
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

    pub fn flush_(&mut self, queue: Vec<Block>) -> Result<()> {
        self.engine.write_many(&queue)?;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        let mut tmp = Vec::new();
        std::mem::swap(&mut tmp, &mut self.queue);
        self.flush_(tmp)?;
        Ok(())
    }
}

//------------------------------------------
