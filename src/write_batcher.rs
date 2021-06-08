use anyhow::{anyhow, Result};
use std::collections::BTreeSet;
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

    // The actual blocks allocated or reserved by this WriteBatcher
    allocations: BTreeSet<u64>,

    // The reserved range covers all the blocks allocated or reserved by this
    // WriteBatcher, and the blocks already occupied. No blocks in this range
    // are expected to be freed, hence a single range is used for the representation.
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
            allocations: BTreeSet::new(),
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
        self.allocations.insert(b);

        sm.set(b, 1)?;

        Ok(Block::new(b))
    }

    pub fn alloc_zeroed(&mut self) -> Result<Block> {
        let mut sm = self.sm.lock().unwrap();
        let b = find_free(sm.deref_mut(), &self.reserved)?;
        self.reserved.end = b + 1;
        self.allocations.insert(b);

        sm.set(b, 1)?;

        Ok(Block::zeroed(b))
    }

    pub fn reserve(&mut self) -> Result<Block> {
        let mut sm = self.sm.lock().unwrap();
        let b = find_free(sm.deref_mut(), &self.reserved)?;
        self.reserved.end = b + 1;
        self.allocations.insert(b);

        Ok(Block::new(b))
    }

    pub fn reserve_zeroed(&mut self) -> Result<Block> {
        let mut sm = self.sm.lock().unwrap();
        let b = find_free(sm.deref_mut(), &self.reserved)?;
        self.reserved.end = b + 1;
        self.allocations.insert(b);

        Ok(Block::zeroed(b))
    }

    pub fn clear_allocations(&mut self) -> BTreeSet<u64> {
        let mut tmp = BTreeSet::new();
        std::mem::swap(&mut tmp, &mut self.allocations);
        tmp
    }

    pub fn write(&mut self, b: Block, kind: checksum::BT) -> Result<()> {
        checksum::write_checksum(&mut b.get_data(), kind)?;

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
