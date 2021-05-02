use anyhow::{anyhow, Result};
use std::collections::BTreeSet;
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
    allocations: BTreeSet<u64>,
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
            allocations: BTreeSet::new(),
        }
    }

    pub fn alloc(&mut self) -> Result<Block> {
        let mut sm = self.sm.lock().unwrap();
        let b = sm.alloc()?;

        if b.is_none() {
            return Err(anyhow!("out of metadata space"));
        }

        Ok(Block::new(b.unwrap()))
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

        self.engine.read(blocknr).map_err(|_| anyhow!("read block error"))
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
