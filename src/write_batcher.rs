use anyhow::{anyhow, Result};
use std::sync::{Arc, Mutex};

use crate::checksum;
use crate::io_engine::*;
use crate::pdata::space_map::*;

//------------------------------------------

pub struct WriteBatcher {
    engine: Arc<dyn IoEngine + Send + Sync>,
    sm: Arc<Mutex<dyn SpaceMap>>,

    batch_size: usize,
    queue: Vec<Block>,
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
        }
    }

    pub fn alloc(&mut self) -> Result<u64> {
        let mut sm = self.sm.lock().unwrap();
        let b = sm.alloc()?;

        if b.is_none() {
            return Err(anyhow!("out of metadata space"));
        }

        Ok(b.unwrap())
    }

    pub fn write(&mut self, b: Block, kind: checksum::BT) -> Result<()> {
        checksum::write_checksum(&mut b.get_data(), kind)?;

        if self.queue.len() == self.batch_size {
            self.flush()?;
        }

        self.queue.push(b);
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        self.engine.write_many(&self.queue)?;
        self.queue.clear();
        Ok(())
    }
}

//------------------------------------------
