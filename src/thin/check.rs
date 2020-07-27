use std::error::Error;
use std::path::Path;
use std::time::{Duration, Instant};
use std::thread;
use std::sync::{Arc, Mutex};

use crate::block_manager::{Block, IoEngine, SyncIoEngine, BLOCK_SIZE};

pub fn check(dev: &Path) -> Result<(), Box<dyn Error>> {
    let mut engine = SyncIoEngine::new(dev)?;
    let count = 4096;

    let mut blocks = Vec::new();
    for n in 0..count {
        blocks.push(Block::new(n));
    }

    let now = Instant::now();
    engine.read(&mut blocks)?;
    println!("read {} blocks in {} ms", count, now.elapsed().as_millis());

    Ok(())
}
