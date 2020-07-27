use std::error::Error;

use crate::block_manager::BlockManager;

pub fn check(dev: &str) -> Result<(), Box<dyn Error>> {
    let mut bm = BlockManager::new(dev, 1024)?;

    for b in 0..100 {
        let _block = bm.get(b)?;
    }

    Ok(())
}
