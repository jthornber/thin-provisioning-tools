use anyhow::{anyhow, Result};

use crate::io_engine::BLOCK_SIZE;
use crate::pdata::space_map::metadata::MAX_METADATA_BLOCKS;

//------------------------------------------

const MIN_CACHE_BLOCK_SIZE: u64 = 32768;
const MAX_CACHE_BLOCK_SIZE: u64 = 1073741824;

pub struct CacheMetadataSizeOptions {
    pub nr_blocks: u64,
    pub max_hint_width: u32, // bytes
}

pub fn check_cache_block_size(block_size: u64) -> Result<()> {
    if block_size == 0 || (block_size & (MIN_CACHE_BLOCK_SIZE - 1)) != 0 {
        return Err(anyhow!("block size must be a multiple of 32 KiB"));
    }

    if block_size > MAX_CACHE_BLOCK_SIZE {
        return Err(anyhow!("maximum block size is 1 GiB"));
    }

    Ok(())
}

// Returns estimated size in bytes
pub fn metadata_size(opts: &CacheMetadataSizeOptions) -> Result<u64> {
    const BYTES_PER_BLOCK_SHIFT: u64 = 4; // 16 bytes for key and value
    const TRANSACTION_OVERHEAD: u64 = 4 * 1024 * 1024; // 4 MB
    const HINT_OVERHEAD_PER_BLOCK: u64 = 8; // 8 bytes for the key

    let mapping_size = opts.nr_blocks << BYTES_PER_BLOCK_SHIFT;
    let hint_size = opts.nr_blocks * (opts.max_hint_width as u64 + HINT_OVERHEAD_PER_BLOCK);

    let mut size = TRANSACTION_OVERHEAD + mapping_size + hint_size;
    size = std::cmp::min(size, MAX_METADATA_BLOCKS as u64 * BLOCK_SIZE as u64);

    Ok(size)
}

//------------------------------------------
