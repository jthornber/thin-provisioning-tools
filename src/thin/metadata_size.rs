use anyhow::{anyhow, Result};

use crate::io_engine::BLOCK_SIZE;
use crate::math::div_up;
use crate::pdata::btree::calc_max_entries;
use crate::pdata::space_map::metadata::MAX_METADATA_BLOCKS;
use crate::thin::block_time::BlockTime;

//------------------------------------------

const MIN_DATA_BLOCK_SIZE: u64 = 65536;
const MAX_DATA_BLOCK_SIZE: u64 = 1073741824;

pub struct ThinMetadataSizeOptions {
    pub nr_blocks: u64,
    pub max_thins: u64,
}

pub fn check_data_block_size(block_size: u64) -> Result<()> {
    if block_size == 0 || (block_size & (MIN_DATA_BLOCK_SIZE - 1)) != 0 {
        return Err(anyhow!("block size must be a multiple of 64 KiB"));
    }

    if block_size > MAX_DATA_BLOCK_SIZE {
        return Err(anyhow!("maximum block size is 1 GiB"));
    }

    Ok(())
}

// Returns estimated size in bytes
pub fn metadata_size(opts: &ThinMetadataSizeOptions) -> Result<u64> {
    // assuming 50% residency on mapping tree leaves
    let entries_per_node: u64 = calc_max_entries::<BlockTime>() as u64 / 2;
    // number of leaves for data mappings
    let nr_leaves = div_up(opts.nr_blocks, entries_per_node);

    // one for the superblock, plus additional roots for each device
    let mut nr_blocks = 1 + nr_leaves + opts.max_thins;
    nr_blocks = std::cmp::min(nr_blocks, MAX_METADATA_BLOCKS as u64);

    Ok(nr_blocks * BLOCK_SIZE as u64)
}

//------------------------------------------
