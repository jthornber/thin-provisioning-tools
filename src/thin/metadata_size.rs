use anyhow::{anyhow, Result};

use crate::math::div_up;

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

pub fn metadata_size(opts: &ThinMetadataSizeOptions) -> Result<u64> {
    const ENTRIES_PER_NODE: u64 = 126; // assumed the mapping leaves are half populated
    const BLOCK_SIZE: u64 = 4096; // bytes

    // size of all the leaf nodes for data mappings
    let mapping_size = div_up(opts.nr_blocks, ENTRIES_PER_NODE) * BLOCK_SIZE;

    // space required by root nodes
    let roots_overhead = opts.max_thins * BLOCK_SIZE;

    Ok(mapping_size + roots_overhead)
}
