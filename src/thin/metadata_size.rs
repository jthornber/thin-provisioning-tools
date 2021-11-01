use anyhow::Result;

use crate::math::div_up;

pub struct ThinMetadataSizeOptions {
    pub nr_blocks: u64,
    pub max_thins: u64,
}

pub fn metadata_size(opts: &ThinMetadataSizeOptions) -> Result<u64> {
    const ENTRIES_PER_NODE: u64 = 126; // assumed the mapping leaves are half populated
    const BLOCK_SIZE: u64 = 8; // sectors

    // size of all the leaf nodes for data mappings
    let mapping_size = div_up(opts.nr_blocks, ENTRIES_PER_NODE) * BLOCK_SIZE;

    // space required by root nodes
    let roots_overhead = opts.max_thins * BLOCK_SIZE;

    Ok(mapping_size + roots_overhead)
}
