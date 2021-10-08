use anyhow::Result;

pub struct CacheMetadataSizeOptions {
    pub nr_blocks: u64,
    pub max_hint_width: u32, // bytes
}

pub fn metadata_size(opts: &CacheMetadataSizeOptions) -> Result<u64> {
    const SECTOR_SHIFT: u64 = 9; // 512 bytes per sector
    const BYTES_PER_BLOCK_SHIFT: u64 = 4; // 16 bytes for key and value
    const TRANSACTION_OVERHEAD: u64 = 8192; // in sectors; 4 MB
    const HINT_OVERHEAD_PER_BLOCK: u64 = 8; // 8 bytes for the key

    let mapping_size = (opts.nr_blocks << BYTES_PER_BLOCK_SHIFT) >> SECTOR_SHIFT;
    let hint_size =
        (opts.nr_blocks * (opts.max_hint_width as u64 + HINT_OVERHEAD_PER_BLOCK)) >> SECTOR_SHIFT;

    Ok(TRANSACTION_OVERHEAD + mapping_size + hint_size)
}
