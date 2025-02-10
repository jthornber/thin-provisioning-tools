use std::collections::HashMap;
use std::io;

use crate::io_engine::BLOCK_SIZE;
use crate::io_engine::base::*;

//--------------------------------

/// Minimum block size in bytes (4KB)
pub const MIN_BLOCK_SIZE: usize = 4 * 1024;
/// Maximum block size in bytes (16MB)
pub const MAX_BLOCK_SIZE: usize = 16 * 1024 * 1024;
/// Minimum buffer size in megabytes
pub const MIN_BUFFER_SIZE_MB: usize = 1;
/// Maximum buffer size in megabytes (1GB)
pub const MAX_BUFFER_SIZE_MB: usize = 1024;

//--------------------------------

/// Maps smaller logical blocks to larger IO blocks for efficient reading.
/// 
/// # Arguments
/// * `blocks` - Iterator over logical block numbers
/// * `blocks_per_io` - Number of logical blocks that fit in one IO block
/// 
/// # Returns
/// * `HashMap<u64, u64>` - Mapping from logical block numbers to IO block numbers
pub fn map_small_blocks_to_io(blocks: &mut dyn Iterator<Item = u64>, blocks_per_io: usize) -> HashMap<u64, u64> {
    let mut io_block_map = HashMap::new();
    for block in blocks {
        let io_block = block / blocks_per_io as u64;
        io_block_map.insert(block, io_block);
    }
    io_block_map
}

/// Processes the result of reading an IO block and splits it into logical blocks.
/// 
/// # Arguments
/// * `io_idx` - Index of the IO block
/// * `result` - Result containing the read data or an error
/// * `blocks_per_io` - Number of logical blocks per IO block
/// * `io_block_map` - Mapping from logical to IO block numbers
/// * `handler` - Handler to receive the processed results
pub fn process_io_block_result(
    io_idx: u64,
    result: Result<&[u8], io::Error>,
    blocks_per_io: usize,
    io_block_map: &HashMap<u64, u64>,
    handler: &mut dyn ReadHandler,
) {
    match result {
        Ok(data) => {
            // Find all the logical blocks that map to this IO block
            let logical_blocks: Vec<_> = io_block_map
                .iter()
                .filter(|(_, &io_block)| io_block == io_idx)
                .map(|(&logical_block, _)| logical_block)
                .collect();

            // Process each logical block
            for logical_block in logical_blocks {
                let offset = ((logical_block % blocks_per_io as u64) * BLOCK_SIZE as u64) as usize;
                let end = offset + BLOCK_SIZE;
                if end <= data.len() {
                    handler.handle(logical_block, Ok(&data[offset..end]));
                } else {
                    handler.handle(logical_block, Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "incomplete block read"
                    )));
                }
            }
        }
        Err(_e) => {
            // Find all logical blocks affected by this IO error
            for (&logical_block, &io_block) in io_block_map.iter() {
                if io_block == io_idx {
                   handler.handle(logical_block, Err(io::Error::new(io::ErrorKind::Other, "read failed")));
                }
            }
        }
    }
}

