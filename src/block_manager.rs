use std::io;
use std::io::{Read, Seek};
use std::fs::OpenOptions;
use std::os::unix::fs::OpenOptionsExt;
use std::fs::File;

pub const BLOCK_SIZE: usize = 4096;

pub struct Block {
    pub loc: u64,
    pub data: [u8; BLOCK_SIZE as usize],
}
    
pub struct BlockManager {
    pub nr_blocks: u64,
    input: File,
}

fn get_nr_blocks(path: &str) -> io::Result<u64> {
    let metadata = std::fs::metadata(path)?;
    Ok(metadata.len() / (BLOCK_SIZE as u64))
}

impl BlockManager {
    pub fn new(path: &str, _cache_size: usize) -> io::Result<BlockManager> {
        let input = OpenOptions::new()
            .read(true)
            .write(false)
            .custom_flags(libc::O_DIRECT)
            .open(path)?;

        Ok(BlockManager {
            nr_blocks: get_nr_blocks(path)?,
            input,
        })
    }

    pub fn get(&mut self, b: u64) -> io::Result<Block> {
        self.read_block(b)
    }

    fn read_block(&mut self, b: u64) -> io::Result<Block>
    {
        let mut buf = Block {loc: b, data: [0; BLOCK_SIZE]};

        self.input.seek(io::SeekFrom::Start(b * (BLOCK_SIZE as u64)))?;
        self.input.read_exact(&mut buf.data)?;

        Ok(buf)
    }
}
