use anyhow::Result;
use std::fs::File;
use std::os::unix::fs::MetadataExt;
use std::sync::Arc;

use crate::io_engine::*;
use crate::thin::migrate::metadata::*;

//---------------------------------------------

pub enum ChunkContents {
    Copy,
    Skip,
    Discard,
}

pub struct Chunk {
    pub offset: u64,
    pub len: u64,
    pub contents: ChunkContents,
}

pub trait Stream {
    fn next_chunk(&mut self) -> Result<Option<Chunk>>;
    fn size_hint(&self) -> u64;
}

//---------------------------------------------

pub struct DevStream {
    eof: bool,
    file_size: u64,
}

impl DevStream {
    pub fn new(file: File, _chunk_size: u64) -> Result<Self> {
        let file_size = file.metadata()?.size();
        Ok(Self {
            eof: false,
            file_size,
        })
    }
}

impl Stream for DevStream {
    fn next_chunk(&mut self) -> Result<Option<Chunk>> {
        if self.eof {
            Ok(None)
        } else {
            self.eof = true;
            Ok(Some(Chunk {
                offset: 0,
                len: self.file_size,
                contents: ChunkContents::Copy,
            }))
        }
    }

    fn size_hint(&self) -> u64 {
        self.file_size
    }
}

//---------------------------------------------

pub struct ThinStream {
    iter: ThinIterator,
    current_block: u64,
}

impl ThinStream {
    pub fn new(metadata_engine: &Arc<dyn IoEngine + Sync + Send>, thin_id: u32) -> Result<Self> {
        let iter = ThinIterator::new(metadata_engine, thin_id)?;
        Ok(Self {
            iter,
            current_block: 0,
        })
    }

    fn contiguous_run(&mut self, begin: u64) -> Result<u64> {
        let mut count = 0u64;
        loop {
            count += 1;
            self.iter.mappings.step()?;
            if let Some((thin_block, _)) = self.iter.mappings.get() {
                if thin_block != begin + count {
                    break;
                }
            } else {
                break;
            }
        }

        Ok(count)
    }
}

impl Stream for ThinStream {
    fn next_chunk(&mut self) -> Result<Option<Chunk>> {
        use ChunkContents::*;

        let offset = self.current_block * self.iter.data_block_size;
        match self.iter.mappings.get() {
            Some((thin_block, _)) if thin_block == self.current_block => {
                // There was no intermediate gap, so we can return the next mapping.
                let nr_blocks = self.contiguous_run(thin_block)?;
                let len = nr_blocks * self.iter.data_block_size;
                self.current_block = thin_block + nr_blocks;
                Ok(Some(Chunk {
                    offset,
                    len,
                    contents: Copy,
                }))
            }
            Some((thin_block, _)) => {
                // Emit the intermediate unmapped region.
                let len = (thin_block - self.current_block) * self.iter.data_block_size;
                self.current_block = thin_block;
                Ok(Some(Chunk {
                    offset,
                    len,
                    contents: Skip,
                }))
            }
            None => Ok(None),
        }
    }

    fn size_hint(&self) -> u64 {
        self.iter.mapped_blocks * self.iter.data_block_size
    }
}

//---------------------------------------------

/*
pub struct DeltaStream {
    info: DeltaInfo,
}

impl DeltaStream {}

impl Stream for DeltaStream {
    fn next_chunk(&mut self) -> Option<Chunk> {
        todo!();
    }
}
*/

//---------------------------------------------
