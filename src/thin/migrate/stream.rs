use anyhow::{anyhow, Result};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::os::unix::fs::MetadataExt;
use std::sync::Arc;

use crate::io_engine::*;
use crate::thin::migrate::metadata;

//---------------------------------------------

pub enum ChunkContents {
    Data,
    Unmapped,
    Discard,
}

pub struct Chunk {
    offset: u64,
    len: u64,
    contents: ChunkContents,
}

pub trait Stream {
    fn next_chunk(&mut self) -> Option<Chunk>;
}

//---------------------------------------------

pub struct DevStream {
    eof: bool,
    file_size: u64,
}

impl DevStream {
    pub fn new(file: File, chunk_size: u64) -> Result<Self> {
        let file_size = file.metadata()?.size();
        Ok(Self {
            eof: false,
            file_size,
        })
    }
}

impl Stream for DevStream {
    fn next_chunk(&mut self) -> Option<Chunk> {
        if self.eof {
            None
        } else {
            Some(Chunk {
                offset: 0,
                len: self.file_size,
                contents: ChunkContents::Data,
            })
        }
    }
}

//---------------------------------------------

// FIXME: allow chunks to be bigger than the data_block_size
pub struct ThinStream {
    info: metadata::ThinInfo,
    current_block: u64,
    nr_blocks: u64,
}

impl ThinStream {
    pub fn new<Engine>(metadata_engine: &Arc<Engine>, thin_id: u32, thin: File) -> Result<Self>
    where
        Engine: IoEngine + Send + Sync,
    {
        let info = metadata::read_info(metadata_engine, thin_id)?;
        let nr_blocks = thin.metadata()?.size() / info.data_block_size as u64;
        Ok(Self {
            info,
            current_block: 0,
            nr_blocks,
        })
    }

    // FIXME: we need a more efficient way of building runs.  Info shouldn't store in a bitmap.
    fn get_mapped_run_len(&mut self, begin: u64) -> u64 {
        let mut end = begin;
        while end < self.nr_blocks {
            if !self.info.provisioned_blocks.contains(end as u32) {
                break;
            }

            end += 1;
        }

        end - begin
    }

    fn get_unmapped_run_len(&mut self, begin: u64) -> u64 {
        let mut end = begin;
        while end < self.nr_blocks {
            if self.info.provisioned_blocks.contains(end as u32) {
                break;
            }

            end += 1;
        }

        end - begin
    }
}

impl Stream for ThinStream {
    fn next_chunk(&mut self) -> Option<Chunk> {
        let begin = self.current_block;

        if begin >= self.nr_blocks {
            return None;
        }

        // FIXME: is the data_block_size in bytes or sectors?
        let bsize = self.info.data_block_size as u64;
        if self.info.provisioned_blocks.contains(begin as u32) {
            let len = self.get_mapped_run_len(begin);
            Some(Chunk {
                offset: begin * bsize,
                len: len * bsize,
                contents: ChunkContents::Data,
            })
        } else {
            let len = self.get_unmapped_run_len(begin);
            Some(Chunk {
                offset: begin * bsize,
                len: len * bsize,
                contents: ChunkContents::Unmapped,
            })
        }
    }
}

//---------------------------------------------

pub struct DeltaStream {
    info: DeltaInfo,
}

impl DeltaStream {}

impl Stream for DeltaStream {
    fn next_chunk(&mut self) -> Option<Chunk> {
        todo!();
    }
}

//---------------------------------------------
