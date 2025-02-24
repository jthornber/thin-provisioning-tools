use anyhow::{anyhow, Result};
use std::sync::Arc;

use crate::io_engine::*;
use crate::pdata::btree_iterator::*;
use crate::pdata::btree_lookup::*;
use crate::pdata::unpack::*;
use crate::thin::block_time::*;
use crate::thin::device_detail::*;
use crate::thin::superblock::*;

//---------------------------------

type ArcEngine = Arc<dyn IoEngine + Send + Sync>;

fn read_by_thin_id<V>(engine: &ArcEngine, root: u64, thin_id: u32) -> Result<V>
where
    V: Unpack + Clone,
{
    let lookup_result = btree_lookup::<V>(engine.as_ref(), root, thin_id as u64)?;

    if let Some(d) = lookup_result {
        Ok(d.clone())
    } else {
        Err(anyhow!("couldn't find thin device with id {}", thin_id))
    }
}

fn read_device_detail(engine: &ArcEngine, details_root: u64, thin_id: u32) -> Result<DeviceDetail> {
    read_by_thin_id(engine, details_root, thin_id)
}

fn read_mapping_root(
    engine: &ArcEngine,
    mappings_top_level_root: u64,
    thin_id: u32,
) -> Result<u64> {
    read_by_thin_id(engine, mappings_top_level_root, thin_id)
}

pub struct ThinIterator {
    pub thin_id: u32,
    pub data_block_size: u64,
    pub mappings: BTreeIterator<BlockTime>,
    pub mapped_blocks: u64,
}

impl ThinIterator {
    pub fn new(engine: &ArcEngine, thin_id: u32) -> Result<Self> {
        let sb = read_superblock_snap(engine.as_ref())?;
        let details = read_device_detail(engine, sb.details_root, thin_id)?;
        let mapping_root = read_mapping_root(engine, sb.mapping_root, thin_id)?;
        let mappings = BTreeIterator::new(engine.clone(), mapping_root)?;

        Ok(Self {
            thin_id,
            data_block_size: sb.data_block_size as u64,
            mappings,
            mapped_blocks: details.mapped_blocks,
        })
    }
}

//---------------------------------
