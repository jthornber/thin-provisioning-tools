use anyhow::{anyhow, Result};
use roaring::bitmap::RoaringBitmap;
use std::collections::*;
use std::sync::Arc;

use crate::io_engine::*;
use crate::pdata::btree_iterator::*;
use crate::pdata::btree_lookup::*;
use crate::pdata::btree_walker::*;
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
    let lookup_result = btree_lookup::<V>(engine, root, thin_id as u64)?;

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
}

impl ThinIterator {
    pub fn new(engine: &ArcEngine, thin_id: u32) -> Result<Self> {
        let sb = read_superblock_snap(engine.as_ref())?;
        let _details = read_device_detail(engine, sb.details_root, thin_id)?;
        let mapping_root = read_mapping_root(engine, sb.mapping_root, thin_id)?;
        let mappings = BTreeIterator::new(engine.clone(), mapping_root)?;

        Ok(Self {
            thin_id,
            data_block_size: sb.data_block_size as u64,
            mappings,
        })
    }
}

//---------------------------------

#[allow(dead_code)]
#[derive(Debug)]
pub struct DeltaInfo {
    pub thin_id: u32,
    pub data_block_size: u32,
    pub details: DeviceDetail,
    pub additions: RoaringBitmap,
    pub removals: RoaringBitmap,
}

fn read_mappings(
    engine: &ArcEngine,
    mapping_top_level_root: u64,
    thin_id: u32,
) -> Result<BTreeMap<u64, BlockTime>> {
    let root = read_mapping_root(engine, mapping_top_level_root, thin_id)?;

    let mut path = Vec::new();
    let new_mappings: BTreeMap<u64, BlockTime> =
        btree_to_map(&mut path, engine.clone(), true, root)?;

    Ok(new_mappings)
}

pub fn read_delta_info(
    engine: &ArcEngine,
    old_thin_id: u32,
    new_thin_id: u32,
) -> Result<DeltaInfo> {
    // Read metadata superblock
    let sb = read_superblock_snap(engine.as_ref())?;
    let details = read_device_detail(engine, sb.details_root, new_thin_id)?;

    // FIXME: this uses a lot of memory
    let old_mappings = read_mappings(engine, sb.mapping_root, old_thin_id)?;
    let new_mappings = read_mappings(engine, sb.mapping_root, new_thin_id)?;

    let mut additions = RoaringBitmap::default();
    let mut removals = RoaringBitmap::default();
    for (k, v1) in &old_mappings {
        if let Some(v2) = new_mappings.get(k) {
            if *v2 == *v1 {
                // mapping hasn't changed
            } else {
                additions.insert(*k as u32);
            }
        } else {
            // unmapped
            removals.insert(*k as u32);
        }
    }

    for k in new_mappings.keys() {
        if !old_mappings.contains_key(k) {
            additions.insert(*k as u32);
        }
    }

    Ok(DeltaInfo {
        thin_id: new_thin_id,
        data_block_size: sb.data_block_size,
        details,
        additions,
        removals,
    })
}

//---------------------------------
