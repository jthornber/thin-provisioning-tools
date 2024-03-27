use anyhow::{anyhow, Result};
use devicemapper::*;
use roaring::bitmap::RoaringBitmap;
use std::collections::*;
use std::sync::Arc;
use std::sync::Mutex;

use crate::io_engine::*;
use crate::pdata::btree;
use crate::pdata::btree::*;
use crate::pdata::btree_lookup::*;
use crate::pdata::btree_walker::*;
use crate::pdata::unpack::*;
use crate::thin::block_time::*;
use crate::thin::device_detail::*;
use crate::thin::superblock::*;

//---------------------------------

/// A NodeVisitor that collects the virtual blocks that have been
/// mapped.  The results are collected in a RoaringBitmap so we can
/// handle sparsely provisioned volumes nicely.
#[derive(Default)]
struct MappingCollector {
    provisioned: Mutex<RoaringBitmap>,
}

impl MappingCollector {
    fn provisioned(self) -> RoaringBitmap {
        self.provisioned.into_inner().unwrap()
    }
}

impl NodeVisitor<BlockTime> for MappingCollector {
    fn visit(
        &self,
        _path: &[u64],
        _kr: &KeyRange,
        _header: &NodeHeader,
        keys: &[u64],
        _values: &[BlockTime],
    ) -> btree::Result<()> {
        let mut bits = self.provisioned.lock().unwrap();
        for k in keys {
            assert!(*k <= u32::MAX as u64);
            bits.insert(*k as u32);
        }
        Ok(())
    }

    fn visit_again(&self, _path: &[u64], _b: u64) -> btree::Result<()> {
        Ok(())
    }

    fn end_walk(&self) -> btree::Result<()> {
        Ok(())
    }
}

//---------------------------------

#[allow(dead_code)]
#[derive(Debug)]
pub struct ThinInfo {
    pub thin_id: u32,
    pub data_block_size: u32,
    pub details: DeviceDetail,
    pub provisioned_blocks: RoaringBitmap,
}

fn read_by_thin_id<V, Engine>(engine: &Engine, root: u64, thin_id: u32) -> Result<V>
where
    V: Unpack + Clone,
    Engine: IoEngine + Send + Sync,
{
    let lookup_result = btree_lookup::<V, Engine>(engine, root, thin_id as u64)?;

    if let Some(d) = lookup_result {
        Ok(d.clone())
    } else {
        Err(anyhow!("couldn't find thin device with id {}", thin_id))
    }
}

fn read_device_detail<Engine>(
    engine: &Engine,
    details_root: u64,
    thin_id: u32,
) -> Result<DeviceDetail>
where
    Engine: IoEngine + Send + Sync,
{
    read_by_thin_id(engine, details_root, thin_id)
}

fn read_mapping_root<Engine>(
    engine: &Engine,
    mappings_top_level_root: u64,
    thin_id: u32,
) -> Result<u64>
where
    Engine: IoEngine + Send + Sync,
{
    read_by_thin_id(engine, mappings_top_level_root, thin_id)
}

fn read_provisioned_blocks<Engine>(
    engine: &Arc<Engine>,
    mapping_top_level_root: u64,
    thin_id: u32,
) -> Result<RoaringBitmap>
where
    Engine: IoEngine + Send + Sync,
{
    let mapping_root = read_mapping_root(engine.as_ref(), mapping_top_level_root, thin_id)?;

    // walk mapping tree
    let ignore_non_fatal = true;
    let walker = BTreeWalker::new(engine.clone(), ignore_non_fatal);
    let collector = MappingCollector::default();

    let mut path = vec![];
    walker.walk(&mut path, &collector, mapping_root)?;
    Ok(collector.provisioned())
}

fn read_info<Engine>(engine: &Arc<Engine>, thin_id: u32) -> Result<ThinInfo>
where
    Engine: IoEngine + Send + Sync,
{
    let sb = read_superblock_snap(engine.as_ref())?;
    let details = read_device_detail(engine.as_ref(), sb.details_root, thin_id)?;
    let provisioned_blocks = read_provisioned_blocks(&engine, sb.mapping_root, thin_id)?;

    Ok(ThinInfo {
        thin_id,
        data_block_size: sb.data_block_size,
        details,
        provisioned_blocks,
    })
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

fn read_mappings<Engine>(
    engine: &Arc<Engine>,
    mapping_top_level_root: u64,
    thin_id: u32,
) -> Result<BTreeMap<u64, BlockTime>>
where
    Engine: IoEngine + Send + Sync,
{
    let root = read_mapping_root(engine.as_ref(), mapping_top_level_root, thin_id)?;

    let mut path = Vec::new();
    let new_mappings: BTreeMap<u64, BlockTime> =
        btree_to_map(&mut path, engine.clone(), true, root)?;

    Ok(new_mappings)
}

fn read_delta_info<Engine>(
    engine: &Arc<Engine>,
    old_thin_id: u32,
    new_thin_id: u32,
) -> Result<DeltaInfo>
where
    Engine: IoEngine + Send + Sync,
{
    // Read metadata superblock
    let sb = read_superblock_snap(engine.as_ref())?;
    let details = read_device_detail(engine.as_ref(), sb.details_root, new_thin_id)?;

    // FIXME: this uses a lot of memory
    let old_mappings = read_mappings(&engine, sb.mapping_root, old_thin_id)?;
    let new_mappings = read_mappings(&engine, sb.mapping_root, new_thin_id)?;

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
        if old_mappings.get(k).is_none() {
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

fn get_table(dm: &mut DM, dev: &DevId, expected_target_type: &str) -> Result<String> {
    let (_info, table) = dm.table_status(
        dev,
        DmOptions::default().set_flags(DmFlags::DM_STATUS_TABLE),
    )?;
    if table.len() != 1 {
        return Err(anyhow!(
            "thin table has too many rows (is it really a thin/pool device?)"
        ));
    }

    let (_offset, _len, target_type, args) = &table[0];
    if target_type != expected_target_type {
        return Err(anyhow!(format!(
            "dm expected table type {}, dm actual table type {}",
            expected_target_type, target_type
        )));
    }

    Ok(args.to_string())
}

//---------------------------------
