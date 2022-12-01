use anyhow::{anyhow, Result};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::vec::Vec;

use crate::checksum;
use crate::commands::engine::*;
use crate::io_engine::IoEngine;
use crate::pdata::btree::{self, *};
use crate::pdata::btree_walker::*;
use crate::pdata::space_map::common::*;
use crate::pdata::space_map::metadata::*;
use crate::pdata::unpack::*;
use crate::thin::superblock::*;

//------------------------------------------

struct RefCounter {
    histogram: Mutex<BTreeMap<u32, u64>>,
}

impl RefCounter {
    fn new(histogram: BTreeMap<u32, u64>) -> Self {
        RefCounter {
            histogram: Mutex::new(histogram),
        }
    }

    fn complete(self) -> BTreeMap<u32, u64> {
        self.histogram.into_inner().unwrap()
    }
}

impl NodeVisitor<u32> for RefCounter {
    fn visit(
        &self,
        _path: &[u64],
        _kr: &KeyRange,
        _h: &NodeHeader,
        _keys: &[u64],
        values: &[u32],
    ) -> btree::Result<()> {
        let mut histogram = self.histogram.lock().unwrap();
        for count in values {
            *histogram.entry(*count).or_insert(0) += 1;
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

fn gather_btree_index_entries(
    engine: Arc<dyn IoEngine + Send + Sync>,
    bitmap_root: u64,
) -> Result<Vec<IndexEntry>> {
    let entries_map = btree_to_map::<IndexEntry>(&mut vec![0], engine, true, bitmap_root)?;
    let entries: Vec<IndexEntry> = entries_map.values().cloned().collect();
    Ok(entries)
}

fn gather_metadata_index_entries(
    engine: Arc<dyn IoEngine + Send + Sync>,
    bitmap_root: u64,
) -> Result<Vec<IndexEntry>> {
    let b = engine.read(bitmap_root)?;
    let entries = check_and_unpack_metadata_index(&b)?.indexes;
    Ok(entries)
}

fn stat_low_ref_counts_in_bitmap(bitmap: Bitmap, histogram: &mut BTreeMap<u32, u64>) -> Result<()> {
    for e in bitmap.entries {
        if let BitmapEntry::Small(count) = e {
            if count > 0 && count <= 2 {
                *histogram.entry(count as u32).or_insert(0) += 1;
            }
        }
    }

    Ok(())
}

//------------------------------------------

fn stat_low_ref_counts(
    engine: Arc<dyn IoEngine + Send + Sync>,
    entries: &[IndexEntry],
    histogram: &mut BTreeMap<u32, u64>,
) -> Result<()> {
    let blocks: Vec<u64> = entries.iter().map(|entry| entry.blocknr).collect();
    let rblocks = engine.read_many(&blocks)?;
    for rb in rblocks.into_iter() {
        if rb.is_err() {
            return Err(anyhow!("Unable to read bitmap block"));
        }

        let b = rb.unwrap();

        if checksum::metadata_block_type(b.get_data()) != checksum::BT::BITMAP {
            return Err(anyhow!(
                "Index entry points to block ({}) that isn't a bitmap",
                b.loc
            ));
        }

        let bitmap = unpack::<Bitmap>(b.get_data())?;
        stat_low_ref_counts_in_bitmap(bitmap, histogram)?;
    }

    Ok(())
}

fn stat_overflow_ref_counts(
    engine: Arc<dyn IoEngine + Send + Sync>,
    root: u64,
    histogram: BTreeMap<u32, u64>,
) -> Result<BTreeMap<u32, u64>> {
    let w = BTreeWalker::new(engine, true);
    let v = RefCounter::new(histogram);
    w.walk(&mut vec![0], &v, root)
        .map_err(|_| anyhow!("Errors in reading ref-count tree"))?;
    Ok(v.complete())
}

fn stat_data_block_ref_counts(
    engine: Arc<dyn IoEngine + Send + Sync>,
    root: SMRoot,
) -> Result<BTreeMap<u32, u64>> {
    let mut histogram = BTreeMap::<u32, u64>::new();

    let index_entries = gather_btree_index_entries(engine.clone(), root.bitmap_root)?;
    stat_low_ref_counts(engine.clone(), &index_entries, &mut histogram)?;

    let histogram = stat_overflow_ref_counts(engine, root.ref_count_root, histogram)?;

    Ok(histogram)
}

fn stat_metadata_block_ref_counts(
    engine: Arc<dyn IoEngine + Send + Sync>,
    root: SMRoot,
) -> Result<BTreeMap<u32, u64>> {
    let mut histogram = BTreeMap::<u32, u64>::new();

    let index_entries = gather_metadata_index_entries(engine.clone(), root.bitmap_root)?;
    stat_low_ref_counts(engine.clone(), &index_entries, &mut histogram)?;

    let histogram = stat_overflow_ref_counts(engine, root.ref_count_root, histogram)?;

    Ok(histogram)
}

// TODO: plot the graph
fn print_data_blocks_histogram(engine: Arc<dyn IoEngine + Send + Sync>) -> Result<()> {
    let sb = read_superblock(engine.as_ref(), SUPERBLOCK_LOCATION)?;
    let sm_root = unpack::<SMRoot>(&sb.data_sm_root)?;

    let histogram = stat_data_block_ref_counts(engine, sm_root)?;
    let allocated: u64 = histogram.values().sum();
    let mut avg_rc = 0.0;
    for (k, v) in histogram {
        let ratio = v as f64 / allocated as f64;
        avg_rc += k as f64 * ratio;
        println!("{}\t{}\t{:.4}", k, v, ratio * 100.0);
    }

    println!("{} blocks allocated", allocated);
    println!("avg ref count = {:.2}", avg_rc);

    Ok(())
}

// TODO: plot the graph
fn print_metadata_blocks_histogram(engine: Arc<dyn IoEngine + Send + Sync>) -> Result<()> {
    let sb = read_superblock(engine.as_ref(), SUPERBLOCK_LOCATION)?;
    let sm_root = unpack::<SMRoot>(&sb.metadata_sm_root)?;

    let histogram = stat_metadata_block_ref_counts(engine, sm_root)?;
    let allocated: u64 = histogram.values().sum();
    let mut avg_rc = 0.0;
    for (k, v) in histogram {
        let ratio = v as f64 / allocated as f64;
        avg_rc += k as f64 * ratio;
        println!("{}\t{}\t{:.4}", k, v, ratio * 100.0);
    }

    println!("{} blocks allocated", allocated);
    println!("avg ref count = {:.2}", avg_rc);

    Ok(())
}

//------------------------------------------

pub enum StatOp {
    DataBlockRefCounts,
    MetadataBlockRefCounts,
}

pub struct ThinStatOpts<'a> {
    pub input: &'a Path,
    pub engine_opts: EngineOptions,
    pub op: StatOp,
}

pub fn stat(opts: ThinStatOpts) -> Result<()> {
    let engine = EngineBuilder::new(opts.input, &opts.engine_opts)
        .write(true)
        .build()?;

    match opts.op {
        StatOp::DataBlockRefCounts => print_data_blocks_histogram(engine)?,
        StatOp::MetadataBlockRefCounts => print_metadata_blocks_histogram(engine)?,
    }

    Ok(())
}
