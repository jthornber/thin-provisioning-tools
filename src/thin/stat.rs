use anyhow::{anyhow, Result};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
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
use crate::thin::block_time::*;
use crate::thin::dump::RunBuilder;
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
    println!("ref-count\ttimes\tpercentage");
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
    println!("ref-count\ttimes\tpercentage");
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

struct RunLengthCounter {
    histogram: Mutex<BTreeMap<u32, u64>>,
    builder: Mutex<RunBuilder>,
    nr_leaves: AtomicU64,
}

impl RunLengthCounter {
    fn new() -> Self {
        RunLengthCounter {
            histogram: Mutex::new(BTreeMap::new()),
            builder: Mutex::new(RunBuilder::new()),
            nr_leaves: AtomicU64::default(),
        }
    }

    fn complete(self) -> (BTreeMap<u32, u64>, u64) {
        (
            self.histogram.into_inner().unwrap(),
            self.nr_leaves.load(Ordering::SeqCst),
        )
    }
}

impl NodeVisitor<BlockTime> for RunLengthCounter {
    fn visit(
        &self,
        _path: &[u64],
        _kr: &KeyRange,
        _h: &NodeHeader,
        keys: &[u64],
        values: &[BlockTime],
    ) -> btree::Result<()> {
        let mut histogram = self.histogram.lock().unwrap();
        let mut builder = self.builder.lock().unwrap();

        for (k, v) in keys.iter().zip(values.iter()) {
            if let Some(run) = builder.next(*k, v.block, v.time) {
                *histogram.entry(run.len as u32).or_insert(0) += 1;
            }
        }

        self.nr_leaves.fetch_add(1, Ordering::SeqCst);

        Ok(())
    }

    fn visit_again(&self, _path: &[u64], _b: u64) -> btree::Result<()> {
        Ok(())
    }

    fn end_walk(&self) -> btree::Result<()> {
        let mut histogram = self.histogram.lock().unwrap();
        let mut builder = self.builder.lock().unwrap();

        if let Some(run) = builder.complete() {
            *histogram.entry(run.len as u32).or_insert(0) += 1;
        }

        Ok(())
    }
}

fn stat_data_run_lengths(
    engine: Arc<dyn IoEngine + Send + Sync>,
    mapping_root: u64,
) -> Result<(BTreeMap<u32, u64>, u64)> {
    let mut path = vec![];
    let roots = btree_to_map::<u64>(&mut path, engine.clone(), true, mapping_root)?;

    let counter = RunLengthCounter::new();
    let w = BTreeWalker::new(engine.clone(), true);
    for root in roots.values() {
        w.walk(&mut path, &counter, *root)?;
    }

    Ok(counter.complete())
}

fn print_data_run_length_histogram(engine: Arc<dyn IoEngine + Send + Sync>) -> Result<()> {
    let sb = read_superblock(engine.as_ref(), SUPERBLOCK_LOCATION)?;
    let (histogram, nr_leaves) = stat_data_run_lengths(engine, sb.mapping_root)?;

    let nr_runs: u64 = histogram.values().sum();
    let mut avg_len = 0.0;
    println!("length\tcounts\tpercentage");
    for (k, v) in histogram {
        let ratio = v as f64 / nr_runs as f64;
        avg_len += k as f64 * ratio;
        println!("{}\t{}\t{:.4}", k, v, ratio * 100.0);
    }

    println!("{} runs in {} leaves", nr_runs, nr_leaves);
    println!("avg run length = {:.2}", avg_len);

    Ok(())
}

//------------------------------------------

pub enum StatOp {
    DataBlockRefCounts,
    MetadataBlockRefCounts,
    DataRunLength,
}

pub struct ThinStatOpts<'a> {
    pub input: &'a Path,
    pub engine_opts: EngineOptions,
    pub op: StatOp,
}

pub fn stat(opts: ThinStatOpts) -> Result<()> {
    let engine = EngineBuilder::new(opts.input, &opts.engine_opts).build()?;

    match opts.op {
        StatOp::DataBlockRefCounts => print_data_blocks_histogram(engine)?,
        StatOp::MetadataBlockRefCounts => print_metadata_blocks_histogram(engine)?,
        StatOp::DataRunLength => print_data_run_length_histogram(engine)?,
    }

    Ok(())
}
