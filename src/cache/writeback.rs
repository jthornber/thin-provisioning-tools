use anyhow::anyhow;
use roaring::RoaringBitmap;
use std::fs::File;
use std::io::Cursor;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread;

use crate::cache::mapping::*;
use crate::cache::superblock::*;
use crate::checksum;
use crate::commands::engine::*;
use crate::io_engine::copier::*;
use crate::io_engine::sync_copier::*;
use crate::io_engine::utils::VectoredBlockIo;
use crate::io_engine::{self, *};
use crate::pdata::array::{self, *};
use crate::pdata::array_walker::*;
use crate::pdata::bitset::read_bitset;
use crate::pdata::btree_walker::*;
use crate::report::Report;

//-----------------------------------------

#[derive(Clone)]
struct WritebackStats {
    /// Number of blocks that need writing back
    nr_blocks: u64,

    /// Number of blocks that were successfully copied
    nr_copied: u64,

    /// Number of read errors
    nr_read_errors: u64,

    // Number of write errors
    nr_write_errors: u64,
}

/// Updates the progress bar in the reporter as the stats are
/// updated by the copier threads.
struct ProgressReporter {
    report: Arc<Report>,
    inner: Mutex<WritebackStats>,
}

impl ProgressReporter {
    /// nr_blocks - nr to be copied
    fn new(report: Arc<Report>, nr_blocks: u64) -> Self {
        Self {
            report,
            inner: Mutex::new(WritebackStats {
                nr_blocks,
                nr_copied: 0,
                nr_read_errors: 0,
                nr_write_errors: 0,
            }),
        }
    }

    /// Adds stats into the internal base.  Then updates the progress.
    fn inc_stats(&self, stats: &CopyStats) {
        let mut inner = self.inner.lock().unwrap();
        inner.nr_copied += stats.nr_copied.load(Ordering::Relaxed);
        inner.nr_read_errors += stats.read_errors.len() as u64;
        inner.nr_write_errors += stats.write_errors.len() as u64;

        self.report.set_sub_title(&format!(
            "read errors {}, write errors {}",
            inner.nr_read_errors, inner.nr_write_errors
        ));

        let percent = (inner.nr_copied * 100)
            .checked_div(inner.nr_blocks)
            .unwrap_or(100);
        self.report.progress(percent as u8);
    }

    /// Accessor for the current stats
    fn stats(&self) -> WritebackStats {
        let inner = self.inner.lock().unwrap();
        (*inner).clone()
    }
}

impl CopyProgress for ProgressReporter {
    // This doesn't update the data fields, that is left until the copy
    // batch is complete.
    fn update(&self, stats: &CopyStats) {
        let inner = self.inner.lock().unwrap();

        self.report.set_sub_title(&format!(
            "read errors {}, write errors {}",
            inner.nr_read_errors + stats.read_errors.len() as u64,
            inner.nr_write_errors + stats.write_errors.len() as u64
        ));

        let percent =
            ((inner.nr_copied + stats.nr_copied.load(Ordering::Relaxed)) * 100) / inner.nr_blocks;
        self.report.progress(percent as u8);
    }
}

//-----------------------------------------

// Indicates whether a block should be copied.  There are differences
// between v1 and v2 metadata, also an unclean shutdown will result in
// all blocks being copied.
trait WritebackSelector {
    fn get_nr_to_writeback(&self) -> u32;

    /// The caller should ensure the mapping is valid.
    fn needs_writeback(&self, cblock: u32, m: &Mapping) -> bool;
}

//---------------

/// Selector for version 1 metadata.
struct V1Selector {
    nr_blocks: u32,
}

impl WritebackSelector for V1Selector {
    fn get_nr_to_writeback(&self) -> u32 {
        self.nr_blocks
    }

    fn needs_writeback(&self, _cblock: u32, m: &Mapping) -> bool {
        m.is_dirty()
    }
}

//---------------

/// Version 2 metadata has a separate dirty bitset
struct V2Selector {
    nr_blocks: u32,
    dirty: roaring::RoaringBitmap,
}

impl WritebackSelector for V2Selector {
    fn get_nr_to_writeback(&self) -> u32 {
        self.nr_blocks
    }

    fn needs_writeback(&self, cblock: u32, _m: &Mapping) -> bool {
        self.dirty.contains(cblock)
    }
}

//---------------

/// Selects every block
struct AllSelector {
    nr_blocks: u32,
}

impl WritebackSelector for AllSelector {
    fn get_nr_to_writeback(&self) -> u32 {
        self.nr_blocks
    }

    fn needs_writeback(&self, _cblock: u32, _m: &Mapping) -> bool {
        true
    }
}

//---------------

struct MappingCounter<F: Fn(&Mapping) -> bool> {
    count: AtomicU64,
    predicate: F,
}

impl<F: Fn(&Mapping) -> bool> MappingCounter<F> {
    fn new(predicate: F) -> Self {
        Self {
            count: AtomicU64::default(),
            predicate,
        }
    }
}

impl<F: Fn(&Mapping) -> bool> ArrayVisitor<Mapping> for MappingCounter<F> {
    fn visit(&self, _index: u64, b: ArrayBlock<Mapping>) -> array::Result<()> {
        for m in b.values {
            if m.is_valid() && (self.predicate)(&m) {
                self.count.fetch_add(1, Ordering::SeqCst);
            }
        }

        Ok(())
    }
}

/// Counts the valid mappings in the metadata that match the given
/// predicate.
fn count_mappings<F: Fn(&Mapping) -> bool>(
    engine: &Arc<dyn IoEngine + Sync + Send>,
    sb: &Superblock,
    predicate: F,
) -> anyhow::Result<u32> {
    let w = ArrayWalker::new(engine.clone(), true);
    let v = MappingCounter::new(predicate);
    w.walk(&v, sb.mapping_root)?;
    Ok(v.count.load(Ordering::SeqCst) as u32)
}

//---------------

fn v2_read_dirty_bitset(
    engine: Arc<dyn IoEngine + Sync + Send>,
    sb: &Superblock,
) -> anyhow::Result<(u32, RoaringBitmap)> {
    let (cbits, err) = read_bitset(
        engine,
        sb.dirty_root.unwrap(),
        sb.cache_blocks as usize,
        true,
    );
    if err.is_some() {
        return Err(anyhow!("metadata errors present"));
    }

    let mut dirty = RoaringBitmap::new();
    let mut nr_blocks = 0;
    for i in 0..cbits.len() {
        if cbits.contains(i).unwrap_or(false) {
            nr_blocks += 1;
            dirty.insert(i as u32);
        }
    }

    Ok((nr_blocks, dirty))
}

//---------------

/// Build a selector object for this metadata.
fn mk_selector(
    engine: Arc<dyn IoEngine + Sync + Send>,
    sb: &Superblock,
) -> anyhow::Result<Box<dyn WritebackSelector>> {
    if sb.flags.clean_shutdown {
        match sb.version {
            1 => Ok(Box::new(V1Selector {
                nr_blocks: count_mappings(&engine, sb, |m| m.is_dirty())?,
            })),
            2 => {
                let (nr_blocks, dirty) = v2_read_dirty_bitset(engine, sb)?;
                Ok(Box::new(V2Selector { nr_blocks, dirty }))
            }
            _ => {
                // This should have already been checked.
                panic!("bad metadata version");
            }
        }
    } else {
        // assume everything is dirty
        Ok(Box::new(AllSelector {
            nr_blocks: count_mappings(&engine, sb, |_| true)?,
        }))
    }
}

//------------------------------------------

// This builds vectors of copy operations and posts them into a channel.
// The larger the batch size the more chance we have of finding and
// aggregating adjacent copies.  It also keeps track of the array block
// _indices_ (not blocknr) that contained dirty mappings.
struct CopyOpBatcher_ {
    batch_size: usize,
    ops: Vec<CopyOp>,
    tx: SyncSender<Vec<CopyOp>>,
}

impl CopyOpBatcher_ {
    fn new(batch_size: usize, tx: SyncSender<Vec<CopyOp>>) -> Self {
        Self {
            batch_size,
            ops: Vec::with_capacity(batch_size),
            tx,
        }
    }

    /// Append a CopyOp to the current batch.
    fn push(&mut self, op: CopyOp) -> anyhow::Result<()> {
        self.ops.push(op);
        if self.ops.len() >= self.batch_size {
            self.send_ops()?;
        }

        Ok(())
    }

    /// Send the current batch.
    fn send_ops(&mut self) -> anyhow::Result<()> {
        let mut ops = std::mem::take(&mut self.ops);

        // We sort by the destination since this is likely to be a spindle
        // and keeping the io in sequential order will help performance.
        ops.sort_by(|lhs, rhs| lhs.dst.cmp(&rhs.dst));
        self.tx
            .send(ops)
            .map_err(|_| anyhow!("unable to send copy op array"))
    }

    /// Send the current batch and return the dirty ablocks bitmap.
    fn complete(&mut self) -> anyhow::Result<()> {
        self.send_ops()?;
        Ok(())
    }
}

struct CopyOpBatcher {
    inner: Mutex<CopyOpBatcher_>,
    selector: Box<dyn WritebackSelector>,
}

impl CopyOpBatcher {
    fn new(
        batch_size: usize,
        tx: SyncSender<Vec<CopyOp>>,
        selector: Box<dyn WritebackSelector>,
    ) -> Self {
        Self {
            inner: Mutex::new(CopyOpBatcher_::new(batch_size, tx)),
            selector,
        }
    }

    fn complete(self) -> anyhow::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.complete()
    }
}

impl ArrayVisitor<Mapping> for CopyOpBatcher {
    fn visit(&self, index: u64, b: ArrayBlock<Mapping>) -> array::Result<()> {
        let mut inner = self.inner.lock().unwrap();

        let cbegin = index as u32 * b.header.max_entries;
        let cend = cbegin + b.header.nr_entries;
        for (m, cblock) in b.values.into_iter().zip(cbegin..cend) {
            if m.is_valid() && self.selector.needs_writeback(cblock, &m) {
                let src = cblock as u64;
                let dst = m.oblock;
                inner
                    .push(CopyOp { src, dst })
                    .map_err(|_| ArrayError::ValueError("push CopyOp failed".to_string()))?;
            }
        }

        Ok(())
    }
}

//------------------------------------------

const BITS_PER_WORD: usize = 64;

struct BitBlock {
    bitmap_index: usize,
    bit_begin: usize,
    block: ArrayBlock<u64>,
}

/// Cursor for updating the on disk dirty bitmap.
struct BitsetUpdater {
    engine: Arc<dyn IoEngine + Sync + Send>,
    bitmaps: Vec<u64>,
    current_block: Option<BitBlock>,
    last_b: Option<usize>,
}

impl BitsetUpdater {
    fn new(engine: Arc<dyn IoEngine + Sync + Send>, root: u64) -> anyhow::Result<Self> {
        let mut path = Vec::new();
        let bitmaps_map = btree_to_map::<u64>(&mut path, engine.clone(), true, root)?;

        let mut bitmaps = Vec::with_capacity(bitmaps_map.len());
        for (i, (k, v)) in bitmaps_map.iter().enumerate() {
            if i != *k as usize {
                // metadata corruption
                return Err(anyhow!("missing bitmap index"));
            }
            bitmaps.push(*v);
        }

        Ok(Self {
            engine,
            bitmaps,
            current_block: None,
            last_b: None,
        })
    }

    fn read_bits(&self, bitmap_index: usize, bit_begin: usize) -> anyhow::Result<BitBlock> {
        let b = self.engine.read(self.bitmaps[bitmap_index])?;
        let path = Vec::new();
        let block = unpack_array_block::<u64>(&path, b.get_data())?;

        Ok(BitBlock {
            bitmap_index,
            bit_begin,
            block,
        })
    }

    fn write_bits(&mut self, bb: BitBlock) -> anyhow::Result<()> {
        let b = io_engine::Block::new(self.bitmaps[(bb.bitmap_index as u64) as usize]);
        let mut cursor = Cursor::new(b.get_data());
        pack_array_block(&bb.block, &mut cursor)?;
        checksum::write_checksum(b.get_data(), checksum::BT::ARRAY)?;

        self.engine.write(&b)?;
        Ok(())
    }

    fn next_bitmap(&mut self) -> anyhow::Result<()> {
        if let Some(bb) = self.current_block.take() {
            let next_index = bb.bitmap_index + 1;
            let bit_begin = bb.bit_begin + bb.block.values.len() * BITS_PER_WORD;
            self.write_bits(bb)?;
            self.current_block = Some(self.read_bits(next_index, bit_begin)?);
        } else {
            self.current_block = Some(self.read_bits(0, 0)?);
        }

        Ok(())
    }

    fn get_bitmap(&mut self, bit: usize) -> anyhow::Result<()> {
        loop {
            if let Some(bb) = &self.current_block {
                if bb.bit_begin <= bit
                    && (bb.bit_begin + bb.block.values.len() * BITS_PER_WORD > bit)
                {
                    break;
                }
            }
            self.next_bitmap()?;
        }

        Ok(())
    }

    // We don't know for sure how many bits are in each bitmap.
    // So we'll have to read all the bitmaps to be sure.  'b' should
    // be monotonically increasing.
    fn set_bit(&mut self, b: usize, v: bool) -> anyhow::Result<()> {
        // enforce monoticity
        if let Some(last) = self.last_b {
            assert!(b >= last);
        }

        self.get_bitmap(b)?;
        self.last_b = Some(b);
        if let Some(block) = &mut self.current_block {
            assert_eq!(block.bit_begin % BITS_PER_WORD, 0);
            let word = (b - block.bit_begin) / BITS_PER_WORD;
            let bit = b % BITS_PER_WORD;

            if v {
                block.block.values[word as usize] |= 1 << bit;
            } else {
                block.block.values[word as usize] &= !(1 << bit);
            }
        } else {
            panic!("no current block (can't happen)");
        }

        Ok(())
    }

    /// Updates to the most recent metadata block are cached in memory.
    /// You need to call this when you finish setting bits to ensure /
    //this cache is flushed.
    fn flush(&mut self) -> anyhow::Result<()> {
        if let Some(block) = self.current_block.take() {
            self.write_bits(block)?;
        }
        Ok(())
    }
}

impl Drop for BitsetUpdater {
    fn drop(&mut self) {
        self.flush().expect("couldn't write bitset");
    }
}

//------------------------------------------

/// Clears dirty flags in the metadata.
fn update_metadata(
    ctx: &Context,
    sb: &Superblock,
    cleaned_blocks: &RoaringBitmap,
) -> anyhow::Result<()> {
    ctx.report.set_title("Updating metadata");
    match sb.version {
        1 => update_v1_metadata(ctx, sb, cleaned_blocks),
        2 => update_v2_metadata(ctx, sb, cleaned_blocks),
        v => Err(anyhow!("unsupported metadata version: {}", v)),
    }
}

fn update_v1_metadata(
    ctx: &Context,
    sb: &Superblock,
    cleaned_blocks: &RoaringBitmap,
) -> anyhow::Result<()> {
    let mut path = Vec::new();
    let ablocks = btree_to_value_vec::<u64>(&mut path, ctx.engine.clone(), true, sb.mapping_root)?;
    let max_entries = array::calc_max_entries::<Mapping>();
    path.clear();

    let mut index = 0;
    let mut b = ctx.engine.read(ablocks[index])?;
    let mut ablock = unpack_array_block::<Mapping>(&path, b.get_data())?;
    let mut needs_update = false;

    for cblock in cleaned_blocks.iter() {
        if cblock as usize / max_entries as usize != index {
            if needs_update {
                // update array block
                let mut cursor = Cursor::new(b.get_data());
                pack_array_block(&ablock, &mut cursor)?;
                checksum::write_checksum(b.get_data(), checksum::BT::ARRAY)?;
                ctx.engine.write(&b)?;
            }

            // move on to the new array block
            index = cblock as usize / max_entries as usize;
            b = ctx.engine.read(ablocks[index])?;
            ablock = unpack_array_block::<Mapping>(&path, b.get_data())?;
        }

        ablock.values[cblock as usize % max_entries].set_dirty(false);
        needs_update = true;
    }

    if needs_update {
        // update array block
        let mut cursor = Cursor::new(b.get_data());
        pack_array_block(&ablock, &mut cursor)?;
        checksum::write_checksum(b.get_data(), checksum::BT::ARRAY)?;
        ctx.engine.write(&b)?;
    }

    Ok(())
}

fn update_v2_metadata(
    ctx: &Context,
    sb: &Superblock,
    cleaned_blocks: &RoaringBitmap,
) -> anyhow::Result<()> {
    let mut updater = BitsetUpdater::new(
        ctx.engine.clone(),
        sb.dirty_root.expect("dirty bitset unavailable"),
    )?;

    for b in cleaned_blocks {
        updater.set_bit(b as usize, false)?;
    }
    updater.flush()?;

    Ok(())
}

//------------------------------------------

pub struct CacheWritebackOptions<'a> {
    pub metadata_dev: &'a Path,
    pub engine_opts: EngineOptions,
    pub origin_dev: &'a Path,
    pub fast_dev: &'a Path,
    pub origin_dev_offset: Option<u64>, // sectors
    pub fast_dev_offset: Option<u64>,   // sectors
    pub buffer_size: Option<usize>,     // sectors
    pub list_failed_blocks: bool,
    pub update_metadata: bool,
    pub report: Arc<Report>,
}

struct Context {
    report: Arc<Report>,
    engine: Arc<dyn IoEngine + Send + Sync>,
}

fn mk_context(opts: &CacheWritebackOptions) -> anyhow::Result<Context> {
    let engine = EngineBuilder::new(opts.metadata_dev, &opts.engine_opts)
        .write(true)
        .build()?;

    Ok(Context {
        report: opts.report.clone(),
        engine,
    })
}

fn copy_dirty_blocks(
    ctx: &Context,
    sb: &Superblock,
    opts: &CacheWritebackOptions,
) -> anyhow::Result<(WritebackStats, RoaringBitmap)> {
    let block_size = sb.data_block_size * 512;

    // Prepare the copier
    let mut copier = SyncCopier::<VectoredBlockIo<File>>::from_path(
        opts.buffer_size.unwrap_or(128 * 1024) * 512,
        block_size as usize,
        opts.fast_dev,
        opts.origin_dev,
    )?
    .src_offset(opts.fast_dev_offset.unwrap_or(0) * 512)?
    .dest_offset(opts.origin_dev_offset.unwrap_or(0) * 512)?;

    // We pass work to the copy thread via a sync channel with a limit
    // of a single entry, this allows us to prepare one vector of copy ops
    // in advance, but no more.
    let (tx, rx) = mpsc::sync_channel::<Vec<CopyOp>>(1);

    // launch the copy thread
    let selector = mk_selector(ctx.engine.clone(), sb)?;
    let nr_blocks = selector.get_nr_to_writeback();
    let progress = Arc::new(ProgressReporter::new(ctx.report.clone(), nr_blocks as u64));
    let copy_thread = {
        let progress = progress.clone();
        thread::spawn(move || {
            let mut cleaned = RoaringBitmap::new();

            while let Ok(ops) = rx.recv() {
                {
                    // We assume the copies will succeed, and then remove
                    // entries that failed afterwards.
                    for op in &ops {
                        cleaned.insert(op.src as u32);
                    }
                }

                let stats = copier.copy(&ops, progress.clone()).expect("copy failed");
                progress.inc_stats(&stats);

                {
                    for op in stats.read_errors {
                        cleaned.remove(op.src as u32);
                    }

                    for op in stats.write_errors {
                        cleaned.remove(op.src as u32);
                    }
                }
            }

            cleaned
        })
    };

    // Build batches of copy operations and pass them to the copy thread
    ctx.report.set_title("Copying cache blocks");
    let batcher = CopyOpBatcher::new(1_000_000, tx, selector);
    let w = ArrayWalker::new(ctx.engine.clone(), true);

    // FIXME: do something with this
    let _walk_err = w.walk(&batcher, sb.mapping_root);

    batcher.complete()?;
    let cleaned = copy_thread.join().unwrap();

    Ok((progress.stats(), cleaned))
}

fn report_stats(report: Arc<Report>, stats: &WritebackStats) {
    report.info(&format!(
        "{}/{} blocks successfully copied",
        stats.nr_copied, stats.nr_blocks
    ));

    let nr_errors = stats.nr_read_errors + stats.nr_write_errors;
    if nr_errors > 0 {
        report.info(&format!("{} blocks were not copied", nr_errors));
    }
}

pub fn writeback(opts: CacheWritebackOptions) -> anyhow::Result<()> {
    let ctx = mk_context(&opts)?;
    let sb = read_superblock(ctx.engine.as_ref(), SUPERBLOCK_LOCATION)?;

    if sb.version > 2 {
        return Err(anyhow!("unsupported metadata version: {}", sb.version));
    }

    // must be a multiple of page size because we use O_DIRECT
    if !is_page_aligned(opts.fast_dev_offset.unwrap_or(0) * 512)
        || !is_page_aligned(opts.origin_dev_offset.unwrap_or(0) * 512)
    {
        return Err(anyhow!("offsets must be page aligned"));
    }

    match copy_dirty_blocks(&ctx, &sb, &opts) {
        Err(_) => {
            ctx.report
                .info("Metadata corruption was found, some data may not have been copied.");
            if opts.update_metadata {
                ctx.report.info("Unable to update metadata");
            }
            return Err(anyhow!("Metadata contains errors"));
        }
        Ok((stats, cleaned)) => {
            report_stats(ctx.report.clone(), &stats);
            if opts.update_metadata {
                update_metadata(&ctx, &sb, &cleaned)?;
            }

            if stats.nr_copied != stats.nr_blocks {
                return Err(anyhow!("Incompleted writeback"));
            }
        }
    }

    Ok(())
}

//------------------------------------------
