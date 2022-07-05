use anyhow::anyhow;
use fixedbitset::FixedBitSet;
use roaring::RoaringBitmap;
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
use crate::io_engine::*;
use crate::pdata::array::{self, *};
use crate::pdata::array_walker::*;
use crate::pdata::bitset::read_bitset;
use crate::pdata::btree_walker::btree_to_map;
use crate::pdata::space_map::common::SMRoot;
use crate::pdata::unpack::unpack;
use crate::report::Report;

//-----------------------------------------

struct ProgressReporter_ {
    nr_blocks: u64,
    nr_copied: u64,
    nr_read_errors: u64,
    nr_write_errors: u64,
}

struct ProgressReporter {
    report: Arc<Report>,
    inner: Mutex<ProgressReporter_>,
}

impl ProgressReporter {
    fn new(report: Arc<Report>, nr_blocks: u64) -> Self {
        Self {
            report,
            inner: Mutex::new(ProgressReporter_ {
                nr_blocks,
                nr_copied: 0,
                nr_read_errors: 0,
                nr_write_errors: 0,
            }),
        }
    }

    fn update_stats(&self, stats: &CopyStats) {
        let mut inner = self.inner.lock().unwrap();
        inner.nr_copied += stats.nr_copied.load(Ordering::Relaxed);
        inner.nr_read_errors += stats.read_errors.len() as u64;
        inner.nr_write_errors += stats.write_errors.len() as u64;

        self.report.set_sub_title(&format!(
            "read errors {}, write errors {}",
            inner.nr_read_errors, inner.nr_write_errors
        ));

        let percent = (inner.nr_copied * 100) / inner.nr_blocks;
        self.report.progress(percent as u8);
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
    fn needs_writeback(&self, cblock: u32, m: &Mapping) -> bool;
}

//---------------

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

#[derive(Default)]
struct V1DirtyCounter {
    count: AtomicU64,
}

impl ArrayVisitor<Mapping> for V1DirtyCounter {
    fn visit(&self, _index: u64, b: ArrayBlock<Mapping>) -> array::Result<()> {
        for m in b.values {
            if m.is_valid() && m.is_dirty() {
                self.count.fetch_add(1, Ordering::SeqCst);
            }
        }

        Ok(())
    }
}

fn v1_count_dirty(
    engine: &Arc<dyn IoEngine + Sync + Send>,
    sb: &Superblock,
) -> anyhow::Result<u32> {
    let w = ArrayWalker::new(engine.clone(), true);
    let v = V1DirtyCounter::default();
    w.walk(&v, sb.mapping_root)?;
    Ok(v.count.load(Ordering::SeqCst) as u32)
}

fn v2_read_bitset(
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
        if cbits.contains(i).unwrap_or(true) {
            nr_blocks += 1;
            dirty.insert(i as u32);
        }
    }

    Ok((nr_blocks, dirty))
}

fn mk_selector(
    engine: Arc<dyn IoEngine + Sync + Send>,
    sb: &Superblock,
) -> anyhow::Result<Box<dyn WritebackSelector>> {
    if sb.flags.clean_shutdown {
        match sb.version {
            1 => Ok(Box::new(V1Selector {
                nr_blocks: v1_count_dirty(&engine, sb)?,
            })),
            2 => {
                let (nr_blocks, dirty) = v2_read_bitset(engine, sb)?;
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
            nr_blocks: sb.cache_blocks,
        }))
    }
}

//------------------------------------------

// This builds vectors of copy operations and posts them into a channel.
// The larger the batch size the more chance we have of finding and
// aggregating adjacent copies.
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

    fn push(&mut self, op: CopyOp) -> anyhow::Result<()> {
        self.ops.push(op);
        if self.ops.len() >= self.batch_size {
            self.send_ops()?;
        }

        Ok(())
    }

    fn send_ops(&mut self) -> anyhow::Result<()> {
        let mut ops = Vec::new();
        std::mem::swap(&mut ops, &mut self.ops);

        // We sort by the destination since this is likely to be a spindle
        // and keeping the io in sequential order will help performance.
        ops.sort_by(|lhs, rhs| lhs.dst.cmp(&rhs.dst));
        self.tx
            .send(ops)
            .map_err(|_| anyhow!("unable to send copy op array"))
    }

    fn complete(&mut self) -> anyhow::Result<()> {
        self.send_ops()
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
/*
fn update_metadata(
    ctx: &Context,
    sb: &Superblock,
    dirty_ablocks: &FixedBitSet,
    cleaned_blocks: &FixedBitSet,
) -> anyhow::Result<()> {
    match sb.version {
        1 => update_v1_metadata(ctx, sb, dirty_ablocks, cleaned_blocks),
        2 => update_v2_metadata(ctx, sb, cleaned_blocks),
        v => Err(anyhow!("unsupported metadata version: {}", v)),
    }
}

fn update_v1_metadata(
    ctx: &Context,
    sb: &Superblock,
    dirty_ablocks: &FixedBitSet,
    cleaned_blocks: &FixedBitSet,
) -> anyhow::Result<()> {
    ctx.report.set_title("Updating metadata");

    let mut path = Vec::new();
    let ablocks = btree_to_map::<u64>(&mut path, ctx.engine.clone(), true, sb.mapping_root)?;

    path.clear();
    for (index, blocknr) in ablocks.iter() {
        if !dirty_ablocks.contains(*blocknr as usize) {
            continue;
        }

        let b = ctx.engine.read(*blocknr)?;
        let mut ablock = unpack_array_block::<Mapping>(&path, b.get_data())?;

        let mut needs_update = false;
        let cbegin = *index as u32 * ablock.header.max_entries;
        let cend = cbegin + ablock.header.nr_entries;
        for (m, cblock) in ablock.values.iter_mut().zip(cbegin..cend) {
            if !cleaned_blocks.contains(cblock as usize) {
                continue;
            }
            m.set_dirty(false);
            needs_update = true;
        }

        // no update if no successful writebacks
        if !needs_update {
            continue;
        }

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
    cleaned_blocks: &FixedBitSet,
) -> anyhow::Result<()> {
    let mut path = Vec::new();
    let bitmaps = btree_to_map::<u64>(
        &mut path,
        ctx.engine.clone(),
        true,
        sb.dirty_root.expect("dirty bitset unavailable"),
    )?;

    path.clear();
    for blocknr in bitmaps.values() {
        let b = ctx.engine.read(*blocknr)?;
        let mut bitmap = unpack_array_block::<u64>(&path, b.get_data())?;

        let mut needs_update = false;
        for (bits, c) in bitmap
            .values
            .iter_mut()
            .zip(cleaned_blocks.as_slice().chunks(2))
        {
            if c[0] == 0 && c[1] == 0 {
                continue;
            }
            *bits &= !((c[1] as u64) << 32 | (c[0] as u64));
            needs_update = true;
        }

        if !needs_update {
            continue;
        }

        let mut cursor = Cursor::new(b.get_data());
        pack_array_block(&bitmap, &mut cursor)?;
        checksum::write_checksum(b.get_data(), checksum::BT::ARRAY)?;

        ctx.engine.write(&b)?;
    }
    Ok(())
}
*/
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
) -> anyhow::Result<()> {
    // Prepare the copier
    let mut copier = SyncCopier::new(
        opts.buffer_size.unwrap_or(128 * 1024) * 512,
        (sb.data_block_size * 512) as usize,
        opts.fast_dev,
        opts.fast_dev_offset.unwrap_or(0) * 512,
        opts.origin_dev,
        opts.origin_dev_offset.unwrap_or(0) * 512,
    )?;

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
        thread::spawn(move || loop {
            if let Ok(ops) = rx.recv() {
                let stats = copier.copy(&ops, progress.clone()).expect("copy failed");
                progress.update_stats(&stats);
            } else {
                break;
            }
        })
    };

    // Build batches of copy operations and pass them to the copy thread
    ctx.report.set_title("Copying cache blocks");
    let batcher = CopyOpBatcher::new(1_000_000, tx, selector);
    let w = ArrayWalker::new(ctx.engine.clone(), true);
    let walk_err = w.walk(&batcher, sb.mapping_root);
    batcher.complete()?;
    copy_thread.join().unwrap();

    Ok(())
}

/*
fn report_stats(report: Arc<Report>, stats: &CopyStats) {
    let blocks_copied = stats.blocks_completed - stats.blocks_failed;
    report.info(&format!(
        "{}/{} blocks successfully copied",
        blocks_copied, stats.blocks_needed
    ));
    if stats.blocks_failed > 0 {
        report.info(&format!("{} blocks were not copied", stats.blocks_failed));
    }
}
*/

pub fn writeback(opts: CacheWritebackOptions) -> anyhow::Result<()> {
    let ctx = mk_context(&opts)?;
    let sb = read_superblock(ctx.engine.as_ref(), SUPERBLOCK_LOCATION)?;

    if sb.version > 2 {
        return Err(anyhow!("unsupported metadata version: {}", sb.version));
    }

    let corrupted = copy_dirty_blocks(&ctx, &sb, &opts)?;
    // report_stats(ctx.report.clone(), &stats);

    /*
    if corrupted {
        ctx.report
            .info("Metadata corruption was found, some data may not have been copied.");
        if opts.update_metadata {
            ctx.report.info("Unable to update metadata");
        }
        return Err(anyhow!("Metadata contains errors"));
    }

    if opts.update_metadata {
        update_metadata(&ctx, &sb, &dirty_ablocks, &cleaned_blocks)?;
    }

    if stats.blocks_completed != stats.blocks_needed || stats.blocks_failed > 0 {
        return Err(anyhow!("Incompleted writeback"));
    }
    */

    Ok(())
}

//------------------------------------------
