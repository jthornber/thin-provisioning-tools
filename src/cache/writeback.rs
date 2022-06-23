use anyhow::anyhow;
use fixedbitset::FixedBitSet;
use std::io::{self, Cursor};
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use threadpool::ThreadPool;

use crate::async_copier::*;
use crate::cache::mapping::*;
use crate::cache::superblock::*;
use crate::checksum;
use crate::io_engine::{AsyncIoEngine, IoEngine, SyncIoEngine, SECTOR_SHIFT};
use crate::pdata::array::{self, *};
use crate::pdata::array_walker::*;
use crate::pdata::bitset::{read_bitset, CheckedBitSet};
use crate::pdata::btree_walker::btree_to_map;
use crate::pdata::space_map::common::SMRoot;
use crate::pdata::unpack::unpack;
use crate::report::{ProgressMonitor, Report};
use crate::sync_copier::SyncCopier;

//-----------------------------------------

const MAX_CONCURRENT_IO: u32 = 1024;

//-----------------------------------------

struct CopyStats {
    blocks_needed: u32, // blocks to copy
    blocks_completed: u32,
    blocks_failed: u32,
}

impl CopyStats {
    fn new() -> Self {
        CopyStats {
            blocks_needed: 0,
            blocks_completed: 0,
            blocks_failed: 0,
        }
    }
}

//-----------------------------------------

struct DirtyIndicator {
    version: u32,
    dirty_bits: CheckedBitSet,
}

impl DirtyIndicator {
    fn new(engine: Arc<dyn IoEngine + Send + Sync>, sb: &Superblock) -> Self {
        if sb.version == 1 {
            return DirtyIndicator {
                version: 1,
                dirty_bits: CheckedBitSet::with_capacity(0),
            };
        }

        let (dirty_bits, _) = read_bitset(
            engine,
            sb.dirty_root.unwrap(),
            sb.cache_blocks as usize,
            true,
        );

        DirtyIndicator {
            version: sb.version,
            dirty_bits,
        }
    }

    fn is_dirty(&self, cblock: u32, m: &Mapping) -> bool {
        if self.version == 1 {
            m.is_dirty()
        } else {
            self.dirty_bits.contains(cblock as usize).unwrap_or(true)
        }
    }
}

//-----------------------------------------

struct CVInner {
    copier: AsyncCopier,
    indicator: DirtyIndicator,
    stats: CopyStats,
    dirty_ablocks: FixedBitSet,  // array blocks containing dirty mappings
    cleaned_blocks: FixedBitSet, // cache blocks that had been writeback successfully
}

struct AsyncCopyVisitor {
    inner: Mutex<CVInner>,
    only_dirty: bool,
    progress: Arc<AtomicU64>,
}

impl AsyncCopyVisitor {
    fn new(
        c: AsyncCopier,
        indicator: DirtyIndicator,
        only_dirty: bool,
        nr_metadata_blocks: u64,
        nr_cache_blocks: u32,
    ) -> Self {
        AsyncCopyVisitor {
            inner: Mutex::new(CVInner {
                copier: c,
                indicator,
                stats: CopyStats::new(),
                dirty_ablocks: FixedBitSet::with_capacity(nr_metadata_blocks as usize),
                cleaned_blocks: FixedBitSet::with_capacity(nr_cache_blocks as usize),
            }),
            only_dirty,
            progress: Arc::new(AtomicU64::new(0)),
        }
    }

    fn get_progress(&self) -> Arc<AtomicU64> {
        self.progress.clone()
    }

    fn wait_completion(inner: &mut CVInner) -> io::Result<()> {
        let mut last_err = None;
        let rets = inner.copier.wait()?;
        for r in rets {
            inner.stats.blocks_completed += 1;
            match r {
                Ok(op) => {
                    inner.cleaned_blocks.set(op.src_b as usize, true);
                }
                Err(IoError::ReadError(_, _)) | Err(IoError::WriteError(_, _)) => {
                    inner.stats.blocks_failed += 1;
                }
                Err(e) => last_err = Some(e), // unrecoverable errors, e.g., lost tracking
            }
        }
        if let Some(e) = last_err {
            return Err(io::Error::new(io::ErrorKind::Other, e.to_string()));
        }
        Ok(())
    }

    fn complete(self) -> io::Result<(CopyStats, FixedBitSet, FixedBitSet)> {
        {
            let mut inner = self.inner.lock().unwrap();
            while inner.copier.nr_pending() > 0 {
                Self::wait_completion(&mut inner).expect("internal error");
                self.progress
                    .store(inner.stats.blocks_completed as u64, Ordering::Relaxed);
            }
        }

        let inner = self.inner.into_inner().unwrap();
        Ok((inner.stats, inner.dirty_ablocks, inner.cleaned_blocks))
    }
}

// TODO: Avoid visiting clean mappings in v2 metadata.
//       The AsyncCopyVisitor should not read an mapping array block,
//       if all the mappings in this block are clean.
impl ArrayVisitor<Mapping> for AsyncCopyVisitor {
    fn visit(&self, index: u64, b: ArrayBlock<Mapping>) -> array::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let mut blocks_dirty = 0;

        let cbegin = index as u32 * b.header.max_entries;
        let cend = cbegin + b.header.nr_entries;
        for (m, cblock) in b.values.iter().zip(cbegin..cend) {
            // skip unused or clean blocks
            if !m.is_valid() || (self.only_dirty && !inner.indicator.is_dirty(cblock, m)) {
                continue;
            }

            blocks_dirty += 1;

            while inner.copier.nr_pending() >= inner.copier.queue_depth() as usize {
                // TODO: better error handling, rather than panic
                Self::wait_completion(&mut inner).expect("internal error");
                self.progress
                    .store(inner.stats.blocks_completed as u64, Ordering::Relaxed);
            }

            let cop = CopyOp::new(cblock as u64, m.oblock);
            inner.copier.issue(cop).expect("internal error");
        }

        if blocks_dirty > 0 {
            inner.stats.blocks_needed += blocks_dirty;
            inner.dirty_ablocks.set(b.header.blocknr as usize, true);
        }

        Ok(())
    }
}

//------------------------------------------

struct SVInner {
    stats: CopyStats,
    dirty_ablocks: FixedBitSet, // array blocks containing dirty mappings
}

struct SyncCopyVisitor {
    inner: Mutex<SVInner>,
    cleaned_blocks: Arc<Mutex<FixedBitSet>>, // cache blocks that had been writeback successfully
    copier: Arc<SyncCopier>,
    indicator: DirtyIndicator,
    only_dirty: bool,
    data_block_size: u32, // bytes
    cache_offset: u64,    // bytes
    origin_offset: u64,   // bytes
    pool: ThreadPool,
    progress: Arc<AtomicU64>,
}

impl SyncCopyVisitor {
    fn new(
        c: SyncCopier,
        indicator: DirtyIndicator,
        only_dirty: bool,
        nr_metadata_blocks: u64,
        nr_cache_blocks: u32,
        data_block_size: u32,
        pool: ThreadPool,
    ) -> Self {
        SyncCopyVisitor {
            inner: Mutex::new(SVInner {
                stats: CopyStats::new(),
                dirty_ablocks: FixedBitSet::with_capacity(nr_metadata_blocks as usize),
            }),
            cleaned_blocks: Arc::new(Mutex::new(FixedBitSet::with_capacity(
                nr_cache_blocks as usize,
            ))),
            copier: Arc::new(c),
            indicator,
            only_dirty,
            data_block_size,
            cache_offset: 0,
            origin_offset: 0,
            pool,
            progress: Arc::new(AtomicU64::new(0)),
        }
    }

    fn set_cache_offset(&mut self, cache_offset: u64) {
        self.cache_offset = cache_offset;
    }

    fn set_origin_offset(&mut self, origin_offset: u64) {
        self.origin_offset = origin_offset;
    }

    fn get_progress(&self) -> Arc<AtomicU64> {
        self.progress.clone()
    }

    fn complete(self) -> anyhow::Result<(CopyStats, FixedBitSet, FixedBitSet)> {
        let inner = self.inner.into_inner()?;
        let cleaned_blocks = if let Ok(cb) = Arc::try_unwrap(self.cleaned_blocks) {
            cb.into_inner()?
        } else {
            return Err(anyhow!("Cannot acquire writeback statistics"));
        };
        Ok((inner.stats, inner.dirty_ablocks, cleaned_blocks))
    }
}

impl ArrayVisitor<Mapping> for SyncCopyVisitor {
    fn visit(&self, index: u64, b: ArrayBlock<Mapping>) -> array::Result<()> {
        let mut blocks_dirty = 0;
        let blocks_failed = Arc::new(AtomicU32::new(0));

        let cbegin = index as u32 * b.header.max_entries;
        let cend = cbegin + b.header.nr_entries;
        for (m, cblock) in b.values.into_iter().zip(cbegin..cend) {
            // skip unused or clean blocks
            if !m.is_valid() || (self.only_dirty && !self.indicator.is_dirty(cblock, &m)) {
                continue;
            }

            blocks_dirty += 1;

            let copier = self.copier.clone();
            let bs = self.data_block_size as u64;
            let src = cblock as u64 * bs + self.cache_offset;
            let dest = m.oblock * bs + self.origin_offset;
            let nr_failed = blocks_failed.clone();
            let cleaned = self.cleaned_blocks.clone();
            let progress = self.progress.clone();

            self.pool.execute(move || {
                if copier.copy(src, dest, bs).is_ok() {
                    cleaned.lock().unwrap().set(cblock as usize, true); // TODO: avoid locks
                } else {
                    nr_failed.fetch_add(1, Ordering::Relaxed);
                }
                progress.fetch_add(1, Ordering::Relaxed);
            });
        }

        self.pool.join();

        // update statistics
        let mut inner = self.inner.lock().unwrap();
        if blocks_dirty > 0 {
            inner.dirty_ablocks.set(b.header.blocknr as usize, true);
        }
        inner.stats.blocks_needed += blocks_dirty;
        inner.stats.blocks_completed += blocks_dirty;
        inner.stats.blocks_failed += blocks_failed.load(Ordering::Relaxed);

        Ok(())
    }
}

//------------------------------------------

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

//------------------------------------------

pub struct CacheWritebackOptions<'a> {
    pub metadata_dev: &'a Path,
    pub async_io: bool,
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
    let engine: Arc<dyn IoEngine + Send + Sync> = if opts.async_io {
        Arc::new(AsyncIoEngine::new(
            opts.metadata_dev,
            MAX_CONCURRENT_IO,
            opts.update_metadata,
        )?)
    } else {
        Arc::new(SyncIoEngine::new(
            opts.metadata_dev,
            opts.update_metadata,
        )?)
    };

    Ok(Context {
        report: opts.report.clone(),
        engine,
    })
}

// The output queue depth is limited to a range [1, 256]
fn calc_queue_depth(buffer_size: usize, data_block_size: u32) -> anyhow::Result<u32> {
    let qd = std::cmp::max(buffer_size / data_block_size as usize, 1);
    let qd = std::cmp::min(qd, 256);
    Ok(qd as u32)
}

fn copy_dirty_blocks_async(
    ctx: &Context,
    sb: &Superblock,
    opts: &CacheWritebackOptions,
) -> anyhow::Result<(bool, CopyStats, FixedBitSet, FixedBitSet)> {
    if sb.version > 2 {
        return Err(anyhow!("unsupported metadata version: {}", sb.version));
    }

    // default to 4MB buffer size
    let queue_depth = calc_queue_depth(opts.buffer_size.unwrap_or(8192), sb.data_block_size)?;

    let copier = AsyncCopier::new(
        opts.fast_dev,
        opts.origin_dev,
        sb.data_block_size << SECTOR_SHIFT,
        queue_depth,
        opts.fast_dev_offset.unwrap_or(0) << SECTOR_SHIFT,
        opts.origin_dev_offset.unwrap_or(0) << SECTOR_SHIFT,
    )?;

    let nr_metadata_blocks = unpack::<SMRoot>(&sb.metadata_sm_root[0..])?.nr_blocks;
    let indicator = DirtyIndicator::new(ctx.engine.clone(), sb);
    let cv = AsyncCopyVisitor::new(
        copier,
        indicator,
        sb.flags.clean_shutdown,
        nr_metadata_blocks,
        sb.cache_blocks,
    );

    ctx.report.set_title("Copying cache blocks");
    let monitor = ProgressMonitor::new(
        ctx.report.clone(),
        sb.cache_blocks as u64,
        cv.get_progress(),
    );
    let w = ArrayWalker::new(ctx.engine.clone(), true);
    let err = w.walk(&cv, sb.mapping_root).is_err();

    let (stats, dirty_ablocks, cleaned_blocks) = cv.complete()?;
    monitor.stop();

    Ok((err, stats, dirty_ablocks, cleaned_blocks))
}

fn copy_dirty_blocks_sync(
    ctx: &Context,
    sb: &Superblock,
    opts: &CacheWritebackOptions,
) -> anyhow::Result<(bool, CopyStats, FixedBitSet, FixedBitSet)> {
    if sb.version > 2 {
        return Err(anyhow!("unsupported metadata version: {}", sb.version));
    }

    let copier = SyncCopier::new(opts.fast_dev, opts.origin_dev)?;

    let nr_threads = std::cmp::max(8, num_cpus::get() * 2);
    let pool = ThreadPool::new(nr_threads);

    let nr_metadata_blocks = unpack::<SMRoot>(&sb.metadata_sm_root[0..])?.nr_blocks;
    let indicator = DirtyIndicator::new(ctx.engine.clone(), sb);
    let mut cv = SyncCopyVisitor::new(
        copier,
        indicator,
        sb.flags.clean_shutdown,
        nr_metadata_blocks,
        sb.cache_blocks,
        sb.data_block_size << SECTOR_SHIFT,
        pool,
    );
    cv.set_cache_offset(opts.fast_dev_offset.unwrap_or(0) << SECTOR_SHIFT);
    cv.set_origin_offset(opts.origin_dev_offset.unwrap_or(0) << SECTOR_SHIFT);

    ctx.report.set_title("Copying cache blocks");
    let monitor = ProgressMonitor::new(
        ctx.report.clone(),
        sb.cache_blocks as u64,
        cv.get_progress(),
    );
    let w = ArrayWalker::new(ctx.engine.clone(), true);
    let err = w.walk(&cv, sb.mapping_root).is_err();

    let (stats, dirty_ablocks, cleaned_blocks) = cv.complete()?;
    monitor.stop();

    Ok((err, stats, dirty_ablocks, cleaned_blocks))
}

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

pub fn writeback(opts: CacheWritebackOptions) -> anyhow::Result<()> {
    let ctx = mk_context(&opts)?;
    let sb = read_superblock(ctx.engine.as_ref(), SUPERBLOCK_LOCATION)?;

    let (corrupted, stats, dirty_ablocks, cleaned_blocks) = if opts.async_io {
        copy_dirty_blocks_async(&ctx, &sb, &opts)?
    } else {
        copy_dirty_blocks_sync(&ctx, &sb, &opts)?
    };
    report_stats(ctx.report.clone(), &stats);

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

    Ok(())
}

//------------------------------------------
