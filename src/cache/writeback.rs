use anyhow::anyhow;
use fixedbitset::FixedBitSet;
use std::io::{self, Cursor};
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::cache::mapping::*;
use crate::cache::superblock::*;
use crate::checksum;
use crate::copier::*;
use crate::io_engine::{AsyncIoEngine, IoEngine, SyncIoEngine};
use crate::pdata::array::{self, *};
use crate::pdata::array_builder::pack_array_block;
use crate::pdata::array_walker::*;
use crate::pdata::bitset::{read_bitset, CheckedBitSet};
use crate::pdata::btree_walker::btree_to_map;
use crate::pdata::space_map_common::SMRoot;
use crate::pdata::unpack::unpack;
use crate::report::Report;

//-----------------------------------------

const MAX_CONCURRENT_IO: u32 = 1024;

//-----------------------------------------

struct CopyStats {
    blocks_scanned: u32, // scanned indices
    blocks_needed: u32,  // blocks to copy
    blocks_issued: u32,
    blocks_completed: u32,
    blocks_failed: u32,
}

impl CopyStats {
    fn new() -> Self {
        CopyStats {
            blocks_scanned: 0,
            blocks_needed: 0,
            blocks_issued: 0,
            blocks_completed: 0,
            blocks_failed: 0,
        }
    }
}

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

struct CVInner {
    copier: Copier,
    indicator: DirtyIndicator,
    stats: CopyStats,
    dirty_ablocks: FixedBitSet,  // array blocks containing dirty mappings
    cleaned_blocks: FixedBitSet, // cache blocks that had been writeback successfully
}

struct CopyVisitor {
    inner: Mutex<CVInner>,
    only_dirty: bool,
}

impl CopyVisitor {
    fn new(
        c: Copier,
        indicator: DirtyIndicator,
        only_dirty: bool,
        nr_metadata_blocks: u64,
        nr_cache_blocks: u32,
    ) -> Self {
        CopyVisitor {
            inner: Mutex::new(CVInner {
                copier: c,
                indicator,
                stats: CopyStats::new(),
                dirty_ablocks: FixedBitSet::with_capacity(nr_metadata_blocks as usize),
                cleaned_blocks: FixedBitSet::with_capacity(nr_cache_blocks as usize),
            }),
            only_dirty,
        }
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
            }
        }

        let inner = self.inner.into_inner().unwrap();
        Ok((inner.stats, inner.dirty_ablocks, inner.cleaned_blocks))
    }
}

// TODO: Avoid visiting clean mappings in v2 metadata.
//       The CopyVisitor should not read an mapping array block,
//       if all the mappings in this block are clean.
impl ArrayVisitor<Mapping> for CopyVisitor {
    fn visit(&self, index: u64, b: ArrayBlock<Mapping>) -> array::Result<()> {
        let mut inner = self.inner.lock().unwrap();

        let prev_issued = inner.stats.blocks_issued;
        let cbegin = index as u32 * b.header.max_entries;
        let cend = cbegin + b.header.max_entries;
        for (m, cblock) in b.values.iter().zip(cbegin..cend) {
            inner.stats.blocks_scanned = cblock;

            // skip unused or clean blocks
            if !m.is_valid() || (self.only_dirty && !inner.indicator.is_dirty(cblock, m)) {
                continue;
            }

            inner.stats.blocks_needed += 1;

            while inner.copier.nr_pending() >= inner.copier.queue_depth() as usize {
                // TODO: better error handling, rather than panic
                Self::wait_completion(&mut inner).expect("internal error");
            }

            let cop = CopyOp::new(cblock as u64, m.oblock);
            inner.copier.issue(cop).expect("internal error");

            inner.stats.blocks_issued += 1;
        }

        if inner.stats.blocks_issued > prev_issued {
            inner.dirty_ablocks.set(b.header.blocknr as usize, true);
        }

        // TODO: update progress bar

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
        let cend = cbegin + ablock.header.max_entries;
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
    let engine: Arc<dyn IoEngine + Send + Sync>;

    if opts.async_io {
        engine = Arc::new(AsyncIoEngine::new(
            opts.metadata_dev,
            MAX_CONCURRENT_IO,
            opts.update_metadata,
        )?);
    } else {
        let nr_threads = std::cmp::max(8, num_cpus::get() * 2);
        engine = Arc::new(SyncIoEngine::new(
            opts.metadata_dev,
            nr_threads,
            opts.update_metadata,
        )?);
    }

    Ok(Context {
        report: opts.report.clone(),
        engine,
    })
}

fn calc_queue_depth(buffer_size: Option<usize>, data_block_size: u32) -> anyhow::Result<u32> {
    let queue_depth = if let Some(buf_size) = buffer_size {
        if buf_size > data_block_size as usize {
            let qd = buf_size / data_block_size as usize;
            if qd > u32::MAX as usize {
                return Err(anyhow!("buffer size exceeds limit"));
            }
            qd as u32
        } else {
            1
        }
    } else {
        // default up to 1GiB buffer, or up to 256 queue depth
        std::cmp::max(2097152 / data_block_size, 256)
    };
    Ok(queue_depth)
}

fn copy_dirty_blocks(
    ctx: &Context,
    sb: &Superblock,
    opts: &CacheWritebackOptions,
) -> anyhow::Result<(bool, CopyStats, FixedBitSet, FixedBitSet)> {
    if sb.version > 2 {
        return Err(anyhow!("unsupported metadata version: {}", sb.version));
    }

    // TODO: handle large data block size
    let queue_depth = calc_queue_depth(opts.buffer_size, sb.data_block_size)?;

    let copier = Copier::new(
        opts.fast_dev,
        opts.origin_dev,
        sb.data_block_size,
        queue_depth as u32,
        opts.fast_dev_offset.unwrap_or(0),
        opts.origin_dev_offset.unwrap_or(0),
    )?;

    let nr_metadata_blocks = unpack::<SMRoot>(&sb.metadata_sm_root[0..])?.nr_blocks;
    let indicator = DirtyIndicator::new(ctx.engine.clone(), sb);
    let mut cv = CopyVisitor::new(
        copier,
        indicator,
        sb.flags.clean_shutdown,
        nr_metadata_blocks,
        sb.cache_blocks,
    );
    let w = ArrayWalker::new(ctx.engine.clone(), true);
    let err = w.walk(&mut cv, sb.mapping_root).is_err();
    let (stats, dirty_ablocks, cleaned_blocks) = cv.complete()?;

    Ok((err, stats, dirty_ablocks, cleaned_blocks))
}

fn report_stats(report: Arc<Report>, stats: &CopyStats) {
    let blocks_copied = stats.blocks_completed - stats.blocks_failed;
    report.info(&format!(
        "{}/{} blocks successfully copied",
        blocks_copied, stats.blocks_issued
    ));
    if stats.blocks_failed > 0 {
        report.info(&format!("{} blocks were not copied", stats.blocks_failed));
    }
}

pub fn writeback(opts: CacheWritebackOptions) -> anyhow::Result<()> {
    let ctx = mk_context(&opts)?;
    let sb = read_superblock(ctx.engine.as_ref(), SUPERBLOCK_LOCATION)?;

    let (corrupted, stats, dirty_ablocks, cleaned_blocks) = copy_dirty_blocks(&ctx, &sb, &opts)?;
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

    if stats.blocks_completed != stats.blocks_issued || stats.blocks_failed > 0 {
        return Err(anyhow!("Incompleted writeback"));
    }

    Ok(())
}

//------------------------------------------
