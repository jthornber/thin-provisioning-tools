use anyhow::{anyhow, Result};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use threadpool::ThreadPool;

use crate::io_engine::IoEngine;
use crate::pdata::btree::{self, *};
use crate::pdata::btree_walker::*;
use crate::pdata::space_map::*;
use crate::pdata::space_map_checker::*;
use crate::pdata::space_map_common::*;
use crate::pdata::unpack::*;
use crate::report::*;
use crate::thin::block_time::*;
use crate::thin::device_detail::*;
use crate::thin::superblock::*;

//------------------------------------------

struct BottomLevelVisitor {
    data_sm: ASpaceMap,
}

//------------------------------------------

impl NodeVisitor<BlockTime> for BottomLevelVisitor {
    fn visit(
        &self,
        _path: &[u64],
        _kr: &KeyRange,
        _h: &NodeHeader,
        _k: &[u64],
        values: &[BlockTime],
    ) -> btree::Result<()> {
        // FIXME: do other checks

        if values.is_empty() {
            return Ok(());
        }

        let mut data_sm = self.data_sm.lock().unwrap();

        let mut start = values[0].block;
        let mut len = 1;

        for b in values.iter().skip(1) {
            let block = b.block;
            if block == start + len {
                len += 1;
            } else {
                data_sm.inc(start, len).unwrap();
                start = block;
                len = 1;
            }
        }

        data_sm.inc(start, len).unwrap();
        Ok(())
    }

    fn visit_again(&self, _path: &[u64], _b: u64) -> btree::Result<()> {
        Ok(())
    }

    fn end_walk(&self) -> btree::Result<()> {
        Ok(())
    }
}

//------------------------------------------

fn inc_superblock(sm: &ASpaceMap) -> Result<()> {
    let mut sm = sm.lock().unwrap();
    sm.inc(SUPERBLOCK_LOCATION, 1)?;
    Ok(())
}

//------------------------------------------

pub const MAX_CONCURRENT_IO: u32 = 1024;

pub struct ThinCheckOptions {
    pub engine: Arc<dyn IoEngine + Send + Sync>,
    pub sb_only: bool,
    pub skip_mappings: bool,
    pub ignore_non_fatal: bool,
    pub auto_repair: bool,
    pub clear_needs_check: bool,
    pub report: Arc<Report>,
}

fn spawn_progress_thread(
    sm: Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    nr_allocated_metadata: u64,
    report: Arc<Report>,
) -> Result<(JoinHandle<()>, Arc<AtomicBool>)> {
    let tid;
    let stop_progress = Arc::new(AtomicBool::new(false));

    {
        let stop_progress = stop_progress.clone();
        tid = thread::spawn(move || {
            let interval = std::time::Duration::from_millis(250);
            loop {
                if stop_progress.load(Ordering::Relaxed) {
                    break;
                }

                let sm = sm.lock().unwrap();
                let mut n = sm.get_nr_allocated().unwrap();
                drop(sm);

                n *= 100;
                n /= nr_allocated_metadata;

                let _r = report.progress(n as u8);
                thread::sleep(interval);
            }
        });
    }

    Ok((tid, stop_progress))
}

struct Context {
    report: Arc<Report>,
    engine: Arc<dyn IoEngine + Send + Sync>,
    pool: ThreadPool,
}

// Check the mappings filling in the data_sm as we go.
fn check_mapping_bottom_level(
    ctx: &Context,
    metadata_sm: &Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    data_sm: &Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    roots: &BTreeMap<u64, (Vec<u64>, u64)>,
    ignore_non_fatal: bool,
) -> Result<()> {
    ctx.report.set_sub_title("mapping tree");

    let w = Arc::new(BTreeWalker::new_with_sm(
        ctx.engine.clone(),
        metadata_sm.clone(),
        ignore_non_fatal,
    )?);

    // We want to print out errors as we progress, so we aggregate for each thin and print
    // at that point.
    let mut failed = false;

    if roots.len() > 64 {
        let errs = Arc::new(Mutex::new(Vec::new()));
        for (path, root) in roots.values() {
            let data_sm = data_sm.clone();
            let root = *root;
            let v = BottomLevelVisitor { data_sm };
            let w = w.clone();
            let mut path = path.clone();
            let errs = errs.clone();

            ctx.pool.execute(move || {
                if let Err(e) = w.walk(&mut path, &v, root) {
                    let mut errs = errs.lock().unwrap();
                    errs.push(e);
                }
            });
        }
        ctx.pool.join();
        let errs = Arc::try_unwrap(errs).unwrap().into_inner().unwrap();
        if !errs.is_empty() {
            ctx.report.fatal(&format!("{}", aggregate_error(errs)));
            failed = true;
        }
    } else {
        for (path, root) in roots.values() {
            let w = w.clone();
            let data_sm = data_sm.clone();
            let root = *root;
            let v = Arc::new(BottomLevelVisitor { data_sm });
            let mut path = path.clone();

            if let Err(e) = walk_threaded(&mut path, w, &ctx.pool, v, root) {
                failed = true;
                ctx.report.fatal(&format!("{}", e));
            }
        }
    }

    if failed {
        Err(anyhow!("Check of mappings failed"))
    } else {
        Ok(())
    }
}

fn mk_context(engine: Arc<dyn IoEngine + Send + Sync>, report: Arc<Report>) -> Result<Context> {
    let nr_threads = std::cmp::max(8, num_cpus::get() * 2);
    let pool = ThreadPool::new(nr_threads);

    Ok(Context {
        report,
        engine,
        pool,
    })
}

fn bail_out(ctx: &Context, task: &str) -> Result<()> {
    use ReportOutcome::*;

    match ctx.report.get_outcome() {
        Fatal => Err(anyhow!(format!(
            "Check of {} failed, ending check early.",
            task
        ))),
        _ => Ok(()),
    }
}

pub fn check(opts: ThinCheckOptions) -> Result<()> {
    let ctx = mk_context(opts.engine.clone(), opts.report.clone())?;

    // FIXME: temporarily get these out
    let report = &ctx.report;
    let engine = &ctx.engine;

    report.set_title("Checking thin metadata");

    // superblock
    let sb = read_superblock(engine.as_ref(), SUPERBLOCK_LOCATION)?;

    report.info(&format!("TRANSACTION_ID={}", sb.transaction_id));

    if opts.sb_only {
        return Ok(());
    }

    let metadata_root = unpack::<SMRoot>(&sb.metadata_sm_root[0..])?;
    let mut path = vec![0];

    // Device details.   We read this once to get the number of thin devices, and hence the
    // maximum metadata ref count.  Then create metadata space map, and reread to increment
    // the ref counts for that metadata.
    let devs = btree_to_map::<DeviceDetail>(
        &mut path,
        engine.clone(),
        opts.ignore_non_fatal,
        sb.details_root,
    )?;
    let nr_devs = devs.len();
    let metadata_sm = core_sm(engine.get_nr_blocks(), nr_devs as u32);
    inc_superblock(&metadata_sm)?;

    report.set_sub_title("device details tree");
    let _devs = btree_to_map_with_sm::<DeviceDetail>(
        &mut path,
        engine.clone(),
        metadata_sm.clone(),
        opts.ignore_non_fatal,
        sb.details_root,
    )?;

    let (tid, stop_progress) = spawn_progress_thread(
        metadata_sm.clone(),
        metadata_root.nr_allocated,
        report.clone(),
    )?;

    // mapping top level
    report.set_sub_title("mapping tree");
    let roots = btree_to_map_with_path::<u64>(
        &mut path,
        engine.clone(),
        metadata_sm.clone(),
        opts.ignore_non_fatal,
        sb.mapping_root,
    )?;

    if opts.skip_mappings {
        let cleared = clear_needs_check_flag(ctx.engine.clone())?;
        if cleared {
            ctx.report.info("Cleared needs_check flag");
        }
        return Ok(());
    }

    // mapping bottom level
    let root = unpack::<SMRoot>(&sb.data_sm_root[0..])?;
    let data_sm = core_sm(root.nr_blocks, nr_devs as u32);
    check_mapping_bottom_level(&ctx, &metadata_sm, &data_sm, &roots, opts.ignore_non_fatal)?;
    bail_out(&ctx, "mapping tree")?;

    //-----------------------------------------

    report.set_sub_title("data space map");
    let root = unpack::<SMRoot>(&sb.data_sm_root[0..])?;
    let data_leaks = check_disk_space_map(
        engine.clone(),
        report.clone(),
        root,
        data_sm.clone(),
        metadata_sm.clone(),
        opts.ignore_non_fatal,
    )?;
    bail_out(&ctx, "data space map")?;

    //-----------------------------------------

    report.set_sub_title("metadata space map");
    let root = unpack::<SMRoot>(&sb.metadata_sm_root[0..])?;
    report.info(&format!(
        "METADATA_FREE_BLOCKS={}",
        root.nr_blocks - root.nr_allocated
    ));

    // Now the counts should be correct and we can check it.
    let metadata_leaks = check_metadata_space_map(
        engine.clone(),
        report.clone(),
        root,
        metadata_sm.clone(),
        opts.ignore_non_fatal,
    )?;

    bail_out(&ctx, "metadata space map")?;

    //-----------------------------------------

    if opts.auto_repair {
        if !data_leaks.is_empty() {
            ctx.report.info("Repairing data leaks.");
            repair_space_map(ctx.engine.clone(), data_leaks, data_sm.clone())?;
        }

        if !metadata_leaks.is_empty() {
            ctx.report.info("Repairing metadata leaks.");
            repair_space_map(ctx.engine.clone(), metadata_leaks, metadata_sm.clone())?;
        }

        let cleared = clear_needs_check_flag(ctx.engine.clone())?;
        if cleared {
            ctx.report.info("Cleared needs_check flag");
        }
    } else if !opts.ignore_non_fatal {
        if !data_leaks.is_empty() {
            return Err(anyhow!("data space map contains leaks"));
        }

        if !metadata_leaks.is_empty() {
            return Err(anyhow!("metadata space map contains leaks"));
        }

        if opts.clear_needs_check {
            let cleared = clear_needs_check_flag(ctx.engine.clone())?;
            if cleared {
                ctx.report.info("Cleared needs_check flag");
            }
        }
    }

    stop_progress.store(true, Ordering::Relaxed);
    tid.join().unwrap();

    Ok(())
}

pub fn clear_needs_check_flag(engine: Arc<dyn IoEngine + Send + Sync>) -> Result<bool> {
    let mut sb = read_superblock(engine.as_ref(), SUPERBLOCK_LOCATION)?;
    if !sb.flags.needs_check {
        return Ok(false);
    }
    sb.flags.needs_check = false;
    write_superblock(engine.as_ref(), SUPERBLOCK_LOCATION, &sb).map(|_| true)
}

//------------------------------------------

// Some callers wish to know which blocks are allocated.
pub struct CheckMaps {
    pub metadata_sm: Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    pub data_sm: Arc<Mutex<dyn SpaceMap + Send + Sync>>,
}

pub fn check_with_maps(
    engine: Arc<dyn IoEngine + Send + Sync>,
    report: Arc<Report>,
) -> Result<CheckMaps> {
    let ctx = mk_context(engine.clone(), report.clone())?;
    report.set_title("Checking thin metadata");

    // superblock
    let sb = read_superblock(engine.as_ref(), SUPERBLOCK_LOCATION)?;

    report.info(&format!("TRANSACTION_ID={}", sb.transaction_id));

    let metadata_root = unpack::<SMRoot>(&sb.metadata_sm_root[0..])?;
    let mut path = vec![0];

    // Device details.   We read this once to get the number of thin devices, and hence the
    // maximum metadata ref count.  Then create metadata space map, and reread to increment
    // the ref counts for that metadata.
    let devs = btree_to_map::<DeviceDetail>(&mut path, engine.clone(), false, sb.details_root)?;
    let nr_devs = devs.len();
    let metadata_sm = core_sm(engine.get_nr_blocks(), nr_devs as u32);
    inc_superblock(&metadata_sm)?;

    report.set_sub_title("device details tree");
    let _devs = btree_to_map_with_sm::<DeviceDetail>(
        &mut path,
        engine.clone(),
        metadata_sm.clone(),
        false,
        sb.details_root,
    )?;

    let (tid, stop_progress) = spawn_progress_thread(
        metadata_sm.clone(),
        metadata_root.nr_allocated,
        report.clone(),
    )?;

    // mapping top level
    report.set_sub_title("mapping tree");
    let roots = btree_to_map_with_path::<u64>(
        &mut path,
        engine.clone(),
        metadata_sm.clone(),
        false,
        sb.mapping_root,
    )?;

    // mapping bottom level
    let root = unpack::<SMRoot>(&sb.data_sm_root[0..])?;
    let data_sm = core_sm(root.nr_blocks, nr_devs as u32);
    check_mapping_bottom_level(&ctx, &metadata_sm, &data_sm, &roots, false)?;
    bail_out(&ctx, "mapping tree")?;

    //-----------------------------------------

    report.set_sub_title("data space map");
    let root = unpack::<SMRoot>(&sb.data_sm_root[0..])?;
    let _data_leaks = check_disk_space_map(
        engine.clone(),
        report.clone(),
        root,
        data_sm.clone(),
        metadata_sm.clone(),
        false,
    )?;
    bail_out(&ctx, "data space map")?;

    //-----------------------------------------

    report.set_sub_title("metadata space map");
    let root = unpack::<SMRoot>(&sb.metadata_sm_root[0..])?;
    report.info(&format!(
        "METADATA_FREE_BLOCKS={}",
        root.nr_blocks - root.nr_allocated
    ));

    // Now the counts should be correct and we can check it.
    let _metadata_leaks =
        check_metadata_space_map(engine.clone(), report, root, metadata_sm.clone(), false)?;
    bail_out(&ctx, "metadata space map")?;

    //-----------------------------------------

    stop_progress.store(true, Ordering::Relaxed);
    tid.join().unwrap();

    Ok(CheckMaps {
        metadata_sm: metadata_sm.clone(),
        data_sm: data_sm.clone(),
    })
}

//------------------------------------------
