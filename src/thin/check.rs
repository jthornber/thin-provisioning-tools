use anyhow::{anyhow, Result};
use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use threadpool::ThreadPool;

use crate::checksum;
use crate::io_engine::IoEngine;
use crate::pdata::btree::{self, *};
use crate::pdata::btree_walker::*;
use crate::pdata::space_map::*;
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

struct OverflowChecker<'a> {
    data_sm: &'a dyn SpaceMap,
}

impl<'a> OverflowChecker<'a> {
    fn new(data_sm: &'a dyn SpaceMap) -> OverflowChecker<'a> {
        OverflowChecker { data_sm }
    }
}

impl<'a> NodeVisitor<u32> for OverflowChecker<'a> {
    fn visit(
        &self,
        _path: &[u64],
        _kr: &KeyRange,
        _h: &NodeHeader,
        keys: &[u64],
        values: &[u32],
    ) -> btree::Result<()> {
        for n in 0..keys.len() {
            let k = keys[n];
            let v = values[n];
            let expected = self.data_sm.get(k).unwrap();
            if expected != v {
                return Err(value_err(format!("Bad reference count for data block {}.  Expected {}, but space map contains {}.",
                                  k, expected, v)));
            }
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

//------------------------------------------

struct BitmapLeak {
    blocknr: u64, // blocknr for the first entry in the bitmap
    loc: u64,     // location of the bitmap
}

// This checks the space map and returns any leak blocks for auto-repair to process.
fn check_space_map(
    path: &mut Vec<u64>,
    ctx: &Context,
    kind: &str,
    entries: Vec<IndexEntry>,
    metadata_sm: Option<ASpaceMap>,
    sm: ASpaceMap,
    root: SMRoot,
) -> Result<Vec<BitmapLeak>> {
    let report = ctx.report.clone();
    let engine = ctx.engine.clone();

    let sm = sm.lock().unwrap();

    // overflow btree
    {
        let v = OverflowChecker::new(&*sm);
        let w;
        if metadata_sm.is_none() {
            w = BTreeWalker::new(engine.clone(), false);
        } else {
            w = BTreeWalker::new_with_sm(engine.clone(), metadata_sm.unwrap().clone(), false)?;
        }
        w.walk(path, &v, root.ref_count_root)?;
    }

    let mut blocks = Vec::with_capacity(entries.len());
    for i in &entries {
        blocks.push(i.blocknr);
    }

    // FIXME: we should do this in batches
    let blocks = engine.read_many(&blocks)?;

    let mut leaks = 0;
    let mut blocknr = 0;
    let mut bitmap_leaks = Vec::new();
    for b in blocks.iter().take(entries.len()) {
        match b {
            Err(_e) => {
                return Err(anyhow!("Unable to read bitmap block"));
            }
            Ok(b) => {
                if checksum::metadata_block_type(&b.get_data()) != checksum::BT::BITMAP {
                    report.fatal(&format!(
                        "Index entry points to block ({}) that isn't a bitmap",
                        b.loc
                    ));
                }

                let bitmap = unpack::<Bitmap>(b.get_data())?;
                let first_blocknr = blocknr;
                let mut contains_leak = false;
                for e in bitmap.entries.iter() {
                    if blocknr >= root.nr_blocks {
                        break;
                    }

                    match e {
                        BitmapEntry::Small(actual) => {
                            let expected = sm.get(blocknr)?;
                            if *actual == 1 && expected == 0 {
                                leaks += 1;
                                contains_leak = true;
                            } else if *actual != expected as u8 {
                                report.fatal(&format!("Bad reference count for {} block {}.  Expected {}, but space map contains {}.",
                                          kind, blocknr, expected, actual));
                            }
                        }
                        BitmapEntry::Overflow => {
                            let expected = sm.get(blocknr)?;
                            if expected < 3 {
                                report.fatal(&format!("Bad reference count for {} block {}.  Expected {}, but space map says it's >= 3.",
                                                  kind, blocknr, expected));
                            }
                        }
                    }
                    blocknr += 1;
                }
                if contains_leak {
                    bitmap_leaks.push(BitmapLeak {
                        blocknr: first_blocknr,
                        loc: b.loc,
                    });
                }
            }
        }
    }

    if leaks > 0 {
        report.non_fatal(&format!("{} {} blocks have leaked.", leaks, kind));
    }

    Ok(bitmap_leaks)
}

// This assumes the only errors in the space map are leaks.  Entries should just be
// those that contain leaks.
fn repair_space_map(ctx: &Context, entries: Vec<BitmapLeak>, sm: ASpaceMap) -> Result<()> {
    let engine = ctx.engine.clone();

    let sm = sm.lock().unwrap();

    let mut blocks = Vec::with_capacity(entries.len());
    for i in &entries {
        blocks.push(i.loc);
    }

    // FIXME: we should do this in batches
    let rblocks = engine.read_many(&blocks[0..])?;
    let mut write_blocks = Vec::new();

    for (i, rb) in rblocks.into_iter().enumerate() {
        if let Ok(b) = rb {
            let be = &entries[i];
            let mut blocknr = be.blocknr;
            let mut bitmap = unpack::<Bitmap>(b.get_data())?;
            for e in bitmap.entries.iter_mut() {
                if blocknr >= sm.get_nr_blocks()? {
                    break;
                }

                if let BitmapEntry::Small(actual) = e {
                    let expected = sm.get(blocknr)?;
                    if *actual == 1 && expected == 0 {
                        *e = BitmapEntry::Small(0);
                    }
                }

                blocknr += 1;
            }

            let mut out = Cursor::new(b.get_data());
            bitmap.pack(&mut out)?;
            checksum::write_checksum(b.get_data(), checksum::BT::BITMAP)?;

            write_blocks.push(b);
        } else {
            return Err(anyhow!("Unable to reread bitmap blocks for repair"));
        }
    }

    engine.write_many(&write_blocks[0..])?;
    Ok(())
}

//------------------------------------------

fn inc_entries(sm: &ASpaceMap, entries: &[IndexEntry]) -> Result<()> {
    let mut sm = sm.lock().unwrap();
    for ie in entries {
        sm.inc(ie.blocknr, 1)?;
    }
    Ok(())
}

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
) -> Result<()> {
    ctx.report.set_sub_title("mapping tree");

    let w = Arc::new(BTreeWalker::new_with_sm(
        ctx.engine.clone(),
        metadata_sm.clone(),
        false,
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
    let mut path = Vec::new();
    path.push(0);

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
        return Ok(());
    }

    // mapping bottom level
    let root = unpack::<SMRoot>(&sb.data_sm_root[0..])?;
    let data_sm = core_sm(root.nr_blocks, nr_devs as u32);
    check_mapping_bottom_level(&ctx, &metadata_sm, &data_sm, &roots)?;
    bail_out(&ctx, "mapping tree")?;

    report.set_sub_title("data space map");
    let root = unpack::<SMRoot>(&sb.data_sm_root[0..])?;

    let entries = btree_to_map_with_sm::<IndexEntry>(
        &mut path,
        engine.clone(),
        metadata_sm.clone(),
        opts.ignore_non_fatal,
        root.bitmap_root,
    )?;
    let entries: Vec<IndexEntry> = entries.values().cloned().collect();
    inc_entries(&metadata_sm, &entries[0..])?;

    let data_leaks = check_space_map(
        &mut path,
        &ctx,
        "data",
        entries,
        Some(metadata_sm.clone()),
        data_sm.clone(),
        root,
    )?;
    bail_out(&ctx, "data space map")?;

    report.set_sub_title("metadata space map");
    let root = unpack::<SMRoot>(&sb.metadata_sm_root[0..])?;
    report.info(&format!(
        "METADATA_FREE_BLOCKS={}",
        root.nr_blocks - root.nr_allocated
    ));

    let b = engine.read(root.bitmap_root)?;
    metadata_sm.lock().unwrap().inc(root.bitmap_root, 1)?;
    let entries = unpack::<MetadataIndex>(b.get_data())?.indexes;

    // Unused entries will point to block 0
    let entries: Vec<IndexEntry> = entries
        .iter()
        .take_while(|e| e.blocknr != 0)
        .cloned()
        .collect();
    inc_entries(&metadata_sm, &entries[0..])?;

    // We call this for the side effect of incrementing the ref counts
    // for the metadata that holds the tree.
    let _counts = btree_to_map_with_sm::<u32>(
        &mut path,
        engine.clone(),
        metadata_sm.clone(),
        opts.ignore_non_fatal,
        root.ref_count_root,
    )?;

    // Now the counts should be correct and we can check it.
    let metadata_leaks = check_space_map(
        &mut path,
        &ctx,
        "metadata",
        entries,
        None,
        metadata_sm.clone(),
        root,
    )?;
    bail_out(&ctx, "metadata space map")?;

    if opts.auto_repair {
        if !data_leaks.is_empty() {
            ctx.report.info("Repairing data leaks.");
            repair_space_map(&ctx, data_leaks, data_sm.clone())?;
        }

        if !metadata_leaks.is_empty() {
            ctx.report.info("Repairing metadata leaks.");
            repair_space_map(&ctx, metadata_leaks, metadata_sm.clone())?;
        }
    }

    stop_progress.store(true, Ordering::Relaxed);
    tid.join().unwrap();

    Ok(())
}

//------------------------------------------

// Some callers wish to know which blocks are allocated.
pub struct CheckMaps {
    pub metadata_sm: Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    pub data_sm: Arc<Mutex<dyn SpaceMap + Send + Sync>>,
}

pub fn check_with_maps(engine: Arc<dyn IoEngine + Send + Sync>, report: Arc<Report>) -> Result<CheckMaps> {
    let ctx = mk_context(engine.clone(), report.clone())?;
    report.set_title("Checking thin metadata");

    // superblock
    let sb = read_superblock(engine.as_ref(), SUPERBLOCK_LOCATION)?;

    report.info(&format!("TRANSACTION_ID={}", sb.transaction_id));

    let metadata_root = unpack::<SMRoot>(&sb.metadata_sm_root[0..])?;
    let mut path = Vec::new();
    path.push(0);

    // Device details.   We read this once to get the number of thin devices, and hence the
    // maximum metadata ref count.  Then create metadata space map, and reread to increment
    // the ref counts for that metadata.
    let devs = btree_to_map::<DeviceDetail>(
        &mut path,
        engine.clone(),
        false,
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
    check_mapping_bottom_level(&ctx, &metadata_sm, &data_sm, &roots)?;
    bail_out(&ctx, "mapping tree")?;

    report.set_sub_title("data space map");
    let root = unpack::<SMRoot>(&sb.data_sm_root[0..])?;

    let entries = btree_to_map_with_sm::<IndexEntry>(
        &mut path,
        engine.clone(),
        metadata_sm.clone(),
        false,
        root.bitmap_root,
    )?;
    let entries: Vec<IndexEntry> = entries.values().cloned().collect();
    inc_entries(&metadata_sm, &entries[0..])?;

    let _data_leaks = check_space_map(
        &mut path,
        &ctx,
        "data",
        entries,
        Some(metadata_sm.clone()),
        data_sm.clone(),
        root,
    )?;
    bail_out(&ctx, "data space map")?;

    report.set_sub_title("metadata space map");
    let root = unpack::<SMRoot>(&sb.metadata_sm_root[0..])?;
    report.info(&format!(
        "METADATA_FREE_BLOCKS={}",
        root.nr_blocks - root.nr_allocated
    ));

    let b = engine.read(root.bitmap_root)?;
    metadata_sm.lock().unwrap().inc(root.bitmap_root, 1)?;
    let entries = unpack::<MetadataIndex>(b.get_data())?.indexes;

    // Unused entries will point to block 0
    let entries: Vec<IndexEntry> = entries
        .iter()
        .take_while(|e| e.blocknr != 0)
        .cloned()
        .collect();
    inc_entries(&metadata_sm, &entries[0..])?;

    // We call this for the side effect of incrementing the ref counts
    // for the metadata that holds the tree.
    let _counts = btree_to_map_with_sm::<u32>(
        &mut path,
        engine.clone(),
        metadata_sm.clone(),
        false,
        root.ref_count_root,
    )?;

    // Now the counts should be correct and we can check it.
    let _metadata_leaks = check_space_map(
        &mut path,
        &ctx,
        "metadata",
        entries,
        None,
        metadata_sm.clone(),
        root,
    )?;
    bail_out(&ctx, "metadata space map")?;

    stop_progress.store(true, Ordering::Relaxed);
    tid.join().unwrap();

    Ok(CheckMaps {
        metadata_sm: metadata_sm.clone(),
        data_sm: data_sm.clone(),
    })
}


