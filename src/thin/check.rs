use anyhow::{anyhow, Result};
use nom::{number::complete::*, IResult};
use std::collections::BTreeMap;
use std::io::Cursor;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use threadpool::ThreadPool;

use crate::checksum;
use crate::io_engine::{AsyncIoEngine, Block, IoEngine, SyncIoEngine};
use crate::pdata::btree::{btree_to_map, btree_to_map_with_sm, BTreeWalker, Node, NodeVisitor};
use crate::pdata::space_map::*;
use crate::pdata::unpack::*;
use crate::report::*;
use crate::thin::superblock::*;

//------------------------------------------

#[allow(dead_code)]
struct BlockTime {
    block: u64,
    time: u32,
}

impl Unpack for BlockTime {
    fn disk_size() -> u32 {
        8
    }

    fn unpack(i: &[u8]) -> IResult<&[u8], BlockTime> {
        let (i, n) = le_u64(i)?;
        let block = n >> 24;
        let time = n & ((1 << 24) - 1);

        Ok((
            i,
            BlockTime {
                block,
                time: time as u32,
            },
        ))
    }
}

struct BottomLevelVisitor {
    data_sm: ASpaceMap,
}

//------------------------------------------

impl NodeVisitor<BlockTime> for BottomLevelVisitor {
    fn visit(&mut self, _w: &BTreeWalker, _b: &Block, node: &Node<BlockTime>) -> Result<()> {
        // FIXME: do other checks

        if let Node::Leaf {
            header: _h,
            keys: _k,
            values,
        } = node
        {
            if values.len() == 0 {
                return Ok(());
            }

            let mut data_sm = self.data_sm.lock().unwrap();

            let mut start = values[0].block;
            let mut len = 1;

            for n in 1..values.len() {
                let block = values[n].block;
                if block == start + len {
                    len += 1;
                } else {
                    data_sm.inc(start, len)?;
                    start = block;
                    len = 1;
                }
            }

            data_sm.inc(start, len)?;
        }

        Ok(())
    }
}

//------------------------------------------

#[derive(Clone)]
struct DeviceDetail {
    mapped_blocks: u64,
    transaction_id: u64,
    creation_time: u32,
    snapshotted_time: u32,
}

impl Unpack for DeviceDetail {
    fn disk_size() -> u32 {
        24
    }

    fn unpack(i: &[u8]) -> IResult<&[u8], DeviceDetail> {
        let (i, mapped_blocks) = le_u64(i)?;
        let (i, transaction_id) = le_u64(i)?;
        let (i, creation_time) = le_u32(i)?;
        let (i, snapshotted_time) = le_u32(i)?;

        Ok((
            i,
            DeviceDetail {
                mapped_blocks,
                transaction_id,
                creation_time,
                snapshotted_time,
            },
        ))
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
    fn visit(&mut self, _w: &BTreeWalker, _b: &Block, node: &Node<u32>) -> Result<()> {
        if let Node::Leaf {
            header: _h,
            keys,
            values,
        } = node
        {
            for n in 0..keys.len() {
                let k = keys[n];
                let v = values[n];
                let expected = self.data_sm.get(k)?;
                if expected != v {
                    return Err(anyhow!("Bad reference count for data block {}.  Expected {}, but space map contains {}.",
                                      k, expected, v));
                }
            }
        }

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
        let mut v = OverflowChecker::new(&*sm);
        let mut w;
        if metadata_sm.is_none() {
            w = BTreeWalker::new(engine.clone(), false);
        } else {
            w = BTreeWalker::new_with_sm(engine.clone(), metadata_sm.unwrap().clone(), false)?;
        }
        w.walk(&mut v, root.ref_count_root)?;
    }

    let mut blocks = Vec::new();
    for i in &entries {
        blocks.push(Block::new(i.blocknr));
    }

    // FIXME: we should do this in batches
    engine.read_many(&mut blocks)?;

    let mut leaks = 0;
    let mut blocknr = 0;
    let mut bitmap_leaks = Vec::new();
    for n in 0..entries.len() {
        let b = &blocks[n];
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

    let mut blocks = Vec::new();
    for i in &entries {
        blocks.push(Block::new(i.loc));
    }

    // FIXME: we should do this in batches
    engine.read_many(&mut blocks)?;

    for (be, b) in entries.iter().zip(blocks.iter()) {
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
    }

    engine.write_many(&blocks)?;
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

const MAX_CONCURRENT_IO: u32 = 1024;

pub struct ThinCheckOptions<'a> {
    pub dev: &'a Path,
    pub async_io: bool,
    pub ignore_non_fatal: bool,
    pub auto_repair: bool,
    pub report: Arc<Report>,
}

fn spawn_progress_thread(
    sm: Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    nr_allocated_metadata: u64,
    report: Arc<Report>,
) -> Result<(JoinHandle<()>, Arc<Mutex<bool>>)> {
    let tid;
    let stop_progress = Arc::new(Mutex::new(false));

    {
        let stop_progress = stop_progress.clone();
        tid = thread::spawn(move || {
            let interval = std::time::Duration::from_millis(250);
            loop {
                {
                    let stop_progress = stop_progress.lock().unwrap();
                    if *stop_progress {
                        break;
                    }
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
    roots: &BTreeMap<u64, u64>,
) -> Result<()> {
    ctx.report.set_sub_title("mapping tree");

    for (_thin_id, root) in roots {
        let mut w = BTreeWalker::new_with_sm(ctx.engine.clone(), metadata_sm.clone(), false)?;
        let data_sm = data_sm.clone();
        let root = *root;
        ctx.pool.execute(move || {
            let mut v = BottomLevelVisitor { data_sm };

            // FIXME: return error
            match w.walk(&mut v, root) {
                Err(e) => {
                    eprintln!("walk failed {:?}", e);
                    std::process::abort();
                }
                Ok(_result) => {}
            }
        });
    }
    ctx.pool.join();

    Ok(())
}

fn mk_context(opts: &ThinCheckOptions) -> Result<Context> {
    let engine: Arc<dyn IoEngine + Send + Sync>;
    let nr_threads;

    if opts.async_io {
        nr_threads = std::cmp::min(4, num_cpus::get());
        engine = Arc::new(AsyncIoEngine::new(opts.dev, MAX_CONCURRENT_IO, opts.auto_repair)?);
    } else {
        nr_threads = num_cpus::get() * 2;
        engine = Arc::new(SyncIoEngine::new(opts.dev, nr_threads, opts.auto_repair)?);
    }
    let pool = ThreadPool::new(nr_threads);

    Ok(Context {
        report: opts.report.clone(),
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
    let ctx = mk_context(&opts)?;

    // FIXME: temporarily get these out
    let report = &ctx.report;
    let engine = &ctx.engine;

    report.set_title("Checking thin metadata");

    // superblock
    let sb = read_superblock(engine.as_ref(), SUPERBLOCK_LOCATION)?;
    let metadata_root = unpack::<SMRoot>(&sb.metadata_sm_root[0..])?;

    // Device details.   We read this once to get the number of thin devices, and hence the
    // maximum metadata ref count.  Then create metadata space map, and reread to increment
    // the ref counts for that metadata.
    let devs = btree_to_map::<DeviceDetail>(engine.clone(), false, sb.details_root)?;
    let nr_devs = devs.len();
    let metadata_sm = core_sm(engine.get_nr_blocks(), nr_devs as u32);
    inc_superblock(&metadata_sm)?;

    report.set_sub_title("device details tree");
    let _devs = btree_to_map_with_sm::<DeviceDetail>(
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
    let roots =
        btree_to_map_with_sm::<u64>(engine.clone(), metadata_sm.clone(), false, sb.mapping_root)?;

    // mapping bottom level
    report.set_sub_title("mapping tree");
    let root = unpack::<SMRoot>(&sb.data_sm_root[0..])?;
    let data_sm = core_sm(root.nr_blocks, nr_devs as u32);
    check_mapping_bottom_level(&ctx, &metadata_sm, &data_sm, &roots)?;
    bail_out(&ctx, "mapping tree")?;

    report.set_sub_title("data space map");
    let root = unpack::<SMRoot>(&sb.data_sm_root[0..])?;

    let entries = btree_to_map_with_sm::<IndexEntry>(
        engine.clone(),
        metadata_sm.clone(),
        false,
        root.bitmap_root,
    )?;
    let entries: Vec<IndexEntry> = entries.values().cloned().collect();
    inc_entries(&metadata_sm, &entries[0..])?;

    let data_leaks = check_space_map(
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
    let mut b = Block::new(root.bitmap_root);
    engine.read(&mut b)?;
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
        engine.clone(),
        metadata_sm.clone(),
        false,
        root.ref_count_root,
    )?;

    // Now the counts should be correct and we can check it.
    let metadata_leaks = check_space_map(&ctx, "metadata", entries, None, metadata_sm.clone(), root)?;
    bail_out(&ctx, "metadata space map")?;

    if opts.auto_repair {
        if data_leaks.len() > 0 {
            ctx.report.info("Repairing data leaks.");
            repair_space_map(&ctx, data_leaks, data_sm.clone())?;
        }

        if metadata_leaks.len() > 0 {
            ctx.report.info("Repairing metadata leaks.");
            repair_space_map(&ctx, metadata_leaks, metadata_sm.clone())?;
        }
    }

    {
        let mut stop_progress = stop_progress.lock().unwrap();
        *stop_progress = true;
    }

    tid.join();

    Ok(())
}

//------------------------------------------
