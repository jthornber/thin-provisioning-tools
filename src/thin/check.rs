use anyhow::{anyhow, Result};
use indicatif::{ProgressBar, ProgressStyle};
use nom::{number::complete::*, IResult};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::mpsc::{channel, Receiver, Sender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::{thread, time};
use threadpool::ThreadPool;

use crate::checksum;
use crate::io_engine::{AsyncIoEngine, Block, IoEngine, SyncIoEngine};
use crate::pdata::btree::{btree_to_map, btree_to_map_with_sm, BTreeWalker, Node, NodeVisitor};
use crate::pdata::space_map::*;
use crate::pdata::unpack::*;
use crate::thin::superblock::*;

//------------------------------------------

struct TopLevelVisitor<'a> {
    roots: &'a mut BTreeMap<u32, u64>,
}

impl<'a> NodeVisitor<u64> for TopLevelVisitor<'a> {
    fn visit(&mut self, _w: &BTreeWalker, _b: &Block, node: &Node<u64>) -> Result<()> {
        if let Node::Leaf {
            header: _h,
            keys,
            values,
        } = node
        {
            for n in 0..keys.len() {
                let k = keys[n];
                let root = values[n];
                self.roots.insert(k as u32, root);
            }
        }

        Ok(())
    }
}

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
    data_sm: Arc<Mutex<dyn SpaceMap + Send>>,
}

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

struct ReportOptions {}

#[derive(Clone)]
enum ReportOutcome {
    Success,
    NonFatal,
    Fatal,
}

use ReportOutcome::*;

impl ReportOutcome {
    fn combine(lhs: &ReportOutcome, rhs: &ReportOutcome) -> ReportOutcome {
        match (lhs, rhs) {
            (Success, rhs) => rhs.clone(),
            (lhs, Success) => lhs.clone(),
            (Fatal, _) => Fatal,
            (_, Fatal) => Fatal,
            (_, _) => NonFatal,
        }
    }
}

enum ReportCmd {
    Log(String),
    Complete,
    Title(String),
}

struct Report {
    opts: ReportOptions,
    outcome: ReportOutcome,
    tx: Sender<ReportCmd>,
    tid: thread::JoinHandle<()>,
}

impl Report {
    fn new(
        opts: ReportOptions,
        sm: Arc<Mutex<dyn SpaceMap + Send + Sync>>,
        total_allocated: u64,
    ) -> Result<Report> {
        let (tx, rx) = channel();
        let tid = thread::spawn(move || report_thread(sm, total_allocated, rx));
        Ok(Report {
            opts,
            outcome: ReportOutcome::Success,
            tx,
            tid,
        })
    }

    fn info<I: Into<String>>(&mut self, txt: I) -> Result<()> {
        self.tx.send(ReportCmd::Log(txt.into()))?;
        Ok(())
    }

    fn add_outcome(&mut self, rhs: ReportOutcome) {
        self.outcome = ReportOutcome::combine(&self.outcome, &rhs);
    }

    fn non_fatal<I: Into<String>>(&mut self, txt: I) -> Result<()> {
        self.add_outcome(NonFatal);
        self.tx.send(ReportCmd::Log(txt.into()))?;
        Ok(())
    }

    fn fatal<I: Into<String>>(&mut self, txt: I) -> Result<()> {
        self.add_outcome(Fatal);
        self.tx.send(ReportCmd::Log(txt.into()))?;
        Ok(())
    }

    fn complete(self) -> Result<()> {
        self.tx.send(ReportCmd::Complete)?;
        self.tid.join();
        Ok(())
    }

    fn set_title(&mut self, txt: &str) -> Result<()> {
        self.tx.send(ReportCmd::Title(txt.to_string()))?;
        Ok(())
    }
}

fn report_thread(
    sm: Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    total_allocated: u64,
    rx: Receiver<ReportCmd>,
) {
    let interval = time::Duration::from_millis(250);
    let bar = ProgressBar::new(total_allocated);
    loop {
        loop {
            match rx.try_recv() {
                Ok(ReportCmd::Log(txt)) => {
                    bar.println(txt);
                }
                Ok(ReportCmd::Complete) => {
                    bar.finish();
                    return;
                }
                Ok(ReportCmd::Title(txt)) => {
                    let mut fmt = "Checking thin metadata [{bar:40}] Remaining {eta}, ".to_string();
                    fmt.push_str(&txt);
                    bar.set_style(
                        ProgressStyle::default_bar()
                            .template(&fmt)
                            .progress_chars("=> "),
                    );
                }
                Err(TryRecvError::Disconnected) => {
                    return;
                }
                Err(TryRecvError::Empty) => {
                    break;
                }
            }
        }

        let sm = sm.lock().unwrap();
        let nr_allocated = sm.get_nr_allocated().unwrap();
        drop(sm);

        bar.set_position(nr_allocated);
        bar.tick();

        thread::sleep(interval);
    }
}

//------------------------------------------

fn check_space_map(
    kind: &str,
    engine: Arc<dyn IoEngine + Send + Sync>,
    bar: &mut Report,
    entries: Vec<IndexEntry>,
    metadata_sm: Option<Arc<Mutex<dyn SpaceMap + Send + Sync>>>,
    sm: Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    root: SMRoot,
) -> Result<()> {
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
    let mut fail = false;
    let mut blocknr = 0;
    for n in 0..entries.len() {
        let b = &blocks[n];
        if checksum::metadata_block_type(&b.get_data()) != checksum::BT::BITMAP {
            return Err(anyhow!(
                "Index entry points to block ({}) that isn't a bitmap",
                b.loc
            ));
        }

        let bitmap = unpack::<Bitmap>(b.get_data())?;
        for e in bitmap.entries {
            if blocknr >= root.nr_blocks {
                break;
            }

            match e {
                BitmapEntry::Small(actual) => {
                    let expected = sm.get(blocknr)?;
                    if actual == 1 && expected == 0 {
                        leaks += 1;
                    } else if actual != expected as u8 {
                        bar.fatal(format!("Bad reference count for {} block {}.  Expected {}, but space map contains {}.",
                                  kind, blocknr, expected, actual))?;
                        fail = true;
                    }
                }
                BitmapEntry::Overflow => {
                    let expected = sm.get(blocknr)?;
                    if expected < 3 {
                        bar.fatal(format!("Bad reference count for {} block {}.  Expected {}, but space map says it's >= 3.",
                                          kind, blocknr, expected))?;
                        fail = true;
                    }
                }
            }
            blocknr += 1;
        }
    }

    if leaks > 0 {
        bar.non_fatal(format!(
            "{} {} blocks have leaked.  Use --auto-repair to fix.",
            leaks, kind
        ))?;
    }

    if fail {
        return Err(anyhow!("Inconsistent data space map"));
    }
    Ok(())
}

//------------------------------------------

fn inc_entries(sm: &Arc<Mutex<dyn SpaceMap + Sync + Send>>, entries: &[IndexEntry]) -> Result<()> {
    let mut sm = sm.lock().unwrap();
    for ie in entries {
        sm.inc(ie.blocknr, 1)?;
    }
    Ok(())
}

//------------------------------------------

const MAX_CONCURRENT_IO: u32 = 1024;

pub struct ThinCheckOptions<'a> {
    pub dev: &'a Path,
    pub async_io: bool,
}

pub fn check(opts: &ThinCheckOptions) -> Result<()> {
    let engine: Arc<dyn IoEngine + Send + Sync>;

    let nr_threads;
    if opts.async_io {
        nr_threads = std::cmp::min(4, num_cpus::get());
        engine = Arc::new(AsyncIoEngine::new(opts.dev, MAX_CONCURRENT_IO)?);
    } else {
        nr_threads = num_cpus::get() * 2;
        engine = Arc::new(SyncIoEngine::new(opts.dev, nr_threads)?);
    }

    // superblock
    let sb = read_superblock(engine.as_ref(), SUPERBLOCK_LOCATION)?;

    let nr_allocated_metadata;
    {
        let root = unpack::<SMRoot>(&sb.metadata_sm_root[0..])?;
        nr_allocated_metadata = root.nr_allocated;
    }

    // Device details.   We read this once to get the number of thin devices, and hence the
    // maximum metadata ref count.  Then create metadata space map, and reread to increment
    // the ref counts for that metadata.
    let devs = btree_to_map::<DeviceDetail>(engine.clone(), false, sb.details_root)?;
    let nr_devs = devs.len();
    let metadata_sm = core_sm(engine.get_nr_blocks(), nr_devs as u32);
    let opts = ReportOptions {};
    let mut report = Report::new(opts, metadata_sm.clone(), nr_allocated_metadata)?;

    report.set_title("device details tree")?;
    let _devs = btree_to_map_with_sm::<DeviceDetail>(
        engine.clone(),
        metadata_sm.clone(),
        false,
        sb.details_root,
    )?;

    // increment superblock
    {
        let mut sm = metadata_sm.lock().unwrap();
        sm.inc(SUPERBLOCK_LOCATION, 1)?;
    }

    // mapping top level
    let roots = btree_to_map::<u64>(engine.clone(), false, sb.mapping_root)?;

    // Check the mappings filling in the data_sm as we go.
    report.set_title("mapping tree")?;
    let data_sm;
    {
        // FIXME: with a thread pool we need to return errors another way.
        let nr_workers = nr_threads;
        let pool = ThreadPool::new(nr_workers);

        let root = unpack::<SMRoot>(&sb.data_sm_root[0..])?;
        data_sm = core_sm(root.nr_blocks, nr_devs as u32);

        for (_thin_id, root) in roots {
            let mut w = BTreeWalker::new_with_sm(engine.clone(), metadata_sm.clone(), false)?;
            let data_sm = data_sm.clone();
            pool.execute(move || {
                let mut v = BottomLevelVisitor { data_sm };

                // FIXME: return error
                match w.walk(&mut v, root) {
                    Err(e) => {
                        eprintln!("walk failed {:?}", e);
                        std::process::abort();
                    }
                    Ok(_result) => {
                        //eprintln!("checked thin_dev {} -> {:?}", thin_id, result);
                    }
                }
            });
        }

        pool.join();
    }

    report.set_title("data space map")?;
    let root = unpack::<SMRoot>(&sb.data_sm_root[0..])?;

    let entries = btree_to_map_with_sm::<IndexEntry>(
        engine.clone(),
        metadata_sm.clone(),
        false,
        root.bitmap_root,
    )?;
    let entries: Vec<IndexEntry> = entries.values().cloned().collect();
    inc_entries(&metadata_sm, &entries[0..])?;

    check_space_map(
        "data",
        engine.clone(),
        &mut report,
        entries,
        Some(metadata_sm.clone()),
        data_sm.clone(),
        root,
    )?;

    report.set_title("metadata space map")?;
    let root = unpack::<SMRoot>(&sb.metadata_sm_root[0..])?;
    let mut b = Block::new(root.bitmap_root);
    engine.read(&mut b)?;
    let entries = unpack::<MetadataIndex>(b.get_data())?.indexes;

    // Unused entries will point to block 0
    let entries: Vec<IndexEntry> = entries
        .iter()
        .take_while(|e| e.blocknr != 0)
        .cloned()
        .collect();
    inc_entries(&metadata_sm, &entries[0..])?;

    let _counts = btree_to_map_with_sm::<u32>(
        engine.clone(),
        metadata_sm.clone(),
        false,
        root.ref_count_root,
    )?;

    // Now the counts should be correct and we can check it.
    check_space_map(
        "metadata",
        engine.clone(),
        &mut report,
        entries,
        None,
        metadata_sm.clone(),
        root,
    )?;

    report.complete()?;
    Ok(())
}

//------------------------------------------
