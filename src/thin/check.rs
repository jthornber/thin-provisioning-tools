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

enum SpinnerCmd {
    Complete,
    Abort,
    Title(String),
}

struct Spinner {
    tx: Sender<SpinnerCmd>,
    tid: thread::JoinHandle<()>,
}

impl Spinner {
    fn new(sm: Arc<Mutex<dyn SpaceMap + Send + Sync>>, total_allocated: u64) -> Result<Spinner> {
        let (tx, rx) = channel();
        let tid = thread::spawn(move || spinner_thread(sm, total_allocated, rx));
        Ok(Spinner { tx, tid })
    }

    fn complete(self) -> Result<()> {
        self.tx.send(SpinnerCmd::Complete)?;
        self.tid.join();
        Ok(())
    }

    fn abort(self) -> Result<()> {
        self.tx.send(SpinnerCmd::Abort)?;
        self.tid.join();
        Ok(())
    }

    fn set_title(&mut self, txt: &str) -> Result<()> {
        self.tx.send(SpinnerCmd::Title(txt.to_string()))?;
        Ok(())
    }
}

fn spinner_thread(
    sm: Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    total_allocated: u64,
    rx: Receiver<SpinnerCmd>,
) {
    let interval = time::Duration::from_millis(250);
    let bar = ProgressBar::new(total_allocated);
    loop {
        match rx.try_recv() {
            Ok(SpinnerCmd::Complete) => {
                bar.finish();
                return;
            }
            Ok(SpinnerCmd::Abort) => {
                return;
            }
            Ok(SpinnerCmd::Title(txt)) => {
                let mut fmt = "Checking thin metadata [{bar:40.cyan/blue}] Remaining {eta}, ".to_string();
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
            Err(TryRecvError::Empty) => {}
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
        eprintln!("Using synchronous io");
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
    let mut spinner = Spinner::new(metadata_sm.clone(), nr_allocated_metadata)?;

    spinner.set_title("device details tree")?;
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
    spinner.set_title("mapping tree")?;
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

    // Check the data space map.
    {
        spinner.set_title("data space map")?;
        let data_sm = data_sm.lock().unwrap();
        let root = unpack::<SMRoot>(&sb.data_sm_root[0..])?;
        let nr_data_blocks = root.nr_blocks;

        // overflow btree
        {
            let mut v = OverflowChecker::new(&*data_sm);
            let mut w = BTreeWalker::new(engine.clone(), false);
            w.walk(&mut v, root.ref_count_root)?;
        }

        // Bitmaps
        let entries = btree_to_map::<IndexEntry>(engine.clone(), false, root.bitmap_root)?;

        let mut blocks = Vec::new();
        for (_k, i) in &entries {
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
                if blocknr >= nr_data_blocks {
                    break;
                }

                match e {
                    BitmapEntry::Small(actual) => {
                        let expected = data_sm.get(blocknr)?;
                        if actual == 1 && expected == 0 {
                            // eprintln!("Data block {} leaked.", blocknr);
                            leaks += 1;
                        } else if actual != expected as u8 {
                            eprintln!("Bad reference count for data block {}.  Expected {}, but space map contains {}.",
                                      blocknr, expected, actual);
                            fail = true;
                        }
                    }
                    BitmapEntry::Overflow => {
                        let expected = data_sm.get(blocknr)?;
                        if expected < 3 {
                            eprintln!("Bad reference count for data block {}.  Expected {}, but space map says it's >= 3.",
                                              blocknr, expected);
                            fail = true;
                        }
                    }
                }
                blocknr += 1;
            }
        }

        if leaks > 0 {
            eprintln!(
                "{} data blocks have leaked.  Use --auto-repair to fix.",
                leaks
            );
        }

        if fail {
            spinner.abort()?;
            return Err(anyhow!("Inconsistent data space map"));
        }
    }

    // Check the metadata space map.
    spinner.set_title("metadata space map")?;

    spinner.complete()?;
    Ok(())
}

//------------------------------------------
