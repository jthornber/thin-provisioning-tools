use anyhow::{anyhow, Result};
use fixedbitset::FixedBitSet;
use nom::{number::complete::*, IResult};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use threadpool::ThreadPool;

use crate::io_engine::{AsyncIoEngine, SyncIoEngine, Block, IoEngine};
use crate::checksum;
use crate::pdata::btree::{unpack, BTreeWalker, Node, NodeVisitor, Unpack};
use crate::pdata::space_map::*;
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
            if values.len() > 0 {
                let mut data_sm = self.data_sm.lock().unwrap();

                let mut start = values[0].block;
                let mut len = 1;

                for n in 1..values.len() {
                    if values[n].block == start + len {
                        len += 1;
                    } else {
                        data_sm.inc(start, len)?;
                        start = values[n].block;
                        len = 1;
                    }
                }

                data_sm.inc(start, len)?;
            }
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

struct DeviceVisitor {
    devs: BTreeMap<u32, DeviceDetail>,
}

impl DeviceVisitor {
    pub fn new() -> DeviceVisitor {
        DeviceVisitor {
            devs: BTreeMap::new(),
        }
    }
}

impl NodeVisitor<DeviceDetail> for DeviceVisitor {
    fn visit(&mut self, _w: &BTreeWalker, _b: &Block, node: &Node<DeviceDetail>) -> Result<()> {
        if let Node::Leaf {
            header: _h,
            keys,
            values,
        } = node
        {
            for n in 0..keys.len() {
                let k = keys[n] as u32;
                let v = values[n].clone();
                self.devs.insert(k, v);
            }
        }

        Ok(())
    }
}

//------------------------------------------

struct IndexVisitor {
    entries: Vec<IndexEntry>,
}

impl NodeVisitor<IndexEntry> for IndexVisitor {
    fn visit(&mut self, _w: &BTreeWalker, _b: &Block, node: &Node<IndexEntry>) -> Result<()> {
        if let Node::Leaf {
            header: _h,
            keys: _k,
            values,
        } = node
        {
            for v in values {
                // FIXME: check keys are in incremental order
                let v = v.clone();
                self.entries.push(v);
            }
        }

        Ok(())
    }
}

//------------------------------------------

// FIXME: move to btree
struct ValueCollector<V> {
    values: Vec<(u64, V)>,
}

impl<V> ValueCollector<V> {
    fn new() -> ValueCollector<V> {
        ValueCollector { values: Vec::new() }
    }
}

impl<V: Unpack + Clone> NodeVisitor<V> for ValueCollector<V> {
    fn visit(&mut self, _w: &BTreeWalker, _b: &Block, node: &Node<V>) -> Result<()> {
        if let Node::Leaf {
            header: _h,
            keys,
            values,
        } = node
        {
            for n in 0..keys.len() {
                let k = keys[n];
                let v = values[n].clone();
                self.values.push((k, v));
            }
        }

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

const MAX_CONCURRENT_IO: u32 = 1024;

pub fn check(dev: &Path) -> Result<()> {
    let nr_threads = 4;
    let engine = Arc::new(AsyncIoEngine::new(dev, MAX_CONCURRENT_IO)?);
    //let engine: Arc<dyn IoEngine + Send + Sync> = Arc::new(SyncIoEngine::new(dev, nr_threads)?);

    let now = Instant::now();
    let sb = read_superblock(engine.as_ref(), SUPERBLOCK_LOCATION)?;
    eprintln!("{:?}", sb);

    // device details
    let nr_devs;
    {
        let mut visitor = DeviceVisitor::new();
        let mut w = BTreeWalker::new(engine.clone(), false);
        w.walk(&mut visitor, sb.details_root)?;
        nr_devs = visitor.devs.len();
        println!("found {} devices", visitor.devs.len());
    }

    // mapping top level
    let mut roots = BTreeMap::new();
    {
        let mut visitor = TopLevelVisitor { roots: &mut roots };
        let mut w = BTreeWalker::new(engine.clone(), false);
        let _result = w.walk(&mut visitor, sb.mapping_root)?;
        println!("read mapping tree in {} ms", now.elapsed().as_millis());
    }

    // mapping bottom level
    let data_sm;
    {
        // FIXME: with a thread pool we need to return errors another way.
        let nr_workers = nr_threads;
        let pool = ThreadPool::new(nr_workers);
        let seen = Arc::new(Mutex::new(FixedBitSet::with_capacity(
            engine.get_nr_blocks() as usize,
        )));

        let root = unpack::<SMRoot>(&sb.data_sm_root[0..])?;
        data_sm = core_sm(root.nr_blocks, nr_devs as u32);

        for (thin_id, root) in roots {
            let mut w = BTreeWalker::new_with_seen(engine.clone(), seen.clone(), false);
            let data_sm = data_sm.clone();
            pool.execute(move || {
                let mut v = BottomLevelVisitor { data_sm };
                let result = w.walk(&mut v, root).expect("walk failed"); // FIXME: return error
                eprintln!("checked thin_dev {} -> {:?}", thin_id, result);
            });
        }

        pool.join();
    }

    // data space map
    {
        let data_sm = data_sm.lock().unwrap();
        let root = unpack::<SMRoot>(&sb.data_sm_root[0..])?;
        eprintln!("data root: {:?}", root);

        // overflow btree
        {
            let mut v = OverflowChecker::new(&*data_sm);
            let mut w = BTreeWalker::new(engine.clone(), false);
            w.walk(&mut v, root.ref_count_root)?;
        }

        // Bitmaps
        let mut v = IndexVisitor {
            entries: Vec::new(),
        };
        let mut w = BTreeWalker::new(engine.clone(), false);
        let _result = w.walk(&mut v, root.bitmap_root);
        eprintln!("{} index entries", v.entries.len());

        let mut blocks = Vec::new();
        for i in &v.entries {
            blocks.push(Block::new(i.blocknr));
        }

        engine.read_many(&mut blocks)?;

        let mut blocknr = 0;
        for (n, _i) in v.entries.iter().enumerate() {
            let b = &blocks[n];
            if checksum::metadata_block_type(&b.get_data()) != checksum::BT::BITMAP {
                return Err(anyhow!(
                    "Index entry points to block ({}) that isn't a bitmap",
                    b.loc
                ));
            }

            let bitmap = unpack::<Bitmap>(b.get_data())?;
            for e in bitmap.entries {
                match e {
                    BitmapEntry::Small(actual) => {
                        let expected = data_sm.get(blocknr)?;
                        if actual != expected as u8 {
                            return Err(anyhow!("Bad reference count for data block {}.  Expected {}, but space map contains {}.",
                                      blocknr, expected, actual));
                        }
                    }
                    BitmapEntry::Overflow => {
                        let expected = data_sm.get(blocknr)?;
                        if expected < 3 {
                            return Err(anyhow!("Bad reference count for data block {}.  Expected {}, but space map says it's >= 3.",
                                              blocknr, expected));
                        }
                    }
                }
                blocknr += 1;
            }
        }
    }

    Ok(())
}

//------------------------------------------
