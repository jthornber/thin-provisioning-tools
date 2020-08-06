use anyhow::{anyhow, Result};
use fixedbitset::FixedBitSet;
use nom::{number::complete::*, IResult};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use threadpool::ThreadPool;

use crate::block_manager::{AsyncIoEngine, Block, IoEngine};
use crate::pdata::btree::{BTreeWalker, Node, NodeVisitor, Unpack, unpack};
use crate::pdata::space_map::*;
use crate::thin::superblock::*;
use crate::checksum;

//------------------------------------------

struct TopLevelVisitor<'a> {
    roots: &'a mut HashMap<u32, u64>,
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

struct BottomLevelVisitor {}

impl NodeVisitor<BlockTime> for BottomLevelVisitor {
    fn visit(&mut self, _w: &BTreeWalker, _b: &Block, _node: &Node<BlockTime>) -> Result<()> {
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
    devs: HashMap<u32, DeviceDetail>,
}

impl DeviceVisitor {
    pub fn new() -> DeviceVisitor {
        DeviceVisitor {
            devs: HashMap::new(),
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
            keys,
            values,
        } = node {
            for n in 0..keys.len() {
                // FIXME: check keys are in incremental order
                let v = values[n].clone();
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
        ValueCollector {
            values: Vec::new(),
        }
    }
}

impl<V: Unpack + Clone> NodeVisitor<V> for ValueCollector<V> {
    fn visit(&mut self, _w: &BTreeWalker, _b: &Block, node: &Node<V>) -> Result<()> {
        if let Node::Leaf {
            header: _h,
            keys,
            values,
        } = node {
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

pub fn check(dev: &Path) -> Result<()> {
    let engine = Arc::new(AsyncIoEngine::new(dev, 256)?);

    let now = Instant::now();
    let sb = read_superblock(engine.as_ref(), SUPERBLOCK_LOCATION)?;
    eprintln!("{:?}", sb);

    // device details
    {
        let mut visitor = DeviceVisitor::new();
        let mut w = BTreeWalker::new(engine.clone(), false);
        w.walk(&mut visitor, sb.details_root)?;
        println!("found {} devices", visitor.devs.len());
    }

/*
    // mapping top level
    let mut roots = HashMap::new();
    {
        let mut visitor = TopLevelVisitor { roots: &mut roots };
        let mut w = BTreeWalker::new(engine.clone(), false);
        let _result = w.walk(&mut visitor, sb.mapping_root)?;
        println!("read mapping tree in {} ms", now.elapsed().as_millis());
    }

    // mapping bottom level
    {
        // FIXME: with a thread pool we need to return errors another way.
        let nr_workers = 4;
        let pool = ThreadPool::new(nr_workers);
        let seen = Arc::new(Mutex::new(FixedBitSet::with_capacity(
            engine.get_nr_blocks() as usize,
        )));

        for (thin_id, root) in roots {
            let mut w = BTreeWalker::new_with_seen(engine.clone(), seen.clone(), false);
            pool.execute(move || {
                let mut v = BottomLevelVisitor {};
                let result = w.walk(&mut v, root).expect("walk failed"); // FIXME: return error
                eprintln!("checked thin_dev {} -> {:?}", thin_id, result);
            });
        }

        pool.join();
    }
    */

    // data space map
    {
        let root = unpack::<SMRoot>(&sb.data_sm_root[0..])?;
        eprintln!("data root: {:?}", root);

        // overflow btree
        let mut overflow: HashMap<u64, u32> = HashMap::new();
        {
            let mut v: ValueCollector<u32> = ValueCollector::new();
            let mut w = BTreeWalker::new(engine.clone(), false);
            w.walk(&mut v, root.ref_count_root)?;

            for (k, v) in v.values {
                overflow.insert(k, v);
            }
        }
        eprintln!("{} overflow entries", overflow.len());

        // Bitmaps
        let mut v = IndexVisitor {entries: Vec::new()};
        let mut w = BTreeWalker::new(engine.clone(), false);
        let _result = w.walk(&mut v, root.bitmap_root);
        eprintln!("{} index entries", v.entries.len());

        for i in v.entries {
            let mut b = Block::new(i.blocknr);
            engine.read(&mut b)?;
            
            if checksum::metadata_block_type(&b.get_data()) != checksum::BT::BITMAP {
                return Err(anyhow!("Index entry points to block ({}) that isn't a bitmap", b.loc));
            }

            let bitmap = unpack::<Bitmap>(b.get_data())?;
        }
    }

    Ok(())
}

//------------------------------------------
