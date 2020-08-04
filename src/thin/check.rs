use anyhow::{anyhow, Result};
use fixedbitset::FixedBitSet;
use nom::{number::complete::*, IResult};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use threadpool::ThreadPool;

use crate::block_manager::{AsyncIoEngine, Block, IoEngine};
use crate::pdata::btree::{BTreeWalker, Node, NodeVisitor, ValueType};
use crate::thin::superblock::*;

//------------------------------------------

struct TopLevelVisitor<'a> {
    roots: &'a mut HashMap<u32, u64>,
}

impl<'a> NodeVisitor<u64> for TopLevelVisitor<'a> {
    fn visit(&mut self, w: &BTreeWalker, _b: &Block, node: &Node<u64>) -> Result<()> {
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

impl ValueType for BlockTime {
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

impl ValueType for DeviceDetail {
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
                self.devs.insert(k, v.clone());
            }
        }

        Ok(())
    }
}

//------------------------------------------

pub fn check(dev: &Path) -> Result<()> {
    //let mut engine = SyncIoEngine::new(dev)?;
    let mut engine = Arc::new(AsyncIoEngine::new(dev, 256)?);

    let now = Instant::now();
    let sb = read_superblock(engine.as_ref(), SUPERBLOCK_LOCATION)?;
    eprintln!("{:?}", sb);

    {
        let mut visitor = DeviceVisitor::new();
        let mut w = BTreeWalker::new(engine.clone(), false);
        w.walk(&mut visitor, sb.details_root)?;
        println!("found {} devices", visitor.devs.len());
    }

    let mut roots = HashMap::new();
    {
        let mut visitor = TopLevelVisitor { roots: &mut roots };
        let mut w = BTreeWalker::new(engine.clone(), false);
        let _result = w.walk(&mut visitor, sb.mapping_root)?;
        println!("read mapping tree in {} ms", now.elapsed().as_millis());
    }

    // FIXME: with a thread pool we need to return errors another way.
    {
        let nr_workers = 4;
        let pool = ThreadPool::new(nr_workers);
        let mut seen = Arc::new(Mutex::new(FixedBitSet::with_capacity(
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

    Ok(())
}

//------------------------------------------
