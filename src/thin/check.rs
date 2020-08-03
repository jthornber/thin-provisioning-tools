use anyhow::{anyhow, Result};
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

struct TopLevelVisitor {}

impl NodeVisitor<u64> for TopLevelVisitor {
    fn visit(&mut self, w: &BTreeWalker, _b: &Block, node: &Node<u64>) -> Result<()> {
        if let Node::Leaf {
            header: _h,
            keys,
            values,
        } = node
        {
            let mut blocks = Vec::new();
            let mut thin_ids = Vec::new();
            let seen = w.seen.lock().unwrap();
            for n in 0..keys.len() {
                let b = values[n];
                if !seen[b as usize] {
                    thin_ids.push(keys[n]);
                    blocks.push(Block::new(b));
                }
            }
            drop(seen);

            w.engine.read_many(&mut blocks)?;

            // FIXME: with a thread pool we need to return errors another way.
            let nr_workers = 4;
            let pool = ThreadPool::new(nr_workers);

            let mut n = 0;
            for b in blocks {
                let thin_id = thin_ids[n];
                n += 1;

                let mut w = w.clone();
                pool.execute(move || {
                    let mut v = BottomLevelVisitor {};
                    let result = w.walk_b(&mut v, &b).expect("walk failed"); // FIXME: return error
                    eprintln!("checked thin_dev {} -> {:?}", thin_id, result);
                });
            }

            pool.join();
        }

        Ok(())
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
        if let Node::Leaf {header: _h, keys, values} = node {
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
    
    {
        let mut visitor = TopLevelVisitor {};
        let mut w = BTreeWalker::new(engine.clone(), false);
        let _result = w.walk(&mut visitor, sb.mapping_root)?;
        println!("read mapping tree in {} ms", now.elapsed().as_millis());
    }

    Ok(())
}

//------------------------------------------
