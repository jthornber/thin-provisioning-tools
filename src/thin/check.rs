use anyhow::{anyhow, Result};
use nom::{number::complete::*, IResult};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use threadpool::ThreadPool;

use crate::block_manager::{AsyncIoEngine, Block, IoEngine};
use crate::thin::superblock::*;
use crate::pdata::btree::{ValueType, Node, BTreeWalker, NodeVisitor, ValueU64};

//------------------------------------------

#[allow(dead_code)]
struct BlockTime {
    block: u64,
    time: u32,
}

struct ValueBlockTime;

impl ValueType for ValueBlockTime {
    type Value = BlockTime;
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

impl NodeVisitor<ValueU64> for TopLevelVisitor {
    fn visit(&mut self, w: &BTreeWalker, _b: &Block, node: &Node<ValueU64>) -> Result<()> {
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
                    w.walk_node(&mut v, &b).expect("walk failed"); // FIXME: return error
                    eprintln!("checked thin_dev {}", thin_id);
                });
            }

            pool.join();
        }

        Ok(())
    }
}

struct BottomLevelVisitor {}

impl NodeVisitor<ValueBlockTime> for BottomLevelVisitor {
    fn visit(&mut self, _w: &BTreeWalker, _b: &Block, _node: &Node<ValueBlockTime>) -> Result<()> {
        Ok(())
    }
}

//------------------------------------------

pub fn check(dev: &Path) -> Result<()> {
    //let mut engine = SyncIoEngine::new(dev)?;
    let mut engine = AsyncIoEngine::new(dev, 256)?;

    let now = Instant::now();
    let sb = read_superblock(&mut engine, SUPERBLOCK_LOCATION)?;
    eprintln!("{:?}", sb);

    let mut root = Block::new(sb.mapping_root);
    engine.read(&mut root)?;

    let mut w = BTreeWalker::new(engine);
    let mut visitor = TopLevelVisitor {};
    let _result = w.walk_node(&mut visitor, &root)?;
    println!("read mapping tree in {} ms", now.elapsed().as_millis());

    Ok(())
}

//------------------------------------------
