use anyhow::{anyhow, Result};
use fixedbitset::FixedBitSet;
use futures::executor;
use nom::{bytes::complete::*, number::complete::*, IResult};
use std::collections::HashSet;
use std::error::Error;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread::{self, spawn};
use std::time::{Duration, Instant};
use threadpool::ThreadPool;

use crate::block_manager::{AsyncIoEngine, Block, IoEngine, BLOCK_SIZE};
use crate::checksum;
use crate::thin::superblock::*;

//------------------------------------------

trait ValueType {
    type Value;
    fn unpack(data: &[u8]) -> IResult<&[u8], Self::Value>;
}

struct NodeHeader {
    is_leaf: bool,
    block: u64,
    nr_entries: u32,
    max_entries: u32,
    value_size: u32,
}

const INTERNAL_NODE: u32 = 1;
const LEAF_NODE: u32 = 2;

fn unpack_node_header(data: &[u8]) -> IResult<&[u8], NodeHeader> {
    let (i, _csum) = le_u32(data)?;
    let (i, flags) = le_u32(i)?;
    let (i, block) = le_u64(i)?;
    let (i, nr_entries) = le_u32(i)?;
    let (i, max_entries) = le_u32(i)?;
    let (i, value_size) = le_u32(i)?;
    let (i, _padding) = le_u32(i)?;

    Ok((
        i,
        NodeHeader {
            is_leaf: flags == LEAF_NODE,
            block,
            nr_entries,
            max_entries,
            value_size,
        },
    ))
}

enum Node<V: ValueType> {
    Internal {
        header: NodeHeader,
        keys: Vec<u64>,
        values: Vec<u64>,
    },
    Leaf {
        header: NodeHeader,
        keys: Vec<u64>,
        values: Vec<V::Value>,
    },
}

impl<V: ValueType> Node<V> {
    fn get_header(&self) -> &NodeHeader {
        match self {
            Node::Internal {
                header,
                keys: _k,
                values: _v,
            } => &header,
            Node::Leaf {
                header,
                keys: _k,
                values: _v,
            } => &header,
        }
    }

    fn is_leaf(&self) -> bool {
        self.get_header().is_leaf
    }
}

fn unpack_node_<V: ValueType>(data: &[u8]) -> IResult<&[u8], Node<V>> {
    use nom::multi::count;

    let (i, header) = unpack_node_header(data)?;
    let (i, keys) = count(le_u64, header.nr_entries as usize)(i)?;

    let nr_free = header.max_entries - header.nr_entries;
    let (i, _padding) = count(le_u64, nr_free as usize)(i)?;

    if header.is_leaf {
        let (i, values) = count(V::unpack, header.nr_entries as usize)(i)?;
        Ok((
            i,
            Node::Leaf {
                header,
                keys,
                values,
            },
        ))
    } else {
        let (i, values) = count(le_u64, header.nr_entries as usize)(i)?;
        Ok((
            i,
            Node::Internal {
                header,
                keys,
                values,
            },
        ))
    }
}

fn unpack_node<V: ValueType>(data: &[u8]) -> Result<Node<V>> {
    if let Ok((_i, node)) = unpack_node_(data) {
        Ok(node)
    } else {
        Err(anyhow!("couldn't unpack btree node"))
    }
}

//------------------------------------------

struct ValueU64;

impl ValueType for ValueU64 {
    type Value = u64;
    fn unpack(i: &[u8]) -> IResult<&[u8], u64> {
        le_u64(i)
    }
}

//------------------------------------------

trait NodeVisitor<V: ValueType> {
    fn visit<'a>(&mut self, w: &BTreeWalker, b: &Block, node: &Node<V>) -> Result<()>;
}

#[derive(Clone)]
struct BTreeWalker {
    engine: Arc<Mutex<AsyncIoEngine>>,
    seen: Arc<Mutex<FixedBitSet>>,
}

impl BTreeWalker {
    fn new(engine: AsyncIoEngine) -> BTreeWalker {
        let nr_blocks = engine.get_nr_blocks() as usize;
        let r: BTreeWalker = BTreeWalker {
            engine: Arc::new(Mutex::new(engine)),
            seen: Arc::new(Mutex::new(FixedBitSet::with_capacity(nr_blocks))),
        };
        r
    }

    fn walk_nodes<NV, V>(&mut self, visitor: &mut NV, bs: &Vec<u64>) -> Result<()>
    where
        NV: NodeVisitor<V>,
        V: ValueType,
    {
        let mut blocks = Vec::new();
        let seen = self.seen.lock().unwrap();
        for b in bs {
            if !seen[*b as usize] {
                blocks.push(Block::new(*b));
            }
        }
        drop(seen);

        let mut engine = self.engine.lock().unwrap();
        engine.read_many(&mut blocks)?;
        drop(engine);

        for b in blocks {
            self.walk_node(visitor, &b)?;
        }

        Ok(())
    }

    fn walk_node<NV, V>(&mut self, visitor: &mut NV, b: &Block) -> Result<()>
    where
        NV: NodeVisitor<V>,
        V: ValueType,
    {
        let mut seen = self.seen.lock().unwrap();
        seen.insert(b.loc as usize);
        drop(seen);

        let bt = checksum::metadata_block_type(b.get_data());
        if bt != checksum::BT::NODE {
            return Err(anyhow!("checksum failed for node {}, {:?}", b.loc, bt));
        }

        let node = unpack_node::<V>(&b.get_data())?;
        visitor.visit(self, &b, &node)?;

        if let Node::Internal {
            header: _h,
            keys: _k,
            values,
        } = node
        {
            self.walk_nodes(visitor, &values)?;
        }

        Ok(())
    }
}

//------------------------------------------

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

            let mut engine = w.engine.lock().unwrap();
            engine.read_many(&mut blocks)?;
            drop(engine);

	    // FIXME: with a thread pool we need to return errors another way.
            let nr_workers = 16;
            let pool = ThreadPool::new(nr_workers);

            let mut n = 0;
            for b in blocks {
                let thin_id = thin_ids[n];
                n += 1;
                
                let mut w = w.clone();
                pool.execute(move || {
                    let mut v = BottomLevelVisitor {};
                    w.walk_node(&mut v, &b);
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

    let mut seen = FixedBitSet::with_capacity(engine.get_nr_blocks() as usize);
    let mut w = BTreeWalker::new(engine);
    let mut visitor = TopLevelVisitor {};
    let result = w.walk_node(&mut visitor, &root)?;
    println!("read mapping tree in {} ms", now.elapsed().as_millis());

    Ok(())
}

//------------------------------------------
