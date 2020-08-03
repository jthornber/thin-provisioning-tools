use anyhow::{anyhow, Result};
use fixedbitset::FixedBitSet;
use nom::{number::complete::*, IResult};
use std::sync::{Arc, Mutex};

use crate::block_manager::*;
use crate::checksum;

//------------------------------------------

pub trait ValueType {
    type Value;
    fn unpack(data: &[u8]) -> IResult<&[u8], Self::Value>;
}

pub struct NodeHeader {
    is_leaf: bool,
    block: u64,
    nr_entries: u32,
    max_entries: u32,
    value_size: u32,
}

#[allow(dead_code)]
const INTERNAL_NODE: u32 = 1;
const LEAF_NODE: u32 = 2;

pub fn unpack_node_header(data: &[u8]) -> IResult<&[u8], NodeHeader> {
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

pub enum Node<V: ValueType> {
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

pub fn unpack_node_<V: ValueType>(data: &[u8]) -> IResult<&[u8], Node<V>> {
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

pub fn unpack_node<V: ValueType>(data: &[u8]) -> Result<Node<V>> {
    if let Ok((_i, node)) = unpack_node_(data) {
        Ok(node)
    } else {
        Err(anyhow!("couldn't unpack btree node"))
    }
}

//------------------------------------------

pub struct ValueU64;

impl ValueType for ValueU64 {
    type Value = u64;
    fn unpack(i: &[u8]) -> IResult<&[u8], u64> {
        le_u64(i)
    }
}

//------------------------------------------

pub trait NodeVisitor<V: ValueType> {
    fn visit<'a>(&mut self, w: &BTreeWalker, b: &Block, node: &Node<V>) -> Result<()>;
}

#[derive(Clone)]
pub struct BTreeWalker {
    pub engine: Arc<AsyncIoEngine>,
    pub seen: Arc<Mutex<FixedBitSet>>,
}

impl BTreeWalker {
    pub fn new(engine: AsyncIoEngine) -> BTreeWalker {
        let nr_blocks = engine.get_nr_blocks() as usize;
        let r: BTreeWalker = BTreeWalker {
            engine: Arc::new(engine),
            seen: Arc::new(Mutex::new(FixedBitSet::with_capacity(nr_blocks))),
        };
        r
    }

    pub fn walk_nodes<NV, V>(&mut self, visitor: &mut NV, bs: &Vec<u64>) -> Result<()>
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

        self.engine.read_many(&mut blocks)?;

        for b in blocks {
            self.walk_node(visitor, &b)?;
        }

        Ok(())
    }

    pub fn walk_node<NV, V>(&mut self, visitor: &mut NV, b: &Block) -> Result<()>
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
