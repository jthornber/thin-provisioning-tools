use anyhow::{anyhow, Result};
use fixedbitset::FixedBitSet;
use nom::{number::complete::*, IResult};
use std::sync::{Arc, Mutex};

use crate::block_manager::*;
use crate::checksum;

//------------------------------------------

pub trait ValueType {
    // The size of the value when on disk.
    fn disk_size() -> u32;
    fn unpack(data: &[u8]) -> IResult<&[u8], Self>
    where
        Self: std::marker::Sized;
}

const NODE_HEADER_SIZE: usize = 32;

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
        values: Vec<V>,
    },
}

pub fn node_err<V>(msg: String) -> Result<V> {
    let msg = format!("btree node error: {}", msg);
    Err(anyhow!(msg))
}

pub fn to_any<'a, V>(r: IResult<&'a [u8], V>) -> Result<(&'a [u8], V)> {
    if let Ok((i, v)) = r {
        Ok((i, v))
    } else {
        Err(anyhow!("btree node error: parse error"))
    }
}

pub fn unpack_node<V: ValueType>(
    data: &[u8],
    ignore_non_fatal: bool,
    is_root: bool,
) -> Result<Node<V>> {
    use nom::multi::count;

    let (i, header) = to_any(unpack_node_header(data))?;

    if header.is_leaf && header.value_size != V::disk_size() {
        return node_err(format!(
            "value_size mismatch: expected {}, was {}",
            V::disk_size(),
            header.value_size
        ));
    }

    let elt_size = header.value_size + 8;
    if elt_size as usize * header.max_entries as usize + NODE_HEADER_SIZE > BLOCK_SIZE {
        return node_err(format!("max_entries is too large ({})", header.max_entries));
    }

    if header.nr_entries > header.max_entries {
        return node_err(format!("nr_entries > max_entries"));
    }

    if !ignore_non_fatal {
        if header.max_entries % 3 != 0 {
            return node_err(format!("max_entries is not divisible by 3"));
        }

        if !is_root {
            let min = header.max_entries / 3;
            if header.nr_entries < min {
                return node_err(format!("too few entries"));
            }
        }
    }

    let (i, keys) = to_any(count(le_u64, header.nr_entries as usize)(i))?;

    let mut last = None;
    for k in &keys {
        if let Some(l) = last {
            if k <= l {
                return node_err(format!("keys out of order"));
            }
        }

        last = Some(k);
    }

    let nr_free = header.max_entries - header.nr_entries;
    let (i, _padding) = to_any(count(le_u64, nr_free as usize)(i))?;

    if header.is_leaf {
        let (_i, values) = to_any(count(V::unpack, header.nr_entries as usize)(i))?;

        Ok(Node::Leaf {
            header,
            keys,
            values,
        })
    } else {
        let (_i, values) = to_any(count(le_u64, header.nr_entries as usize)(i))?;
        Ok(Node::Internal {
            header,
            keys,
            values,
        })
    }
}

//------------------------------------------

impl ValueType for u64 {
    fn disk_size() -> u32 {
        8
    }

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
    ignore_non_fatal: bool,
}

impl BTreeWalker {
    pub fn new(engine: Arc<AsyncIoEngine>, ignore_non_fatal: bool) -> BTreeWalker {
        let nr_blocks = engine.get_nr_blocks() as usize;
        let r: BTreeWalker = BTreeWalker {
            engine: engine,
            seen: Arc::new(Mutex::new(FixedBitSet::with_capacity(nr_blocks))),
            ignore_non_fatal,
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

        self.engine.read_many(&mut blocks)?;

        for b in blocks {
            self.walk_node(visitor, &b, false)?;
        }

        Ok(())
    }

    fn walk_node<NV, V>(&mut self, visitor: &mut NV, b: &Block, is_root: bool) -> Result<()>
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

        let node = unpack_node::<V>(&b.get_data(), self.ignore_non_fatal, is_root)?;
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

    pub fn walk_b<NV, V>(&mut self, visitor: &mut NV, root: &Block) -> Result<()>
    where
        NV: NodeVisitor<V>,
        V: ValueType,
    {
        self.walk_node(visitor, &root, true)
    }

    pub fn walk<NV, V>(&mut self, visitor: &mut NV, root: u64) -> Result<()>
    where
        NV: NodeVisitor<V>,
        V: ValueType,
    {
        let mut root = Block::new(root);
        self.engine.read(&mut root)?;
        self.walk_node(visitor, &root, true)
    }
}

//------------------------------------------
