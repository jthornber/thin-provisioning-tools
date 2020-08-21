use anyhow::{anyhow, Result};
use nom::{number::complete::*, IResult};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::checksum;
use crate::io_engine::*;
use crate::pdata::space_map::*;
use crate::pdata::unpack::*;

// FIXME: check that keys are in ascending order between nodes.

//------------------------------------------

const NODE_HEADER_SIZE: usize = 32;

pub struct NodeHeader {
    is_leaf: bool,
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
    let (i, _block) = le_u64(i)?;
    let (i, nr_entries) = le_u32(i)?;
    let (i, max_entries) = le_u32(i)?;
    let (i, value_size) = le_u32(i)?;
    let (i, _padding) = le_u32(i)?;

    Ok((
        i,
        NodeHeader {
            is_leaf: flags == LEAF_NODE,
            nr_entries,
            max_entries,
            value_size,
        },
    ))
}

pub enum Node<V: Unpack> {
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

pub fn unpack_node<V: Unpack>(
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
        return node_err("nr_entries > max_entries".to_string());
    }

    if !ignore_non_fatal {
        if header.max_entries % 3 != 0 {
            return node_err("max_entries is not divisible by 3".to_string());
        }

        if !is_root {
            let min = header.max_entries / 3;
            if header.nr_entries < min {
                return node_err("too few entries".to_string());
            }
        }
    }

    let (i, keys) = to_any(count(le_u64, header.nr_entries as usize)(i))?;

    let mut last = None;
    for k in &keys {
        if let Some(l) = last {
            if k <= l {
                return node_err("keys out of order".to_string());
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

pub trait NodeVisitor<V: Unpack> {
    // &self is deliberately non mut to allow the walker to use multiple threads.
    fn visit(&self, header: &NodeHeader, keys: &[u64], values: &[V]) -> Result<()>;
}

#[derive(Clone)]
pub struct BTreeWalker {
    pub engine: Arc<dyn IoEngine + Send + Sync>,
    pub sm: Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    ignore_non_fatal: bool,
}

impl BTreeWalker {
    pub fn new(engine: Arc<dyn IoEngine + Send + Sync>, ignore_non_fatal: bool) -> BTreeWalker {
        let nr_blocks = engine.get_nr_blocks() as usize;
        let r: BTreeWalker = BTreeWalker {
            engine,
            sm: Arc::new(Mutex::new(RestrictedSpaceMap::new(nr_blocks as u64))),
            ignore_non_fatal,
        };
        r
    }

    pub fn new_with_sm(
        engine: Arc<dyn IoEngine + Send + Sync>,
        sm: Arc<Mutex<dyn SpaceMap + Send + Sync>>,
        ignore_non_fatal: bool,
    ) -> Result<BTreeWalker> {
        {
            let sm = sm.lock().unwrap();
            assert_eq!(sm.get_nr_blocks()?, engine.get_nr_blocks());
        }

        Ok(BTreeWalker {
            engine,
            sm,
            ignore_non_fatal,
        })
    }

    // Atomically increments the ref count, and returns the _old_ count.
    fn sm_inc(&self, b: u64) -> Result<u32> {
        let mut sm = self.sm.lock().unwrap();
        let count = sm.get(b)?;
        sm.inc(b, 1)?;
        Ok(count)
    }

    fn walk_nodes<NV, V>(&mut self, visitor: &mut NV, bs: &[u64]) -> Result<()>
    where
        NV: NodeVisitor<V>,
        V: Unpack,
    {
        let mut blocks = Vec::with_capacity(bs.len());
        for b in bs {
            if self.sm_inc(*b)? == 0 {
                blocks.push(Block::new(*b));
            }
        }

        self.engine.read_many(&mut blocks)?;

        for b in blocks {
            self.walk_node(visitor, &b, false)?;
        }

        Ok(())
    }

    fn walk_node<NV, V>(&mut self, visitor: &mut NV, b: &Block, is_root: bool) -> Result<()>
    where
        NV: NodeVisitor<V>,
        V: Unpack,
    {
        use Node::*;

        let bt = checksum::metadata_block_type(b.get_data());
        if bt != checksum::BT::NODE {
            return Err(anyhow!("checksum failed for node {}, {:?}", b.loc, bt));
        }

        let node = unpack_node::<V>(&b.get_data(), self.ignore_non_fatal, is_root)?;

        match node {
            Internal {
                header: _h,
                keys: _k,
                values,
            } => {
                self.walk_nodes(visitor, &values)?;
            }
            Leaf { header, keys, values } => {
                visitor.visit(&header, &keys, &values)?;
            }
        }

        Ok(())
    }

    pub fn walk_b<NV, V>(&mut self, visitor: &mut NV, root: &Block) -> Result<()>
    where
        NV: NodeVisitor<V>,
        V: Unpack,
    {
        if self.sm_inc(root.loc)? > 0 {
            Ok(())
        } else {
            self.walk_node(visitor, &root, true)
        }
    }

    pub fn walk<NV, V>(&mut self, visitor: &mut NV, root: u64) -> Result<()>
    where
        NV: NodeVisitor<V>,
        V: Unpack,
    {
        if self.sm_inc(root)? > 0 {
            Ok(())
        } else {
            let mut root = Block::new(root);
            self.engine.read(&mut root)?;
            self.walk_node(visitor, &root, true)
        }
    }
}

//------------------------------------------

struct ValueCollector<V> {
    values: Mutex<BTreeMap<u64, V>>,
}

impl<V> ValueCollector<V> {
    fn new() -> ValueCollector<V> {
        ValueCollector {
            values: Mutex::new(BTreeMap::new()),
        }
    }
}

impl<V: Unpack + Clone> NodeVisitor<V> for ValueCollector<V> {
    fn visit(&self, _h: &NodeHeader, keys: &[u64], values: &[V]) -> Result<()> {
        let mut vals = self.values.lock().unwrap();
        for n in 0..keys.len() {
            vals.insert(keys[n], values[n].clone());
        }

        Ok(())
    }
}

pub fn btree_to_map<V: Unpack + Clone>(
    engine: Arc<dyn IoEngine + Send + Sync>,
    ignore_non_fatal: bool,
    root: u64,
) -> Result<BTreeMap<u64, V>> {
    let mut walker = BTreeWalker::new(engine, ignore_non_fatal);
    let mut visitor = ValueCollector::<V>::new();

    walker.walk(&mut visitor, root)?;
    Ok(visitor.values.into_inner().unwrap())
}

pub fn btree_to_map_with_sm<V: Unpack + Clone>(
    engine: Arc<dyn IoEngine + Send + Sync>,
    sm: Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    ignore_non_fatal: bool,
    root: u64,
) -> Result<BTreeMap<u64, V>> {
    let mut walker = BTreeWalker::new_with_sm(engine, sm, ignore_non_fatal)?;
    let mut visitor = ValueCollector::<V>::new();

    walker.walk(&mut visitor, root)?;
    Ok(visitor.values.into_inner().unwrap())
}

//------------------------------------------
