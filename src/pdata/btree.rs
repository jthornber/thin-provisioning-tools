use byteorder::{LittleEndian, WriteBytesExt};
use nom::{number::complete::*, IResult};
use std::io;

use crate::io_engine::*;
use crate::pdata::btree_error;
use crate::pdata::unpack::*;

#[cfg(test)]
mod tests;

//------------------------------------------

pub use btree_error::BTreeError;
pub use btree_error::KeyRange;
pub use btree_error::*;

//------------------------------------------

const NODE_HEADER_SIZE: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeHeader {
    pub block: u64,
    pub is_leaf: bool,
    pub nr_entries: u32,
    pub max_entries: u32,
    pub value_size: u32,
}

#[allow(dead_code)]
const INTERNAL_NODE: u32 = 1;
const LEAF_NODE: u32 = 2;

impl Unpack for NodeHeader {
    fn disk_size() -> u32 {
        32
    }

    fn unpack(data: &[u8]) -> IResult<&[u8], NodeHeader> {
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
                block,
                is_leaf: flags == LEAF_NODE,
                nr_entries,
                max_entries,
                value_size,
            },
        ))
    }
}

impl Pack for NodeHeader {
    fn pack<W: WriteBytesExt>(&self, w: &mut W) -> io::Result<()> {
        // csum needs to be calculated right for the whole metadata block.
        w.write_u32::<LittleEndian>(0)?;

        let flags = if self.is_leaf {
            LEAF_NODE
        } else {
            INTERNAL_NODE
        };
        w.write_u32::<LittleEndian>(flags)?;
        w.write_u64::<LittleEndian>(self.block)?;
        w.write_u32::<LittleEndian>(self.nr_entries)?;
        w.write_u32::<LittleEndian>(self.max_entries)?;
        w.write_u32::<LittleEndian>(self.value_size)?;
        w.write_u32::<LittleEndian>(0)
    }
}

#[derive(Clone)]
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

impl<V: Unpack> Node<V> {
    pub fn get_header(&self) -> &NodeHeader {
        use Node::*;
        match self {
            Internal { header, .. } => header,
            Leaf { header, .. } => header,
        }
    }

    fn get_mut_header(&mut self) -> &mut NodeHeader {
        use Node::*;
        match self {
            Internal { header, .. } => header,
            Leaf { header, .. } => header,
        }
    }

    pub fn get_keys(&self) -> &[u64] {
        use Node::*;
        match self {
            Internal { keys, .. } => &keys[0..],
            Leaf { keys, .. } => &keys[0..],
        }
    }

    pub fn set_block(&mut self, b: u64) {
        self.get_mut_header().block = b;
    }
}

fn convert_result<V>(r: IResult<&[u8], V>) -> std::result::Result<(&[u8], V), NodeError> {
    r.map_err(|_e| NodeError::IncompleteData)
}

pub fn convert_io_err<V>(path: &[u64], r: std::io::Result<V>) -> Result<V> {
    r.map_err(|_| io_err(path))
}

pub fn unpack_node<V: Unpack>(
    path: &[u64],
    data: &[u8],
    ignore_non_fatal: bool,
    is_root: bool,
) -> Result<Node<V>> {
    unpack_node_raw(data, ignore_non_fatal, is_root).map_err(|e| node_err(path, e))
}

pub fn unpack_node_raw<V: Unpack>(
    data: &[u8],
    ignore_non_fatal: bool,
    is_root: bool,
) -> std::result::Result<Node<V>, NodeError> {
    use nom::multi::count;

    let (i, header) = NodeHeader::unpack(data).map_err(|_e| NodeError::IncompleteData)?;

    if header.is_leaf && header.value_size != V::disk_size() {
        return Err(NodeError::ValueSizeMismatch);
    }

    let elt_size = header.value_size + 8;
    if elt_size as usize * header.max_entries as usize + NODE_HEADER_SIZE > BLOCK_SIZE {
        return Err(NodeError::MaxEntriesTooLarge);
    }

    if header.nr_entries > header.max_entries {
        return Err(NodeError::NumEntriesTooLarge);
    }

    if !ignore_non_fatal {
        if header.max_entries % 3 != 0 {
            return Err(NodeError::MaxEntriesNotDivisible);
        }

        if !is_root {
            let min = header.max_entries / 3;
            if header.nr_entries < min {
                return Err(NodeError::NumEntriesTooSmall);
            }
        }
    }

    let (i, keys) = convert_result(count(le_u64, header.nr_entries as usize)(i))?;

    let mut last = None;
    for k in &keys {
        if let Some(l) = last {
            if k <= l {
                return Err(NodeError::KeysOutOfOrder);
            }
        }

        last = Some(k);
    }

    let nr_free = header.max_entries - header.nr_entries;
    let (i, _padding) = convert_result(count(le_u64, nr_free as usize)(i))?;

    if header.is_leaf {
        let (_i, values) = convert_result(count(V::unpack, header.nr_entries as usize)(i))?;

        Ok(Node::Leaf {
            header,
            keys,
            values,
        })
    } else {
        let (_i, values) = convert_result(count(le_u64, header.nr_entries as usize)(i))?;
        Ok(Node::Internal {
            header,
            keys,
            values,
        })
    }
}

/// Pack the given node ready to write to disk.
pub fn pack_node<W: WriteBytesExt, V: Pack + Unpack>(node: &Node<V>, w: &mut W) -> io::Result<()> {
    match node {
        Node::Internal {
            header,
            keys,
            values,
        } => {
            header.pack(w)?;
            for k in keys {
                w.write_u64::<LittleEndian>(*k)?;
            }

            // pad with zeroes
            for _i in keys.len()..header.max_entries as usize {
                w.write_u64::<LittleEndian>(0)?;
            }

            for v in values {
                v.pack(w)?;
            }
        }
        Node::Leaf {
            header,
            keys,
            values,
        } => {
            header.pack(w)?;
            for k in keys {
                w.write_u64::<LittleEndian>(*k)?;
            }

            // pad with zeroes
            for _i in keys.len()..header.max_entries as usize {
                w.write_u64::<LittleEndian>(0)?;
            }

            for v in values {
                v.pack(w)?;
            }
        }
    }

    Ok(())
}

//------------------------------------------

pub fn calc_max_entries<V: Unpack>() -> usize {
    let elt_size = 8 + V::disk_size() as usize;
    let total = (BLOCK_SIZE - NodeHeader::disk_size() as usize) / elt_size;
    total / 3 * 3
}

// Verify the checksum of a node
fn verify_checksum(b: &Block) -> std::result::Result<(), NodeError> {
    use crate::checksum;
    match checksum::metadata_block_type(b.get_data()) {
        checksum::BT::NODE => Ok(()),
        checksum::BT::UNKNOWN => Err(NodeError::ChecksumError),
        _ => Err(NodeError::NotANode),
    }
}

// Unpack a node and verify the checksum.
// The returned error is content based. Context information is up to the caller.
pub fn check_and_unpack_node<V: Unpack>(
    b: &Block,
    ignore_non_fatal: bool,
    is_root: bool,
) -> std::result::Result<Node<V>, NodeError> {
    verify_checksum(b)?;
    let node = unpack_node_raw::<V>(b.get_data(), ignore_non_fatal, is_root)?;
    if node.get_header().block != b.loc {
        return Err(NodeError::BlockNrMismatch);
    }
    Ok(node)
}
//------------------------------------------
