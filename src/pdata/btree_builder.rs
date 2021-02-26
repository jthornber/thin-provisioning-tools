use anyhow::Result;
use byteorder::{LittleEndian, WriteBytesExt};
use std::collections::VecDeque;
use std::io::Cursor;
use std::sync::{Arc, Mutex};

use crate::checksum;
use crate::io_engine::*;
use crate::pdata::btree::*;
use crate::pdata::space_map::*;
use crate::pdata::unpack::*;
use crate::write_batcher::*;

//------------------------------------------

pub fn pack_node<W: WriteBytesExt, V: Pack + Unpack>(node: &Node<V>, w: &mut W) -> Result<()> {
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
    ((BLOCK_SIZE - NodeHeader::disk_size() as usize) / elt_size) as usize
}

//------------------------------------------

struct Entries<V> {
    pub max_entries: usize,
    entries: VecDeque<(u64, V)>,
}

enum Action<V> {
    EmitNode(Vec<u64>, Vec<V>),  // keys, values
}

use Action::*;

impl<V> Entries<V> {
    pub fn new(max_entries: usize) -> Entries<V> {
        Entries {
            max_entries,
            entries: VecDeque::new(),
        }
    }

    pub fn add_entry(&mut self, k: u64, v: V) -> Vec<Action<V>> {
        let mut result = Vec::new();
        
        if self.full() {
            let (keys, values) = self.pop(self.max_entries);
            result.push(EmitNode(keys, values));
        }

        self.entries.push_back((k, v));
        result
    }

    fn complete_(&mut self, result: &mut Vec<Action<V>>) {
        let n = self.entries.len();

        if n >= self.max_entries {
            let n1 = n / 2;
            let n2 = n - n1;
            let (keys1, values1) = self.pop(n1);
            let (keys2, values2) = self.pop(n2);

            result.push(EmitNode(keys1, values1));
            result.push(EmitNode(keys2, values2));
        } else if n > 0 {
            let (keys, values) = self.pop(n);
            result.push(EmitNode(keys, values));
        }
    }

    pub fn complete(&mut self) -> Vec<Action<V>> {
        let mut result = Vec::new();
        self.complete_(&mut result);
        result
    }

    fn full(&self) -> bool {
        self.entries.len() >= 2 * self.max_entries
    }

    fn pop(&mut self, count: usize) -> (Vec<u64>, Vec<V>) {
        let mut keys = Vec::new();
        let mut values = Vec::new();

        for _i in 0..count {
            let (k, v) = self.entries.pop_front().unwrap();
            keys.push(k);
            values.push(v);
        }

        (keys, values)
    }
}

//------------------------------------------

#[allow(dead_code)]
pub struct NodeSummary {
    block: u64,
    nr_entries: usize,
    key_low: u64,
    key_high: u64, // inclusive
}

//------------------------------------------

fn write_node_<V: Unpack + Pack>(w: &mut WriteBatcher, mut node: Node<V>) -> Result<(u64, u64)> {
    let keys = node.get_keys();
    let first_key = *keys.first().unwrap_or(&0u64);

    let loc = w.alloc()?;
    node.set_block(loc);

    let b = Block::new(loc);
    let mut cursor = Cursor::new(b.get_data());
    pack_node(&node, &mut cursor)?;
    w.write(b, checksum::BT::NODE)?;

    Ok((first_key, loc))
}

fn write_leaf<V: Unpack + Pack>(
    w: &mut WriteBatcher,
    keys: Vec<u64>,
    values: Vec<V>,
) -> Result<(u64, u64)> {
    let header = NodeHeader {
        block: 0,
        is_leaf: true,
        nr_entries: keys.len() as u32,
        max_entries: calc_max_entries::<V>() as u32,
        value_size: V::disk_size(),
    };

    let node = Node::Leaf {
        header,
        keys,
        values,
    };

    write_node_(w, node)
}

fn write_internal(w: &mut WriteBatcher, keys: Vec<u64>, values: Vec<u64>) -> Result<(u64, u64)> {
    let header = NodeHeader {
        block: 0,
        is_leaf: false,
        nr_entries: keys.len() as u32,
        max_entries: calc_max_entries::<u64>() as u32,
        value_size: u64::disk_size(),
    };

    let node: Node<u64> = Node::Internal {
        header,
        keys,
        values,
    };

    write_node_(w, node)
}

pub struct Builder<V: Unpack + Pack> {
    w: WriteBatcher,
    entries: Entries<V>,

    max_internal_entries: usize,
    internal_entries: Vec<Entries<u64>>,

    root: u64,
}

impl<V: Unpack + Pack> Builder<V> {
    pub fn new(
        engine: Arc<dyn IoEngine + Send + Sync>,
        sm: Arc<Mutex<dyn SpaceMap>>,
    ) -> Builder<V> {
        let max_entries = calc_max_entries::<V>();
        let max_internal_entries = calc_max_entries::<u64>();

        Builder {
            w: WriteBatcher::new(engine, sm, 256),
            entries: Entries::new(max_entries),
            max_internal_entries,
            internal_entries: Vec::new(),
            root: 0,
        }
    }

    pub fn add_entry(&mut self, k: u64, v: V) -> Result<()> {
        let actions = self.entries.add_entry(k, v);
        for a in actions {
            self.perform_action(a)?;
        }

        Ok(())
    }

    pub fn add_leaf_node(&mut self, leaf: &NodeSummary) -> Result<()> {
        match leaf.nr_entries {
            n if n == 0 => {
                // Do nothing
            },
            n if n < (self.entries.max_entries / 2) => {
                // FIXME: what if we've already queued a handful of entries for a node?
                // Add the entries individually
                todo!();
            },
            _n => {
                let actions = self.entries.complete();
                for a in actions {
                    self.perform_action(a)?;
                }
                self.add_internal_entry(0, leaf.key_low, leaf.block)?;
            }
        }

        Ok(())
    }

    pub fn complete(mut self) -> Result<u64> {
        let actions = self.entries.complete();
        for a in actions {
            self.perform_action(a)?;
        }
        self.w.flush()?;
        Ok(self.root)
    }

    //--------------------

    fn add_internal_entry(&mut self, level: usize, k: u64, v: u64) -> Result<()> {
        if self.internal_entries.len() == level {
            self.internal_entries
                .push(Entries::new(self.max_internal_entries));
        }

        let actions = self.internal_entries[level].add_entry(k, v);

        for a in actions {
            self.perform_internal_action(level, a)?;
        }

        Ok(())
    }

    fn perform_internal_action(&mut self, level: usize, action: Action<u64>) -> Result<()> {
        match action {
            EmitNode(keys, values) => {
                let (k, loc) = write_internal(&mut self.w, keys, values)?;
                self.add_internal_entry(level + 1, k, loc)?;
                self.root = loc;
            },
        }

        Ok(())
    }

    fn perform_action<V2: Unpack + Pack>(&mut self, action: Action<V2>) -> Result<()> {
        match action {
            EmitNode(keys, values) => {
                let (k, loc) = write_leaf(&mut self.w, keys, values)?;
                self.add_internal_entry(0, k, loc)?;
            },
        }

        Ok(())
    }
}

//------------------------------------------
