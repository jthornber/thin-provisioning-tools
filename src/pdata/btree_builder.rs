use anyhow::{anyhow, Result};
use byteorder::{LittleEndian, WriteBytesExt};
use std::collections::VecDeque;
use std::io::Cursor;
use std::sync::{Arc, Mutex};

use crate::checksum;
use crate::io_engine::*;
use crate::pdata::btree::*;
use crate::pdata::space_map::*;
use crate::pdata::unpack::*;

//------------------------------------------

fn pack_node<W: WriteBytesExt, V: Pack + Unpack>(node: &Node<V>, w: &mut W) -> Result<()> {
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

fn calc_max_entries<V: Unpack>() -> usize {
    let elt_size = 8 + V::disk_size() as usize;
    ((BLOCK_SIZE - NodeHeader::disk_size() as usize) / elt_size) as usize
}

//------------------------------------------

struct Entries<V> {
    max_entries: usize,
    entries: VecDeque<(u64, V)>,
}

enum Action<V> {
    Noop,
    WriteSingle {
        keys: Vec<u64>,
        values: Vec<V>,
    },
    WritePair {
        keys1: Vec<u64>,
        values1: Vec<V>,
        keys2: Vec<u64>,
        values2: Vec<V>,
    },
}

impl<V> Entries<V> {
    pub fn new(max_entries: usize) -> Entries<V> {
        Entries {
            max_entries,
            entries: VecDeque::new(),
        }
    }

    pub fn add_entry(&mut self, k: u64, v: V) -> Action<V> {
        let result = if self.full() {
            let (keys, values) = self.pop(self.max_entries);
            Action::WriteSingle { keys, values }
        } else {
            Action::Noop
        };

        self.entries.push_back((k, v));

        result
    }

    pub fn complete(&mut self) -> Action<V> {
        let n = self.entries.len();

        if n >= self.max_entries {
            let n1 = n / 2;
            let n2 = n - n1;
            let (keys1, values1) = self.pop(n1);
            let (keys2, values2) = self.pop(n2);

            Action::WritePair {
                keys1,
                values1,
                keys2,
                values2,
            }
        } else if n > 0 {
            let (keys, values) = self.pop(n);
            Action::WriteSingle { keys, values }
        } else {
            Action::Noop
        }
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

struct WriteBatcher {
    engine: Arc<Box<dyn IoEngine>>,
    sm: Arc<Mutex<dyn SpaceMap>>,

    batch_size: usize,
    queue: Vec<Block>,
}

impl WriteBatcher {
    fn new(
        engine: Arc<Box<dyn IoEngine>>,
        sm: Arc<Mutex<dyn SpaceMap>>,
        batch_size: usize,
    ) -> WriteBatcher {
        WriteBatcher {
            engine,
            sm,
            batch_size,
            queue: Vec::with_capacity(batch_size),
        }
    }

    fn alloc(&mut self) -> Result<u64> {
        let mut sm = self.sm.lock().unwrap();
        let b = sm.alloc()?;

        if b.is_none() {
            return Err(anyhow!("out of metadata space"));
        }

        Ok(b.unwrap())
    }

    fn write(&mut self, b: Block) -> Result<()> {
        checksum::write_checksum(&mut b.get_data(), checksum::BT::NODE)?;
        
        if self.queue.len() == self.batch_size {
            self.flush()?;
        }

        self.queue.push(b);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.engine.write_many(&self.queue)?;
        self.queue.clear();
        Ok(())
    }
}

//------------------------------------------

fn write_node_<V: Unpack + Pack>(w: &mut WriteBatcher, mut node: Node<V>) -> Result<(u64, u64)> {
    let keys = node.get_keys();
    let first_key = keys.first().unwrap_or(&0u64).clone();

    let loc = w.alloc()?;
    node.set_block(loc);
    
    let b = Block::new(loc);
    let mut cursor = Cursor::new(b.get_data());
    pack_node(&node, &mut cursor)?;
    w.write(b)?;

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
    pub fn new(engine: Arc<Box<dyn IoEngine>>, sm: Arc<Mutex<dyn SpaceMap>>) -> Builder<V> {
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
        let action = self.entries.add_entry(k, v);
        self.perform_action(action)
    }

    pub fn complete(mut self) -> Result<u64> {
        let action = self.entries.complete();
        self.perform_action(action)?;
        self.w.flush()?;
        Ok(self.root)
    }

    //--------------------

    fn add_internal_entry(&mut self, level: usize, k: u64, v: u64) -> Result<()> {
        if self.internal_entries.len() == level {
            self.internal_entries.push(Entries::new(self.max_internal_entries));
        }

        let action = self.internal_entries[level].add_entry(k, v);
        self.perform_internal_action(level, action)
    }

    fn perform_internal_action(&mut self, level: usize, action: Action<u64>) -> Result<()> {
        match action {
            Action::Noop => {}
            Action::WriteSingle { keys, values } => {
                let (k, loc) = write_internal(&mut self.w, keys, values)?;
                self.add_internal_entry(level + 1, k, loc)?;
                self.root = loc;
            }
            Action::WritePair {
                keys1,
                values1,
                keys2,
                values2,
            } => {
                let (k, loc) = write_leaf(&mut self.w, keys1, values1)?;
                self.add_internal_entry(level + 1, k, loc)?;

                let (k, loc) = write_leaf(&mut self.w, keys2, values2)?;
                self.add_internal_entry(level + 1, k, loc)?;
            }
        }

        Ok(())
    }

    fn perform_action<V2: Unpack + Pack>(&mut self, action: Action<V2>) -> Result<()> {
        match action {
            Action::Noop => {}
            Action::WriteSingle { keys, values } => {
                let (k, loc) = write_leaf(&mut self.w, keys, values)?;
                self.add_internal_entry(0, k, loc)?;
            }
            Action::WritePair {
                keys1,
                values1,
                keys2,
                values2,
            } => {
                let (k, loc) = write_leaf(&mut self.w, keys1, values1)?;
                self.add_internal_entry(0, k, loc)?;

                let (k, loc) = write_leaf(&mut self.w, keys2, values2)?;
                self.add_internal_entry(0, k, loc)?;
            }
        }

        Ok(())
    }
}

//------------------------------------------

#[test]
fn fail() {
    assert!(false);
}

//------------------------------------------
