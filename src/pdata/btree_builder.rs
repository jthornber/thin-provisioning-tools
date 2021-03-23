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

/// A little ref counter abstraction.  Used to manage counts for btree
/// values (eg, the block/time in a thin mapping tree).
pub trait RefCounter<Value> {
    fn get(&self, v: &Value) -> Result<u32>;
    fn inc(&mut self, v: &Value) -> Result<()>;
    fn dec(&mut self, v: &Value) -> Result<()>;
}

/// Wraps a space map up to become a RefCounter.
struct SMRefCounter {
    sm: Arc<Mutex<dyn SpaceMap>>,
}

impl RefCounter<u64> for SMRefCounter {
    fn get(&self, v: &u64) -> Result<u32> {
        self.sm.lock().unwrap().get(*v)
    }

    fn inc(&mut self, v: &u64) -> Result<()> {
        self.sm.lock().unwrap().inc(*v, 1)
    }

    fn dec(&mut self, v: &u64) -> Result<()> {
        self.sm.lock().unwrap().dec(*v)?;
        Ok(())
    }
}

//------------------------------------------

// Building a btree for a given set of values is straight forward.
// But often we want to merge shared subtrees into the btree we're
// building, which _is_ complicated.  Requiring rebalancing of nodes,
// and careful copy-on-write operations so we don't disturb the shared
// subtree.
//
// To avoid these problems this code never produces shared internal nodes.
// With the large fan out of btrees this isn't really a problem; we'll
// allocate more nodes than optimum, but not many compared to the number
// of leaves.  Also we can pack the leaves much better than the kernel
// does due to out of order insertions.
//
// There are thus two stages to building a btree.
//
// i) Produce a list of populated leaves.  These leaves may well be shared.
// ii) Build the upper levels of the btree above the leaves.

//------------------------------------------

/// Pack the given node ready to write to disk.
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
    let total = ((BLOCK_SIZE - NodeHeader::disk_size() as usize) / elt_size) as usize;
    total / 3 * 3
}

pub struct WriteResult {
    first_key: u64,
    loc: u64,
}

/// Write a node to a free metadata block.
fn write_node_<V: Unpack + Pack>(w: &mut WriteBatcher, mut node: Node<V>) -> Result<WriteResult> {
    let keys = node.get_keys();
    let first_key = keys.first().unwrap_or(&0u64).clone();

    let loc = w.alloc()?;
    node.set_block(loc);

    let b = Block::new(loc);
    let mut cursor = Cursor::new(b.get_data());
    pack_node(&node, &mut cursor)?;
    w.write(b, checksum::BT::NODE)?;

    Ok(WriteResult { first_key, loc })
}

/// A node writer takes a Vec of values and packs them into
/// a btree node.  It's up to the specific implementation to
/// decide if it produces internal or leaf nodes.
pub trait NodeIO<V: Unpack + Pack> {
    fn write(&self, w: &mut WriteBatcher, keys: Vec<u64>, values: Vec<V>) -> Result<WriteResult>;
    fn read(
        &self,
        engine: &Arc<dyn IoEngine + Send + Sync>,
        block: u64,
    ) -> Result<(Vec<u64>, Vec<V>)>;
}

struct LeafIO {}

impl<V: Unpack + Pack> NodeIO<V> for LeafIO {
    fn write(&self, w: &mut WriteBatcher, keys: Vec<u64>, values: Vec<V>) -> Result<WriteResult> {
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

    fn read(
        &self,
        engine: &Arc<dyn IoEngine + Send + Sync>,
        block: u64,
    ) -> Result<(Vec<u64>, Vec<V>)> {
        let b = engine.read(block)?;
        let path = Vec::new();
        match unpack_node::<V>(&path, b.get_data(), true, true)? {
            Node::Internal { .. } => {
                panic!("unexpected internal node");
            }
            Node::Leaf { keys, values, .. } => Ok((keys, values)),
        }
    }
}

struct InternalIO {}

impl NodeIO<u64> for InternalIO {
    fn write(&self, w: &mut WriteBatcher, keys: Vec<u64>, values: Vec<u64>) -> Result<WriteResult> {
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

    fn read(
        &self,
        engine: &Arc<dyn IoEngine + Send + Sync>,
        block: u64,
    ) -> Result<(Vec<u64>, Vec<u64>)> {
        let b = engine.read(block)?;
        let path = Vec::new();
        match unpack_node::<u64>(&path, b.get_data(), true, true)? {
            Node::Internal { keys, values, .. } => Ok((keys, values)),
            Node::Leaf { .. } => {
                panic!("unexpected leaf node");
            }
        }
    }
}

//------------------------------------------

/// This takes a sequence of values or nodes, and builds a vector of leaf nodes.
/// Care is taken to make sure that all nodes are at least half full unless there's
/// only a single node.
pub struct NodeBuilder<V: Pack + Unpack> {
    batcher: WriteBatcher,
    nio: Box<dyn NodeIO<V>>,
    value_rc: Box<dyn RefCounter<V>>,
    max_entries_per_node: usize,
    values: VecDeque<(u64, V)>,
    nodes: Vec<NodeSummary>,
}

/// When the builder is including pre-built nodes it has to decide whether
/// to use the node as given, or read it and import the values directly
/// for balancing reasons.  This struct is used to stop us re-reading
/// the NodeHeaders of nodes that are shared multiple times.
#[derive(Clone)]
pub struct NodeSummary {
    block: u64,
    key: u64,
    nr_entries: usize,

    /// This node was passed in pre-built.  Important for deciding if
    /// we need to adjust the ref counts if we unpack.
    shared: bool,
}

impl<V: Pack + Unpack + Clone> NodeBuilder<V> {
    /// Create a new NodeBuilder
    pub fn new(
        batcher: WriteBatcher,
        nio: Box<dyn NodeIO<V>>,
        value_rc: Box<dyn RefCounter<V>>,
    ) -> Self {
        NodeBuilder {
            batcher,
            nio,
            value_rc,
            max_entries_per_node: calc_max_entries::<V>(),
            values: VecDeque::new(),
            nodes: Vec::new(),
        }
    }
    /// Push a single value.  This may emit a new node, hence the Result
    /// return type.  The value's ref count will be incremented.
    pub fn push_value(&mut self, key: u64, val: V) -> Result<()> {
        // Have we got enough values to emit a node?  We try and keep
        // at least max_entries_per_node entries unflushed so we
        // can ensure the final node is balanced properly.
        if self.values.len() == self.max_entries_per_node * 2 {
            self.emit_node()?;
        }

        self.value_rc.inc(&val)?;
        self.values.push_back((key, val));
        Ok(())
    }

    /// Push a number of prebuilt, shared nodes.  The builder may decide to not
    /// use a shared node, instead reading the values and packing them
    /// directly.  This may do IO to emit nodes, so returns a Result.
    /// Any shared nodes that are used have their block incremented in
    /// the space map.  Will only increment the ref count for values
    /// contained in the nodes if it unpacks them.
    pub fn push_nodes(&mut self, nodes: &Vec<NodeSummary>) -> Result<()> {
        assert!(nodes.len() > 0);

        // As a sanity check we make sure that all the shared nodes contain the
        // minimum nr of entries.
        let half_full = self.max_entries_per_node / 2;
        for n in nodes {
            if n.nr_entries < half_full {
                panic!("under populated node");
            }
        }

        // Decide if we're going to use the pre-built nodes.
        if self.values.len() < half_full {
            // To avoid writing an under populated node we have to grab some
            // values from the first of the shared nodes.
            let (keys, values) = self.read_node(nodes.get(0).unwrap().block)?;

            for i in 0..keys.len() {
                self.value_rc.inc(&values[i])?;
                self.values.push_back((keys[i], values[i].clone()));
            }

            // Flush all the values.
            self.emit_all()?;

            // Add the remaining nodes.
            for i in 1..nodes.len() {
                let n = nodes.get(i).unwrap();
                self.batcher.sm.lock().unwrap().inc(n.block, 1)?;
                self.nodes.push(n.clone());
            }
        } else {
            // Flush all the values.
            self.emit_all()?;

            // add the nodes
            for n in nodes {
                self.batcher.sm.lock().unwrap().inc(n.block, 1)?;
                self.nodes.push(n.clone());
            }
        }

        Ok(())
    }

    /// Signal that no more values or nodes will be pushed.  Returns a
    /// vector of the built nodes.  Consumes the builder.
    pub fn complete(mut self) -> Result<Vec<NodeSummary>> {
        let half_full = self.max_entries_per_node / 2;

        if (self.nodes.len() > 0) && (self.values.len() < half_full) {
            // We don't have enough values to emit a node.  So we're going to
            // have to rebalance with the previous node.
            self.unshift_node()?;
        }

        self.emit_all()?;
        Ok(self.nodes)
    }

    //-------------------------

    // We're only interested in the keys and values from the node, and
    // not whether it's a leaf or internal node.
    fn read_node(&self, block: u64) -> Result<(Vec<u64>, Vec<V>)> {
        self.nio.read(&self.batcher.engine, block)
    }

    /// Writes a node with the first 'nr_entries' values.
    fn emit_values(&mut self, nr_entries: usize) -> Result<()> {
        assert!(self.values.len() <= nr_entries);

        // Write the node
        let mut keys = Vec::new();
        let mut values = Vec::new();

        for _i in 0..nr_entries {
            let (k, v) = self.values.pop_front().unwrap();
            keys.push(k);
            values.push(v);
        }

        let wresult = self.nio.write(&mut self.batcher, keys, values)?;

        // Push a summary to the 'nodes' vector.
        self.nodes.push(NodeSummary {
            block: wresult.loc,
            key: wresult.first_key,
            nr_entries,
            shared: false,
        });
        Ok(())
    }

    /// Writes a full node.
    fn emit_node(&mut self) -> Result<()> {
        self.emit_values(self.max_entries_per_node)
    }

    /// Emits all remaining values.  Panics if there are more than 2 *
    /// max_entries_per_node values.
    fn emit_all(&mut self) -> Result<()> {
        match self.values.len() {
            0 => {
                // There's nothing to emit
                Ok(())
            }
            n if n <= self.max_entries_per_node => {
                // Emit a single node.
                self.emit_values(n)
            }
            n if n <= self.max_entries_per_node * 2 => {
                // Emit two nodes.
                let n1 = n / 2;
                let n2 = n - n1;
                self.emit_values(n1)?;
                self.emit_values(n2)
            }
            _ => {
                panic!("self.values shouldn't have more than 2 * max_entries_per_node entries");
            }
        }
    }

    /// Pops the last node, and prepends it's values to 'self.values'.  Used
    /// to rebalance when we have insufficient values for a final node.  The
    /// node is decremented in the space map.
    fn unshift_node(&mut self) -> Result<()> {
        let ls = self.nodes.pop().unwrap();
        let (keys, values) = self.read_node(ls.block)?;
        self.batcher.sm.lock().unwrap().dec(ls.block)?;

        let mut vals = VecDeque::new();

        for i in 0..keys.len() {
            // We only need to inc the values if the node was pre built.
            if ls.shared {
                self.value_rc.inc(&values[i])?;
            }
            vals.push_back((keys[i], values[i].clone()));
        }

        vals.append(&mut self.values);
        std::mem::swap(&mut self.values, &mut vals);

        Ok(())
    }
}

//------------------------------------------

pub struct Builder<V: Unpack + Pack> {
    engine: Arc<dyn IoEngine + Send + Sync>,
    sm: Arc<Mutex<dyn SpaceMap>>,
    leaf_builder: NodeBuilder<V>,
}

const BATCH_SIZE: usize = 128;

impl<V: Unpack + Pack + Clone> Builder<V> {
    pub fn new(
        engine: Arc<dyn IoEngine + Send + Sync>,
        sm: Arc<Mutex<dyn SpaceMap>>,
        value_rc: Box<dyn RefCounter<V>>,
    ) -> Builder<V> {
        Builder {
            engine: engine.clone(),
            sm: sm.clone(),
            leaf_builder: NodeBuilder::new(
                WriteBatcher::new(engine.clone(), sm.clone(), BATCH_SIZE),
                Box::new(LeafIO {}),
                value_rc,
            ),
        }
    }

    pub fn push_value(&mut self, k: u64, v: V) -> Result<()> {
        self.leaf_builder.push_value(k, v)
    }

    pub fn push_leaves(&mut self, leaves: &Vec<NodeSummary>) -> Result<()> {
        self.leaf_builder.push_nodes(leaves)
    }

    pub fn complete(self) -> Result<u64> {
        let mut nodes = self.leaf_builder.complete()?;

        // Now we iterate, adding layers of internal nodes until we end
        // up with a single root.
        while nodes.len() > 1 {
            let mut builder = NodeBuilder::new(
                WriteBatcher::new(self.engine.clone(), self.sm.clone(), BATCH_SIZE),
                Box::new(InternalIO {}),
                Box::new(SMRefCounter {
                    sm: self.sm.clone(),
                }),
            );

            for n in nodes {
                builder.push_value(n.key, n.block)?;
            }

            nodes = builder.complete()?;
        }

        assert!(nodes.len() == 1);
        Ok(nodes[0].block)
    }
}

//------------------------------------------

/*
=======
fn write_node_<V: Unpack + Pack>(w: &mut WriteBatcher, mut node: Node<V>) -> Result<(u64, u64)> {
    let keys = node.get_keys();
    let first_key = *keys.first().unwrap_or(&0u64);
>>>>>>> main

=======
fn write_node_<V: Unpack + Pack>(w: &mut WriteBatcher, mut node: Node<V>) -> Result<(u64, u64)> {
    let keys = node.get_keys();
    let first_key = *keys.first().unwrap_or(&0u64);
>>>>>>> main
*/
