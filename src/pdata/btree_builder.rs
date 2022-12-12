use anyhow::{anyhow, Result};
use std::collections::VecDeque;
use std::io::Cursor;
use std::sync::{Arc, Mutex};

use crate::checksum;
use crate::pdata::btree::*;
use crate::pdata::space_map::*;
use crate::pdata::unpack::*;
use crate::write_batcher::*;

#[cfg(test)]
pub mod test_utils;

#[cfg(test)]
mod tests;

//------------------------------------------

/// A little ref counter abstraction.  Used to manage counts for btree
/// values (eg, the block/time in a thin mapping tree).
pub trait RefCounter<Value> {
    fn get(&self, v: &Value) -> Result<u32>;
    fn inc(&mut self, v: &Value) -> Result<()>;
    fn dec(&mut self, v: &Value) -> Result<()>;
}

pub struct NoopRC {}

impl<Value> RefCounter<Value> for NoopRC {
    fn get(&self, _v: &Value) -> Result<u32> {
        Ok(0)
    }
    fn inc(&mut self, _v: &Value) -> Result<()> {
        Ok(())
    }
    fn dec(&mut self, _v: &Value) -> Result<()> {
        Ok(())
    }
}

/// Wraps a space map up to become a RefCounter.
pub struct SMRefCounter {
    sm: Arc<Mutex<dyn SpaceMap>>,
}

impl SMRefCounter {
    pub fn new(sm: Arc<Mutex<dyn SpaceMap>>) -> SMRefCounter {
        SMRefCounter { sm }
    }
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

pub struct WriteResult {
    first_key: u64,
    loc: u64,
}

/// Write a node to a free metadata block.
fn write_node_<V: Unpack + Pack>(w: &mut WriteBatcher, mut node: Node<V>) -> Result<WriteResult> {
    let keys = node.get_keys();
    let first_key = *keys.first().unwrap_or(&0u64);

    let b = w.alloc()?;
    node.set_block(b.loc);

    let mut cursor = Cursor::new(b.get_data());
    pack_node(&node, &mut cursor)?;
    let loc = b.loc;
    w.write(b, checksum::BT::NODE)?;

    Ok(WriteResult { first_key, loc })
}

/// A node writer takes a Vec of values and packs them into
/// a btree node.  It's up to the specific implementation to
/// decide if it produces internal or leaf nodes.
pub trait NodeIO<V: Unpack + Pack> {
    fn write(&self, w: &mut WriteBatcher, keys: Vec<u64>, values: Vec<V>) -> Result<WriteResult>;
    fn read(&self, w: &mut WriteBatcher, block: u64) -> Result<(Vec<u64>, Vec<V>)>;
}

pub struct LeafIO {}

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

    fn read(&self, w: &mut WriteBatcher, block: u64) -> Result<(Vec<u64>, Vec<V>)> {
        let b = w.read(block)?;
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

    fn read(&self, w: &mut WriteBatcher, block: u64) -> Result<(Vec<u64>, Vec<u64>)> {
        let b = w.read(block)?;
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
    nio: Box<dyn NodeIO<V>>,
    value_rc: Box<dyn RefCounter<V>>,
    max_entries_per_node: usize,
    values: VecDeque<(u64, V)>,
    nodes: Vec<NodeSummary>,
    shared: bool,
    last_key: Option<u64>,
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
    pub fn new(nio: Box<dyn NodeIO<V>>, value_rc: Box<dyn RefCounter<V>>, shared: bool) -> Self {
        NodeBuilder {
            nio,
            value_rc,
            max_entries_per_node: calc_max_entries::<V>(),
            values: VecDeque::new(),
            nodes: Vec::new(),
            shared,
            last_key: None,
        }
    }

    pub fn check_ordered_key(&mut self, key: u64) -> Result<()> {
        if let Some(last) = self.last_key {
            if key <= last {
                return Err(anyhow!("keys out of order"));
            }
        }
        self.last_key = Some(key);

        Ok(())
    }

    /// Push a single value.  This may emit a new node, hence the Result
    /// return type.  The value's ref count will be incremented.
    pub fn push_value(&mut self, w: &mut WriteBatcher, key: u64, val: V) -> Result<()> {
        // Unshift the previously pushed node since it is not the root
        let half_full = self.max_entries_per_node / 2;
        if self.nodes.len() == 1 && (self.nodes.last().unwrap().nr_entries < half_full) {
            self.unshift_node(w)?;
        }
        // Have we got enough values to emit a node?  We try and keep
        // at least max_entries_per_node entries unflushed so we
        // can ensure the final node is balanced properly.
        else if self.values.len() == self.max_entries_per_node * 2 {
            self.emit_node(w)?;
        }

        self.check_ordered_key(key)?;

        self.value_rc.inc(&val)?;
        self.values.push_back((key, val));
        Ok(())
    }

    // To avoid writing an under populated node we have to grab some
    // values from the first of the shared nodes.
    fn append_values(&mut self, w: &mut WriteBatcher, node: &NodeSummary) -> Result<()> {
        let (keys, values) = self.read_node(w, node.block)?;

        for i in 0..keys.len() {
            self.value_rc.inc(&values[i])?;
            self.values.push_back((keys[i], values[i].clone()));
        }

        Ok(())
    }

    /// Push a number of prebuilt, shared nodes.  The builder may decide to not
    /// use a shared node, instead reading the values and packing them
    /// directly.  This may do IO to emit nodes, so returns a Result.
    /// Any shared nodes that are used have their block incremented in
    /// the space map.  Will only increment the ref count for values
    /// contained in the nodes if it unpacks them.
    pub fn push_nodes(&mut self, w: &mut WriteBatcher, nodes: &[NodeSummary]) -> Result<()> {
        assert!(!nodes.is_empty());

        // Assume that the node is a shared root if it is the first comer.
        // A rooted leaf could have any number of entries.
        let maybe_root = (nodes.len() == 1) && self.nodes.is_empty() && self.values.is_empty();
        if maybe_root {
            let n = &nodes[0];
            w.sm.lock().unwrap().inc(n.block, 1)?;
            self.nodes.push(n.clone());
            return Ok(());
        }

        // As a sanity check we make sure that all the shared nodes contain the
        // minimum nr of entries.
        // A single shared node could be possibly under populated (less than half-full)
        // due to btree removal, or even underfull (<33% residency) due to kernel issues.
        // Those kinds of nodes will be merged into their siblings.
        let half_full = self.max_entries_per_node / 2;
        if nodes.len() > 1 {
            for n in nodes {
                if n.nr_entries < half_full {
                    panic!("under populated node");
                }
            }
        }

        // Ensure the ordering of keys
        if let Some(last) = self.last_key {
            if nodes[0].key <= last {
                return Err(anyhow!("keys out of order"));
            }
        }

        // Unshift the previously pushed node since it is not the root
        if self.nodes.len() == 1 && (self.nodes[0].nr_entries < half_full) {
            self.unshift_node(w)?;
        }

        // Decide if we're going to use the pre-built nodes.
        if !self.values.is_empty() && (self.values.len() < half_full) {
            self.append_values(w, &nodes[0])?;

            if nodes.len() > 1 {
                // Flush all the values while there are succeeding nodes.
                self.emit_all(w)?;

                // Add the remaining nodes.
                for n in &nodes[1..] {
                    w.sm.lock().unwrap().inc(n.block, 1)?;
                    self.nodes.push(n.clone());
                }
            }
        } else {
            // Flush all the values since we've collected sufficient numbers of entries.
            self.emit_all(w)?;

            if nodes[0].nr_entries < half_full {
                // An under populated nodes[0] implies nodes.len() == 1,
                // and that has to be merged into their siblings.
                self.append_values(w, &nodes[0])?;
            } else {
                // Add the nodes.
                for n in nodes {
                    w.sm.lock().unwrap().inc(n.block, 1)?;
                    self.nodes.push(n.clone());
                }
            }
        }

        // It's the first key of the last pushed node. Just for roughly checking.
        self.last_key = Some(nodes.last().unwrap().key);

        Ok(())
    }

    /// Signal that no more values or nodes will be pushed.  Returns a
    /// vector of the built nodes.  Consumes the builder.
    pub fn complete(mut self, w: &mut WriteBatcher) -> Result<Vec<NodeSummary>> {
        let half_full = self.max_entries_per_node / 2;

        if !self.values.is_empty() && (self.values.len() < half_full) && !self.nodes.is_empty() {
            // We don't have enough values to emit a node.  So we're going to
            // have to rebalance with the previous node.
            self.unshift_node(w)?;
        }

        self.emit_all(w)?;

        if self.nodes.is_empty() {
            self.emit_empty_leaf(w)?
        }

        Ok(self.nodes)
    }

    //-------------------------

    // We're only interested in the keys and values from the node, and
    // not whether it's a leaf or internal node.
    fn read_node(&self, w: &mut WriteBatcher, block: u64) -> Result<(Vec<u64>, Vec<V>)> {
        self.nio.read(w, block)
    }

    /// Writes a node with the first 'nr_entries' values.
    fn emit_values(&mut self, w: &mut WriteBatcher, nr_entries: usize) -> Result<()> {
        assert!(nr_entries <= self.values.len());

        // Write the node
        let mut keys = Vec::new();
        let mut values = Vec::new();

        for _i in 0..nr_entries {
            let (k, v) = self.values.pop_front().unwrap();
            keys.push(k);
            values.push(v);
        }

        let wresult = self.nio.write(w, keys, values)?;

        // Push a summary to the 'nodes' vector.
        self.nodes.push(NodeSummary {
            block: wresult.loc,
            key: wresult.first_key,
            nr_entries,
            shared: self.shared,
        });
        Ok(())
    }

    /// Writes a full node.
    fn emit_node(&mut self, w: &mut WriteBatcher) -> Result<()> {
        self.emit_values(w, self.max_entries_per_node)
    }

    /// Emits all remaining values.  Panics if there are more than 2 *
    /// max_entries_per_node values.
    fn emit_all(&mut self, w: &mut WriteBatcher) -> Result<()> {
        match self.values.len() {
            0 => {
                // There's nothing to emit
                Ok(())
            }
            n if n <= self.max_entries_per_node => {
                // Emit a single node.
                self.emit_values(w, n)
            }
            n if n <= self.max_entries_per_node * 2 => {
                // Emit two nodes.
                let n1 = n / 2;
                let n2 = n - n1;
                self.emit_values(w, n1)?;
                self.emit_values(w, n2)
            }
            _ => {
                panic!("self.values shouldn't have more than 2 * max_entries_per_node entries");
            }
        }
    }

    fn emit_empty_leaf(&mut self, w: &mut WriteBatcher) -> Result<()> {
        self.emit_values(w, 0)
    }

    /// Pops the last node, and prepends it's values to 'self.values'.  Used
    /// to rebalance when we have insufficient values for a final node.  The
    /// node is decremented in the space map.
    fn unshift_node(&mut self, w: &mut WriteBatcher) -> Result<()> {
        let ls = self.nodes.pop().unwrap();
        let (keys, values) = self.read_node(w, ls.block)?;
        w.sm.lock().unwrap().dec(ls.block)?;

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

pub struct BTreeBuilder<V: Unpack + Pack> {
    leaf_builder: NodeBuilder<V>,
}

impl<V: Unpack + Pack + Clone> BTreeBuilder<V> {
    pub fn new(value_rc: Box<dyn RefCounter<V>>) -> BTreeBuilder<V> {
        BTreeBuilder {
            leaf_builder: NodeBuilder::new(Box::new(LeafIO {}), value_rc, false),
        }
    }

    pub fn push_value(&mut self, w: &mut WriteBatcher, k: u64, v: V) -> Result<()> {
        self.leaf_builder.push_value(w, k, v)
    }

    pub fn push_leaves(&mut self, w: &mut WriteBatcher, leaves: &[NodeSummary]) -> Result<()> {
        self.leaf_builder.push_nodes(w, leaves)
    }

    pub fn complete(self, w: &mut WriteBatcher) -> Result<u64> {
        let nodes = self.leaf_builder.complete(w)?;
        build_btree(w, nodes)
    }
}

//------------------------------------------

// Build a btree from a list of pre-built leaves
pub fn build_btree(w: &mut WriteBatcher, leaves: Vec<NodeSummary>) -> Result<u64> {
    // Now we iterate, adding layers of internal nodes until we end
    // up with a single root.
    let mut nodes = leaves;
    while nodes.len() > 1 {
        let mut builder = NodeBuilder::new(Box::new(InternalIO {}), Box::new(NoopRC {}), false);

        for n in nodes {
            builder.push_value(w, n.key, n.block)?;
        }

        nodes = builder.complete(w)?;
    }

    assert!(nodes.len() == 1);

    let root = nodes[0].block;
    Ok(root)
}

//------------------------------------------

// The pre-built nodes and the contained values were initialized with
// a ref count 1, which is analogous to a "temporary snapshot" of
// potentially shared leaves. We have to drop those temporary references
// to pre-built nodes at the end of device building, and also decrease
// ref counts of the contained values if a pre-built leaf is no longer
// referenced.
pub fn release_leaves<V: Pack + Unpack>(
    w: &mut WriteBatcher,
    leaves: &[NodeSummary],
    value_rc: &mut dyn RefCounter<V>,
) -> Result<()> {
    let nio = LeafIO {};

    for n in leaves {
        let deleted = w.sm.lock().unwrap().dec(n.block)?;
        if deleted {
            let (_, values) = nio.read(w, n.block)?;
            for v in values {
                value_rc.dec(&v)?;
            }
        }
    }

    Ok(())
}

//------------------------------------------
