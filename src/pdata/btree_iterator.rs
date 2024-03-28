use anyhow::{anyhow, Result};
use std::sync::Arc;

use crate::io_engine::*;
use crate::pdata::btree::*;
use crate::pdata::unpack::*;

//------------------------------------------

fn is_leaf<V: Unpack>(node: &Node<V>) -> bool {
    match node {
        Node::Internal { .. } => false,
        Node::Leaf { .. } => true,
    }
}

fn get_nr_entries<V: Unpack>(node: &Node<V>) -> usize {
    match node {
        Node::Internal { header, .. } => header.nr_entries as usize,
        Node::Leaf { header, .. } => header.nr_entries as usize,
    }
}

// Assumes an internal node.
fn get_child_loc<V: Unpack>(node: &Node<V>, index: usize) -> u64 {
    match node {
        Node::Internal { values, .. } => values[index],
        Node::Leaf { .. } => panic!("unexpected leaf"),
    }
}

//------------------------------------------

/// A `Frame` represents a single node within the BTree traversal stack.
///
/// It keeps track of the current node being processed and the index of the next child or entry
/// to visit within that node. This struct is used internally by the `BTreeIterator` to manage
/// the state of its depth-first traversal of the BTree.
struct Frame<V: Unpack> {
    index: usize,
    node: Node<V>,
}

/// An iterator over the entries of a BTree.
///
/// `BTreeIterator` provides a way to traverse all the entries in a BTree in sorted order.
//
/// # Limitations
///
/// - The iterator does not support concurrent modifications to the BTree while iterating.
pub struct BTreeIterator<V: Unpack + Clone> {
    engine: Arc<dyn IoEngine + Send + Sync>,
    path: Vec<u64>,
    stack: Vec<Frame<V>>,
}

impl<V: Unpack + Clone> BTreeIterator<V> {
    fn push_frame(&mut self, block: Block, node: Node<V>) {
        self.path.push(block.loc);
        self.stack.push(Frame { index: 0, node });
    }

    fn pop_frame(&mut self) {
        self.path.pop();
        self.stack.pop();
    }

    // Returns true if just pushed a leaf node.
    fn push_loc(&mut self, loc: u64) -> Result<bool> {
        let is_root = self.stack.is_empty();
        let block = self.engine.read(loc)?;
        let node = unpack_node::<V>(&self.path, block.get_data(), true, is_root)?;
        let done = is_leaf(&node);
        self.push_frame(block, node);
        Ok(done)
    }

    fn left_most_leaf(&mut self, root: u64) -> Result<()> {
        let mut b = root;

        loop {
            if self.push_loc(b)? {
                break;
            }

            b = get_child_loc(&self.stack.last().unwrap().node, 0);
        }
        Ok(())
    }

    pub fn new(engine: Arc<dyn IoEngine + Send + Sync>, root: u64) -> Result<Self> {
        let path = Vec::new();
        let stack = Vec::new();
        let mut me = Self {
            engine,
            path,
            stack,
        };
        me.left_most_leaf(root)?;
        Ok(me)
    }

    /// Access the entry the iterator is curently pointing to.
    pub fn get(&self) -> Option<(u64, &V)> {
        if let Some(frame) = self.stack.last() {
            match &frame.node {
                Node::Internal { .. } => {
                    panic!("not a leaf");
                }
                Node::Leaf { keys, values, .. } => {
                    if keys.is_empty() {
                        None
                    } else {
                        Some((keys[frame.index], &values[frame.index]))
                    }
                }
            }
        } else {
            // Empty stack indicates end of iteration
            None
        }
    }

    // returns true if we need to move onto the next node.
    fn inc_frame(&mut self) -> bool {
        if let Some(frame) = self.stack.last_mut() {
            frame.index += 1;
            frame.index >= get_nr_entries(&frame.node)
        } else {
            // End of iteration
            false
        }
    }

    fn next_node(&mut self) -> Result<()> {
        self.pop_frame();

        if self.inc_frame() {
            self.next_node()?;
        }

        if let Some(frame) = self.stack.last() {
            let loc = get_child_loc(&frame.node, frame.index);
            self.push_loc(loc)?;
        }

        Ok(())
    }

    /// Move to the next entry.
    pub fn step(&mut self) -> Result<()> {
        if self.inc_frame() {
            self.next_node()?;
        }
        Ok(())
    }
}

//------------------------------------------

#[cfg(test)]
mod test {
    use super::*;
    use crate::io_engine::core::*;
    use crate::pdata::btree_builder::test_utils::*;
    use crate::pdata::space_map::*;
    use crate::write_batcher::*;

    use anyhow::ensure;
    use std::sync::Mutex;

    struct Fixture {
        engine: Arc<dyn IoEngine + Send + Sync>,
        tree: u64,
    }

    impl Fixture {
        fn build_values(nr_entries: usize) -> Vec<(u64, u64)> {
            let mut result = Vec::new();
            for i in 0..nr_entries as u64 {
                result.push((i, i * 3));
            }

            result
        }

        fn new(nr_entries: usize) -> Result<Self> {
            let nr_metadata_blocks = 1024;
            let engine = Arc::new(CoreIoEngine::new(nr_metadata_blocks));
            let sm = Arc::new(Mutex::new(CoreSpaceMap::<u8>::new(nr_metadata_blocks)));
            let mut batcher = WriteBatcher::new(engine.clone(), sm.clone(), 16);
            let values = Self::build_values(nr_entries);
            let tree = build_btree_from_mappings(&mut batcher, &values);

            Ok(Self {
                engine: engine.clone(),
                tree: tree.root().block,
            })
        }
    }

    #[test]
    fn empty_tree() -> Result<()> {
        let fix = Fixture::new(0)?;
        let iter = BTreeIterator::<u64>::new(fix.engine.clone(), fix.tree)?;

        if iter.get().is_some() {
            // There should be no entries
            ensure!(false);
        }

        Ok(())
    }

    fn do_test(nr_entries: usize) -> Result<()> {
        let fix = Fixture::new(nr_entries)?;
        let mut iter = BTreeIterator::<u64>::new(fix.engine.clone(), fix.tree)?;

        let mut result: Vec<(u64, u64)> = Vec::new();
        while let Some(v) = iter.get() {
            result.push((v.0, *v.1));
            iter.step()?;
        }

        let expected = Fixture::build_values(nr_entries);
        ensure!(expected == result);

        Ok(())
    }

    #[test]
    fn small_tree() -> Result<()> {
        do_test(16)
    }

    #[test]
    fn large_tree() -> Result<()> {
        do_test(10240)
    }
}
