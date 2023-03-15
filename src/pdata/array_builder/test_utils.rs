use super::*;

use crate::pdata::btree_builder::test_utils::*;

//------------------------------------------

pub struct ArrayLayout {
    ablocks: Vec<u64>,
    pub block_tree: BTreeLayout,
}

impl ArrayLayout {
    pub fn height(&self) -> usize {
        self.block_tree.height()
    }

    pub fn nr_nodes(&self) -> u64 {
        self.block_tree.nr_nodes()
    }

    pub fn nr_leaves(&self) -> u64 {
        self.block_tree.nr_leaves()
    }

    pub fn root(&self) -> NodeInfo {
        self.block_tree.root()
    }

    pub fn nodes(&self, height: usize) -> &[NodeInfo] {
        self.block_tree.nodes(height)
    }

    pub fn leaves(&self) -> &[NodeInfo] {
        self.block_tree.leaves()
    }

    pub fn array_blocks(&self) -> &[u64] {
        &self.ablocks
    }

    pub fn nr_array_blocks(&self) -> u64 {
        self.ablocks.len() as u64
    }

    // Returns the index of the first array block under a specific node
    pub fn first_array_block(&self, height: usize, index: u64) -> u64 {
        let leaf_idx = self.block_tree.first_leaf(height, index);
        let nr_leaves = self.block_tree.nr_leaves();
        assert!(leaf_idx <= nr_leaves);

        if leaf_idx == nr_leaves {
            self.nr_array_blocks()
        } else {
            self.block_tree.leaves()[leaf_idx as usize].entries_begin
        }
    }
}

//------------------------------------------

pub fn build_array_blocks<V: Pack + Unpack + Clone + Default>(
    w: &mut WriteBatcher,
    values: &[V],
) -> Vec<u64> {
    let mut builder = ArrayBlockBuilder::<V>::new(values.len() as u64);
    for (i, v) in values.iter().enumerate() {
        assert!(builder.push_value(w, i as u64, v.clone()).is_ok());
    }
    builder.complete(w).unwrap()
}

pub fn build_array_from_values<V: Pack + Unpack + Clone + Default>(
    w: &mut WriteBatcher,
    values: &[V],
) -> ArrayLayout {
    assert!(!values.is_empty());
    let ablocks = build_array_blocks(w, values);

    // manually build the block tree so that we could obtain the layout
    let mappings: Vec<(u64, u64)> = ablocks
        .iter()
        .enumerate()
        .map(|(i, v)| (i as u64, *v))
        .collect();
    let block_tree = build_btree_from_mappings(w, &mappings);

    ArrayLayout {
        ablocks,
        block_tree,
    }
}

//------------------------------------------
