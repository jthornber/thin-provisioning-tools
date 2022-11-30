use fixedbitset::FixedBitSet;
use std::sync::Arc;

use crate::checksum;
use crate::io_engine::*;
use crate::pdata::btree::*;
use crate::pdata::space_map::*;
use crate::pdata::unpack::*;

//------------------------------------------

pub trait LeafVisitor<V: Unpack> {
    fn visit(&mut self, kr: &KeyRange, b: u64) -> Result<()>;

    // Nodes may be shared and thus visited multiple times.  The walker avoids
    // doing repeated IO, but it does call this method to keep the visitor up to
    // date.  b may be an internal node obviously.
    // FIXME: remove this method?
    fn visit_again(&mut self, b: u64) -> Result<()>;
    fn end_walk(&mut self) -> Result<()>;
}

// This is useful if you just want to get the space map counts from the walk.
pub struct NoopLeafVisitor {}

impl<V: Unpack> LeafVisitor<V> for NoopLeafVisitor {
    fn visit(&mut self, _kr: &KeyRange, _b: u64) -> Result<()> {
        Ok(())
    }

    fn visit_again(&mut self, _b: u64) -> Result<()> {
        Ok(())
    }

    fn end_walk(&mut self) -> Result<()> {
        Ok(())
    }
}

pub struct LeafWalker<'a> {
    engine: Arc<dyn IoEngine + Send + Sync>,
    sm: &'a mut dyn SpaceMap,
    leaves: FixedBitSet,
    ignore_non_fatal: bool,
}

impl<'a> LeafWalker<'a> {
    pub fn new(
        engine: Arc<dyn IoEngine + Send + Sync>,
        sm: &'a mut dyn SpaceMap,
        ignore_non_fatal: bool,
    ) -> LeafWalker<'a> {
        let nr_blocks = engine.get_nr_blocks() as usize;
        LeafWalker {
            engine,
            sm,
            leaves: FixedBitSet::with_capacity(nr_blocks),
            ignore_non_fatal,
        }
    }

    // Atomically increments the ref count, and returns the _old_ count.
    fn sm_inc(&mut self, b: u64) -> u32 {
        let sm = &mut self.sm;
        let count = sm.get(b).unwrap();
        sm.inc(b, 1).unwrap();
        count
    }

    fn walk_nodes<LV, V>(
        &mut self,
        depth: usize,
        path: &mut Vec<u64>,
        visitor: &mut LV,
        krs: &[KeyRange],
        bs: &[u64],
    ) -> Result<()>
    where
        LV: LeafVisitor<V>,
        V: Unpack,
    {
        assert_eq!(krs.len(), bs.len());

        let mut blocks = Vec::with_capacity(bs.len());
        let mut filtered_krs = Vec::with_capacity(krs.len());
        for i in 0..bs.len() {
            self.sm_inc(bs[i]);
            blocks.push(bs[i]);
            filtered_krs.push(krs[i].clone());
        }

        let rblocks = self
            .engine
            .read_many(&blocks[0..])
            .map_err(|_e| io_err(path))?;

        for (i, rb) in rblocks.into_iter().enumerate() {
            match rb {
                Err(_) => {
                    return Err(io_err(path).keys_context(&filtered_krs[i]));
                }
                Ok(b) => {
                    self.walk_node(depth - 1, path, visitor, &filtered_krs[i], &b, false)?;
                }
            }
        }

        Ok(())
    }

    fn walk_node_<LV, V>(
        &mut self,
        depth: usize,
        path: &mut Vec<u64>,
        visitor: &mut LV,
        kr: &KeyRange,
        b: &Block,
        is_root: bool,
    ) -> Result<()>
    where
        LV: LeafVisitor<V>,
        V: Unpack,
    {
        use Node::*;

        let bt = checksum::metadata_block_type(b.get_data());
        if bt != checksum::BT::NODE {
            return Err(node_err(path, NodeError::ChecksumError).keys_context(kr));
        }

        let node = unpack_node::<V>(path, b.get_data(), self.ignore_non_fatal, is_root)?;

        if let Internal { keys, values, .. } = node {
            let krs = split_key_ranges(path, kr, &keys)?;
            if depth == 0 {
                // it is the lowest internal
                for i in 0..krs.len() {
                    self.sm.inc(values[i], 1).expect("sm.inc() failed");
                    for v in &values {
                        self.leaves.insert(*v as usize);
                    }
                    visitor.visit(&krs[i], values[i])?;
                }
                Ok(())
            } else {
                self.walk_nodes(depth, path, visitor, &krs, &values)
            }
        } else {
            Err(context_err(
                path,
                "btree nodes are not all at the same depth.",
            ))
        }
    }

    fn walk_node<LV, V>(
        &mut self,
        depth: usize,
        path: &mut Vec<u64>,
        visitor: &mut LV,
        kr: &KeyRange,
        b: &Block,
        is_root: bool,
    ) -> Result<()>
    where
        LV: LeafVisitor<V>,
        V: Unpack,
    {
        path.push(b.loc);
        let r = self.walk_node_(depth, path, visitor, kr, b, is_root);
        path.pop();
        visitor.end_walk()?;
        r
    }

    fn get_depth<V: Unpack>(&self, path: &mut Vec<u64>, root: u64, is_root: bool) -> Result<usize> {
        use Node::*;

        let b = self.engine.read(root).map_err(|_| io_err(path))?;

        let bt = checksum::metadata_block_type(b.get_data());
        if bt != checksum::BT::NODE {
            return Err(node_err(path, NodeError::ChecksumError));
        }

        let node = unpack_node::<V>(path, b.get_data(), self.ignore_non_fatal, is_root)?;

        match node {
            Internal { values, .. } => {
                let n = self.get_depth::<V>(path, values[0], false)?;
                Ok(n + 1)
            }
            Leaf { .. } => Ok(0),
        }
    }

    pub fn walk<LV, V>(&mut self, path: &mut Vec<u64>, visitor: &mut LV, root: u64) -> Result<()>
    where
        LV: LeafVisitor<V>,
        V: Unpack,
    {
        let kr = KeyRange {
            start: None,
            end: None,
        };

        let depth = self.get_depth::<V>(path, root, true)?;

        self.sm_inc(root);
        if depth == 0 {
            // root is a leaf
            self.leaves.insert(root as usize);
            visitor.visit(&kr, root)?;
            Ok(())
        } else {
            let root = self.engine.read(root).map_err(|_| io_err(path))?;

            self.walk_node(depth - 1, path, visitor, &kr, &root, true)
        }
    }

    // Call this to extract the leaves bitset after you've done your walking.
    pub fn get_leaves(self) -> FixedBitSet {
        self.leaves
    }
}

//------------------------------------------
