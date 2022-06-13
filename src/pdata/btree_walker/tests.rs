use super::*;

use mockall::*;
use rangemap::RangeSet;
use std::ops::Range;

use crate::io_engine::core::*;
use crate::pdata::btree_builder::test_utils::*;
use crate::write_batcher::WriteBatcher;

//------------------------------------------

mock! {
    Visitor<V> {}
    impl<V: Unpack> NodeVisitor<V> for Visitor<V> {
        fn visit(
            &self,
            path: &[u64],
            kr: &KeyRange,
            header: &NodeHeader,
            keys: &[u64],
            values: &[V],
        ) -> Result<()>;
        fn visit_again(&self, path: &[u64], b: u64) -> Result<()>;
        fn end_walk(&self) -> Result<()>;
    }
}

//------------------------------------------

struct BTreeWalkerTests<V> {
    w: WriteBatcher,
    layout: Option<BTreeLayout>,
    mappings: Vec<(u64, V)>,
    damaged_nodes: BTreeMap<(u64, usize), u64>, // maps (key_begin, level) to index
    affected_leaves: RangeSet<u64>,
}

impl<V: 'static + Pack + Unpack + Clone + PartialEq + std::fmt::Debug + std::marker::Send>
    BTreeWalkerTests<V>
{
    fn new(engine: Arc<dyn IoEngine + Send + Sync>) -> BTreeWalkerTests<V> {
        let sm = Arc::new(Mutex::new(CoreSpaceMap::<u8>::new(engine.get_nr_blocks())));

        BTreeWalkerTests {
            w: WriteBatcher::new(engine, sm, 16),
            layout: None,
            mappings: Vec::new(),
            damaged_nodes: BTreeMap::new(),
            affected_leaves: RangeSet::new(),
        }
    }

    fn build_btree(&mut self, mappings: Vec<(u64, V)>) {
        self.layout = Some(build_btree_from_mappings(&mut self.w, &mappings[..]));
        self.mappings = mappings;
    }

    fn damage_nodes(&mut self, height: usize, indices: Range<u64>) {
        assert!(self.layout.is_some());
        let layout = self.layout.as_ref().unwrap();
        let nodes = layout.nodes(height);
        let engine = self.w.engine.as_ref();

        for i in indices.start..indices.end {
            let n = &nodes[i as usize];
            trash_block(engine, n.block);
            let level = layout.height() - height;
            self.damaged_nodes
                .insert((n.key_range.start.unwrap_or(0), level), i);
        }

        let leaves_begin = layout.first_leaf(height, indices.start);
        let leaves_end = layout.first_leaf(height, indices.end);
        self.affected_leaves.insert(leaves_begin..leaves_end);
    }

    fn damage_root(&mut self) {
        assert!(self.layout.is_some());
        let height = self.layout.as_ref().unwrap().height();
        self.damage_nodes(height, 0..1);
    }

    fn damage_leaves(&mut self, indices: Range<u64>) {
        self.damage_nodes(0, indices)
    }

    fn build_expected_leaves(&self) -> Vec<NodeInfo> {
        assert!(self.layout.is_some());
        let layout = self.layout.as_ref().unwrap();
        let leaves = layout.leaves();
        let outer_range = Range {
            start: 0u64,
            end: layout.nr_leaves(),
        };
        let mut expected = Vec::new();
        for range in self.affected_leaves.gaps(&outer_range) {
            expected.extend_from_slice(&leaves[range.start as usize..range.end as usize]);
        }

        expected
    }

    fn build_expected_mappings(&self) -> Vec<(u64, V)> {
        assert!(self.layout.is_some());
        let layout = self.layout.as_ref().unwrap();
        let leaves = layout.leaves();
        let outer_range = Range {
            start: 0u64,
            end: layout.nr_leaves(),
        };
        let mut expected = Vec::new();

        for range in self.affected_leaves.gaps(&outer_range) {
            let last_leaf = &leaves[range.end as usize - 1];
            let m_begin = leaves[range.start as usize].entries_begin as usize;
            let m_end = last_leaf.entries_begin as usize + last_leaf.nr_entries;
            expected.extend_from_slice(&self.mappings[m_begin..m_end]);
        }

        expected
    }

    fn run(&self) {
        assert!(self.layout.is_some());
        let expected_leaves = self.build_expected_leaves();
        let nr_good_leaves = expected_leaves.len();
        let mut leaf_iter = expected_leaves.into_iter();
        let mut m_iter = self.build_expected_mappings().into_iter();

        let walker = BTreeWalker::new(self.w.engine.clone(), false);
        let mut visitor = MockVisitor::<V>::new();

        visitor.expect_visit().times(nr_good_leaves).returning(
            move |_path: &[u64], kr: &KeyRange, header: &NodeHeader, keys: &[u64], values: &[V]| {
                // TODO: verify the path

                // verify the header
                let leaf = leaf_iter.next().unwrap();
                assert_eq!(*kr, leaf.key_range);
                assert_eq!(header.nr_entries as usize, leaf.nr_entries);
                assert!(header.is_leaf);
                assert_eq!(header.value_size, V::disk_size());

                // verify mappings
                for (k, v) in keys.iter().zip(values) {
                    let m = m_iter.next().unwrap();
                    assert_eq!(*k, m.0);
                    assert_eq!(*v, m.1);
                }
                Ok(())
            },
        );

        // TODO: verify the number of calls
        visitor.expect_end_walk().return_const(Ok(()));

        let mut path = Vec::new();
        let layout = self.layout.as_ref().unwrap();
        let root = layout.root().block;
        let ret = walker.walk(&mut path, &visitor, root);

        let mut damage_iter = self.damaged_nodes.iter();
        match self.damaged_nodes.len() {
            0 => assert!(ret.is_ok()),
            1 => self.verify_single_error(ret.unwrap_err(), &mut damage_iter),
            _ => self.verify_aggregated_errors(ret.unwrap_err(), &mut damage_iter),
        }
    }

    fn verify_aggregated_errors(
        &self,
        e: BTreeError,
        iter: &mut dyn Iterator<Item = (&(u64, usize), &u64)>,
    ) {
        match e {
            BTreeError::Aggregate(errs) => {
                for err in errs {
                    self.verify_aggregated_errors(err, iter);
                }
            }
            _ => self.verify_single_error(e, iter),
        }
    }

    fn verify_single_error(
        &self,
        e: BTreeError,
        iter: &mut dyn Iterator<Item = (&(u64, usize), &u64)>,
    ) {
        let ((_key_begin, level), index) = iter.next().unwrap();
        let layout = self.layout.as_ref().unwrap();
        let height = layout.height() - level;
        let n = &layout.nodes(height)[*index as usize];

        if let BTreeError::KeyContext(kr, e1) = e {
            assert_eq!(kr, n.key_range);

            if let BTreeError::Path(p, e2) = *e1 {
                assert_eq!(*p.last().unwrap(), n.block);
                assert!(matches!(*e2, BTreeError::NodeError(_)));
                // TODO: Verify the string?
            } else {
                panic!();
            }
        } else {
            panic!();
        }
    }
}

//------------------------------------------

#[test]
fn walk_empty_tree() {
    let engine = Arc::new(CoreIoEngine::new(64));

    type ValueType = u32;
    let mut t = BTreeWalkerTests::<ValueType>::new(engine);

    let mappings = Vec::new();
    t.build_btree(mappings);

    t.run();
}

#[test]
fn walk_tree_with_no_damage() {
    let engine = Arc::new(CoreIoEngine::new(320));

    type ValueType = u32;
    let mut t = BTreeWalkerTests::<ValueType>::new(engine);

    let nr_entries = 100000;
    let mappings = (0..nr_entries as u64)
        .zip(1234u32..1234u32 + nr_entries as u32)
        .collect::<Vec<(u64, ValueType)>>();
    t.build_btree(mappings);

    t.run();
}

#[test]
fn walk_tree_with_a_trashed_root() {
    let engine = Arc::new(CoreIoEngine::new(320));

    type ValueType = u32;
    let mut t = BTreeWalkerTests::<ValueType>::new(engine);

    let nr_entries = 100000;
    let mappings = (0..nr_entries as u64)
        .zip(1234u32..1234u32 + nr_entries as u32)
        .collect::<Vec<(u64, ValueType)>>();
    t.build_btree(mappings);

    t.damage_root();

    t.run();
}

#[test]
fn walk_tree_with_the_first_leaf_damaged() {
    let engine = Arc::new(CoreIoEngine::new(320));

    type ValueType = u32;
    let mut t = BTreeWalkerTests::<ValueType>::new(engine);

    let nr_entries = 100000;
    let mappings = (0..nr_entries as u64)
        .zip(1234u32..1234u32 + nr_entries as u32)
        .collect::<Vec<(u64, ValueType)>>();
    t.build_btree(mappings);

    t.damage_leaves(0..1);

    t.run();
}

#[test]
fn walk_tree_with_the_last_leaf_damaged() {
    let engine = Arc::new(CoreIoEngine::new(320));

    type ValueType = u32;
    let mut t = BTreeWalkerTests::<ValueType>::new(engine);

    let nr_entries = 100000;
    let mappings = (0..nr_entries as u64)
        .zip(1234u32..1234u32 + nr_entries as u32)
        .collect::<Vec<(u64, ValueType)>>();
    t.build_btree(mappings);

    let nr_leaves = t.layout.as_ref().unwrap().nr_leaves();
    t.damage_leaves(nr_leaves - 1..nr_leaves);

    t.run();
}

#[test]
fn walk_tree_with_a_sequence_of_damaged_leaves() {
    let engine = Arc::new(CoreIoEngine::new(320));

    type ValueType = u32;
    let mut t = BTreeWalkerTests::<ValueType>::new(engine);

    let nr_entries = 100000;
    let mappings = (0..nr_entries as u64)
        .zip(1234u32..1234u32 + nr_entries as u32)
        .collect::<Vec<(u64, ValueType)>>();
    t.build_btree(mappings);

    t.damage_leaves(10..15);

    t.run();
}

#[test]
fn walk_tree_with_a_damaged_internal() {
    let engine = Arc::new(CoreIoEngine::new(320));

    type ValueType = u32;
    let mut t = BTreeWalkerTests::<ValueType>::new(engine);

    let nr_entries = 100000;
    let mappings = (0..nr_entries as u64)
        .zip(1234u32..1234u32 + nr_entries as u32)
        .collect::<Vec<(u64, ValueType)>>();
    t.build_btree(mappings);

    t.damage_nodes(1, 1..2);

    t.run();
}

#[test]
fn walk_tree_with_damaged_leaves_and_internals() {
    let engine = Arc::new(CoreIoEngine::new(320));

    type ValueType = u32;
    let mut t = BTreeWalkerTests::<ValueType>::new(engine);

    let nr_entries = 100000;
    let mappings = (0..nr_entries as u64)
        .zip(1234u32..1234u32 + nr_entries as u32)
        .collect::<Vec<(u64, ValueType)>>();
    t.build_btree(mappings);

    t.damage_nodes(1, 1..2);
    t.damage_leaves(10..15);

    t.run();
}

//------------------------------------------
