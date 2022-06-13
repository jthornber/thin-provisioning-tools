use super::*;

use mockall::*;
use rangemap::RangeSet;
use std::collections::BTreeSet;
use std::ops::Range;

use crate::io_engine::core::*;
use crate::pdata::array;
use crate::pdata::array_builder::test_utils::*;
use crate::write_batcher::WriteBatcher;

//------------------------------------------

mock! {
    Visitor<V: Unpack> {}
    impl<V: Unpack> ArrayVisitor<V> for Visitor<V> {
        fn visit(
            &self,
            index: u64,
            b: ArrayBlock<V>,
        ) -> array::Result<()>;
    }
}

//------------------------------------------

struct ArrayWalkerTests<V> {
    w: WriteBatcher,
    layout: Option<ArrayLayout>,
    values: Vec<V>,
    damaged_nodes: BTreeSet<(usize, u64)>, // (height, index)
    affected_ablocks: RangeSet<u64>,
}

impl<V> ArrayWalkerTests<V>
where
    V: 'static
        + Pack
        + Unpack
        + Clone
        + Copy
        + PartialEq
        + Default
        + std::fmt::Debug
        + std::marker::Send,
{
    fn new(engine: Arc<dyn IoEngine + Send + Sync>) -> ArrayWalkerTests<V> {
        let sm = Arc::new(Mutex::new(CoreSpaceMap::<u8>::new(engine.get_nr_blocks())));

        ArrayWalkerTests {
            w: WriteBatcher::new(engine, sm, 16),
            layout: None,
            values: Vec::new(),
            damaged_nodes: BTreeSet::new(),
            affected_ablocks: RangeSet::new(),
        }
    }

    fn build_array(&mut self, values: Vec<V>) {
        self.layout = Some(build_array_from_values(&mut self.w, &values));
        self.values = values;
    }

    fn damage_nodes(&mut self, height: usize, indices: Range<u64>) {
        assert!(self.layout.is_some());
        let layout = self.layout.as_ref().unwrap();
        let nodes = layout.block_tree.nodes(height);
        let engine = self.w.engine.as_ref();

        for i in indices.start..indices.end {
            trash_block(engine, nodes[i as usize].block);
            self.damaged_nodes.insert((height, i)); // FIXME: sort damaged nodes in depth-first ordering
        }

        let ablocks_begin = layout.first_array_block(height, indices.start);
        let ablocks_end = layout.first_array_block(height, indices.end);
        self.affected_ablocks.insert(ablocks_begin..ablocks_end);
    }

    fn damage_root(&mut self) {
        assert!(self.layout.is_some());
        let height = self.layout.as_ref().unwrap().height();
        self.damage_nodes(height, 0..1);
    }

    fn damage_leaves(&mut self, indices: Range<u64>) {
        self.damage_nodes(0, indices)
    }

    fn damage_ablocks(&mut self, indices: Range<u64>) {
        assert!(self.layout.is_some());
        let layout = self.layout.as_ref().unwrap();
        let ablocks = layout.array_blocks();
        let engine = self.w.engine.as_ref();

        for i in indices.start..indices.end {
            trash_block(engine, ablocks[i as usize]);
        }

        self.affected_ablocks.insert(indices);
    }

    fn build_expected_ablocks(&self) -> Vec<u64> {
        assert!(self.layout.is_some());
        let layout = self.layout.as_ref().unwrap();
        let ablocks = layout.array_blocks(); // block numbers
        let outer_range = Range {
            start: 0u64,
            end: layout.nr_array_blocks(),
        };
        let mut expected = Vec::new();

        for range in self.affected_ablocks.gaps(&outer_range) {
            expected.extend_from_slice(&ablocks[range.start as usize..range.end as usize]);
        }

        expected
    }

    fn build_expected_values(&self) -> Vec<V> {
        assert!(self.layout.is_some());
        let layout = self.layout.as_ref().unwrap();
        let outer_range = Range {
            start: 0u64,
            end: layout.nr_array_blocks(),
        };
        let mut expected = Vec::new();
        let max_entries = array::calc_max_entries::<V>();

        for range in self.affected_ablocks.gaps(&outer_range) {
            let v_begin = range.start as usize * max_entries;
            let v_end = std::cmp::min(range.end as usize * max_entries, self.values.len());
            expected.extend_from_slice(&self.values[v_begin..v_end]);
        }

        expected
    }

    fn run(&self) {
        assert!(self.layout.is_some());

        let expected_ablocks = self.build_expected_ablocks();
        let nr_good_ablocks = expected_ablocks.len();
        let mut ablock_iter = expected_ablocks.into_iter();
        let mut v_iter = self.build_expected_values().into_iter();

        let layout = self.layout.as_ref().unwrap();
        let nr_ablocks = layout.nr_array_blocks();
        let max_entries = array::calc_max_entries::<V>() as u32;
        let last_nr_entries: u32 = if self.values.len() as u64 % nr_ablocks == 0 {
            max_entries
        } else {
            (self.values.len() % max_entries as usize) as u32
        };

        let walker = ArrayWalker::new(self.w.engine.clone(), false);
        let mut visitor = MockVisitor::<V>::new();

        visitor.expect_visit().times(nr_good_ablocks).returning(
            move |index: u64, b: ArrayBlock<V>| {
                // verify the header
                let blocknr = ablock_iter.next().unwrap();
                assert_eq!(b.header.blocknr, blocknr);
                assert_eq!(b.header.max_entries, max_entries);
                assert_eq!(b.header.value_size, V::disk_size());

                if index == nr_ablocks - 1 {
                    assert_eq!(b.header.nr_entries, last_nr_entries);
                } else {
                    assert_eq!(b.header.nr_entries, max_entries);
                }

                // verify values
                for val in b.values {
                    let v = v_iter.next().unwrap();
                    assert_eq!(val, v);
                }

                Ok(())
            },
        );

        // TODO: verify errors
        let _ = walker.walk(&visitor, layout.root().block);
    }
}

//------------------------------------------

const ARRAY_SIZE: usize = 300000;

#[test]
fn walk_array_with_no_damage() {
    let engine = Arc::new(CoreIoEngine::new(768));

    type ValueType = u64;
    let mut t = ArrayWalkerTests::<ValueType>::new(engine);

    let values: Vec<ValueType> = (0..ARRAY_SIZE as u64).collect();
    t.build_array(values);

    t.run();
}

#[test]
fn walk_array_with_a_trashed_root() {
    let engine = Arc::new(CoreIoEngine::new(768));

    type ValueType = u64;
    let mut t = ArrayWalkerTests::<ValueType>::new(engine);

    let values: Vec<ValueType> = (0..ARRAY_SIZE as u64).collect();
    t.build_array(values);

    t.damage_root();

    t.run();
}

#[test]
fn walk_array_with_the_first_leaf_damaged() {
    let engine = Arc::new(CoreIoEngine::new(768));

    type ValueType = u64;
    let mut t = ArrayWalkerTests::<ValueType>::new(engine);

    let values: Vec<ValueType> = (0..ARRAY_SIZE as u64).collect();
    t.build_array(values);

    t.damage_leaves(0..1);

    t.run();
}

#[test]
fn walk_array_with_the_last_leaf_damaged() {
    let engine = Arc::new(CoreIoEngine::new(768));

    type ValueType = u64;
    let mut t = ArrayWalkerTests::<ValueType>::new(engine);

    let values: Vec<ValueType> = (0..ARRAY_SIZE as u64).collect();
    t.build_array(values);

    let nr_leaves = t.layout.as_ref().unwrap().nr_leaves();
    t.damage_leaves(nr_leaves - 1..nr_leaves);

    t.run();
}

#[test]
fn walk_array_with_the_first_ablock_damaged() {
    let engine = Arc::new(CoreIoEngine::new(768));

    type ValueType = u64;
    let mut t = ArrayWalkerTests::<ValueType>::new(engine);

    let values: Vec<ValueType> = (0..ARRAY_SIZE as u64).collect();
    t.build_array(values);

    t.damage_ablocks(0..1);

    t.run();
}

#[test]
fn walk_array_with_the_last_ablock_damaged() {
    let engine = Arc::new(CoreIoEngine::new(768));

    type ValueType = u64;
    let mut t = ArrayWalkerTests::<ValueType>::new(engine);

    let values: Vec<ValueType> = (0..ARRAY_SIZE as u64).collect();
    t.build_array(values);

    let nr_ablocks = t.layout.as_ref().unwrap().nr_array_blocks();
    t.damage_ablocks(nr_ablocks - 1..nr_ablocks);

    t.run();
}

#[test]
fn walk_array_with_a_damaged_ablock_sequence() {
    let engine = Arc::new(CoreIoEngine::new(768));

    type ValueType = u64;
    let mut t = ArrayWalkerTests::<ValueType>::new(engine);

    let values: Vec<ValueType> = (0..ARRAY_SIZE as u64).collect();
    t.build_array(values);

    t.damage_ablocks(250..260);

    t.run();
}

#[test]
fn walk_array_with_damaged_nodes_and_ablocks() {
    let engine = Arc::new(CoreIoEngine::new(768));

    type ValueType = u64;
    let mut t = ArrayWalkerTests::<ValueType>::new(engine);

    let values: Vec<ValueType> = (0..ARRAY_SIZE as u64).collect();
    t.build_array(values);

    t.damage_leaves(0..1);
    t.damage_ablocks(260..270);

    t.run();
}

//------------------------------------------
