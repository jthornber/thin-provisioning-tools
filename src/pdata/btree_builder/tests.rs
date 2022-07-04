use super::test_utils::*;
use super::*;

use crate::io_engine::core::CoreIoEngine;
use crate::io_engine::IoEngine;

//------------------------------------------

struct EntriesCounter {
    remains: Option<u64>,
    max_entries: usize,
}

impl EntriesCounter {
    fn new<V: Unpack>(nr_entries: u64) -> EntriesCounter {
        EntriesCounter {
            remains: Some(nr_entries),
            max_entries: calc_max_entries::<V>(),
        }
    }
}

impl Iterator for EntriesCounter {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(n) = self.remains {
            if n >= 2 * self.max_entries as u64 {
                self.remains = Some(n - self.max_entries as u64);
                Some(self.max_entries)
            } else if n > self.max_entries as u64 {
                self.remains = Some(n - n / 2);
                Some(n as usize / 2)
            } else {
                self.remains = None;
                Some(n as usize)
            }
        } else {
            None
        }
    }
}

#[test]
fn test_entries_counter() {
    type ValueType = u32;
    let max_entries = calc_max_entries::<ValueType>();

    let mut counter1 = EntriesCounter::new::<ValueType>(max_entries as u64);
    assert_eq!(counter1.next(), Some(max_entries));
    assert_eq!(counter1.next(), None);

    let mut counter2 = EntriesCounter::new::<ValueType>(0);
    assert_eq!(counter2.next(), Some(0));
    assert_eq!(counter2.next(), None);

    let mut counter3 = EntriesCounter::new::<ValueType>(max_entries as u64 * 2 + 1);
    assert_eq!(counter3.next(), Some(max_entries));
    assert_eq!(counter3.next(), Some((max_entries + 1) / 2));
    assert_eq!(
        counter3.next(),
        Some((max_entries + 1) - (max_entries + 1) / 2)
    );
    assert_eq!(counter3.next(), None);
}

//------------------------------------------

struct LeafBuilderTests<V: Pack + Unpack + Clone> {
    w: WriteBatcher,
    builder: Option<NodeBuilder<V>>,
    leaves: Vec<NodeSummary>,
}

impl<V: Pack + Unpack + Clone + PartialEq + std::fmt::Debug> LeafBuilderTests<V> {
    fn new(engine: Arc<dyn IoEngine + Send + Sync>) -> LeafBuilderTests<V> {
        let sm = Arc::new(Mutex::new(CoreSpaceMap::<u8>::new(engine.get_nr_blocks())));
        let builder = NodeBuilder::<V>::new(Box::new(LeafIO {}), Box::new(NoopRC {}), false);

        LeafBuilderTests {
            w: WriteBatcher::new(engine, sm, 16),
            builder: Some(builder),
            leaves: Vec::new(),
        }
    }

    fn build_def(&mut self, mappings: &[(u64, V)]) -> Vec<NodeSummary> {
        build_leaves(&mut self.w, mappings, true)
    }

    fn push_values(&mut self, mappings: &[(u64, V)]) {
        assert!(self.builder.is_some());
        let builder = self.builder.as_mut().unwrap();
        push_values(builder, &mut self.w, mappings);
    }

    fn add_ref(&mut self, nodes: &[NodeSummary]) {
        assert!(self.builder.is_some());
        let builder = self.builder.as_mut().unwrap();
        assert!(builder.push_nodes(&mut self.w, nodes).is_ok());
    }

    fn complete(&mut self) {
        assert!(self.builder.is_some());
        let builder = self.builder.take().unwrap();
        self.leaves = builder.complete(&mut self.w).unwrap();
        assert!(self.w.flush().is_ok());
    }

    fn verify_residency<T: IntoIterator<Item = usize>>(&self, counter: T) {
        assert!(!self.leaves.is_empty());

        let mut it = counter.into_iter();
        for leaf in self.leaves.iter() {
            assert_eq!(leaf.nr_entries, it.next().unwrap());
        }
        assert!(it.next().is_none());
    }

    fn verify_mappings(&self, mappings: &[(u64, V)]) {
        assert!(!self.leaves.is_empty());

        let max_entries = calc_max_entries::<V>();
        let is_root = self.leaves.len() == 1;

        let mut m_iter = mappings.iter();
        for leaf in self.leaves.iter() {
            let b = self.w.engine.read(leaf.block).unwrap();
            let n = unpack_node::<V>(&[], b.get_data(), false, is_root).unwrap();
            match n {
                Node::<V>::Leaf {
                    header,
                    keys,
                    values,
                } => {
                    assert!(header.is_leaf);
                    assert_eq!(header.nr_entries as usize, leaf.nr_entries);
                    assert_eq!(header.max_entries as usize, max_entries);
                    assert_eq!(header.value_size, V::disk_size());

                    if header.nr_entries > 0 {
                        assert_eq!(leaf.key, keys[0]);
                    }

                    for (k, v) in keys.into_iter().zip(values) {
                        let m = m_iter.next().unwrap();
                        assert_eq!(k, m.0);
                        assert_eq!(v, m.1);
                    }
                }
                _ => panic!("invalid node"),
            }
        }
    }

    fn verify_block_number(&self, index: usize, loc: u64) {
        assert!(!self.leaves.is_empty());
        assert_eq!(self.leaves[index].block, loc);
    }
}

//------------------------------------------

#[test]
fn build_empty_leaf() {
    let engine = Arc::new(CoreIoEngine::new(64));

    type ValueType = u32;
    let mut t = LeafBuilderTests::<ValueType>::new(engine);

    let mappings = Vec::<(u64, ValueType)>::new();
    t.push_values(&mappings);
    t.complete();

    t.verify_residency([0]);
    t.verify_mappings(&mappings);
}

#[test]
fn build_underfull_root() {
    let engine = Arc::new(CoreIoEngine::new(64));

    type ValueType = u32;
    let mut t = LeafBuilderTests::<ValueType>::new(engine);

    let mappings = (0..10)
        .zip(1234u32..1244u32)
        .collect::<Vec<(u64, ValueType)>>();

    t.push_values(&mappings);
    t.complete();

    t.verify_residency([10]);
    t.verify_mappings(&mappings);
}

#[test]
fn build_full_root() {
    let engine = Arc::new(CoreIoEngine::new(64));

    type ValueType = u32;
    let mut t = LeafBuilderTests::<ValueType>::new(engine);

    let nr_entries = calc_max_entries::<ValueType>();
    let mappings = (0..nr_entries as u64)
        .zip(1234u32..1234u32 + nr_entries as u32)
        .collect::<Vec<(u64, ValueType)>>();
    t.push_values(&mappings);
    t.complete();

    t.verify_residency([nr_entries]);
    t.verify_mappings(&mappings);
}

#[test]
fn build_two_full_leaves() {
    let engine = Arc::new(CoreIoEngine::new(64));

    type ValueType = u32;
    let mut t = LeafBuilderTests::<ValueType>::new(engine);

    let nr_entries = calc_max_entries::<ValueType>() * 2;
    let mappings = (0..nr_entries as u64)
        .zip(1234u32..1234u32 + nr_entries as u32)
        .collect::<Vec<(u64, ValueType)>>();

    t.push_values(&mappings);
    t.complete();

    t.verify_residency(EntriesCounter::new::<ValueType>(nr_entries as u64));
    t.verify_mappings(&mappings);
}

#[test]
fn build_two_balanced_leaves() {
    let engine = Arc::new(CoreIoEngine::new(64));

    type ValueType = u32;
    let mut t = LeafBuilderTests::<ValueType>::new(engine);

    let nr_entries = calc_max_entries::<ValueType>() + 1;
    let mappings = (0..nr_entries as u64)
        .zip(1234u32..1234u32 + nr_entries as u32)
        .collect::<Vec<(u64, ValueType)>>();
    t.push_values(&mappings);
    t.complete();

    t.verify_residency(EntriesCounter::new::<ValueType>(nr_entries as u64));
    t.verify_mappings(&mappings);
}

#[test]
fn build_three_leaves() {
    let engine = Arc::new(CoreIoEngine::new(64));

    type ValueType = u32;
    let mut t = LeafBuilderTests::<ValueType>::new(engine);

    let nr_entries = calc_max_entries::<ValueType>() * 2 + 1;
    let mappings = (0..nr_entries as u64)
        .zip(1234u32..1234u32 + nr_entries as u32)
        .collect::<Vec<(u64, ValueType)>>();
    t.push_values(&mappings);
    t.complete();

    t.verify_residency(EntriesCounter::new::<ValueType>(nr_entries as u64));
    t.verify_mappings(&mappings);
}

// Push some regular (half-full) nodes while there are sufficient
// numbers of entries unflushed
#[test]
fn push_regular_nodes() {
    let engine = Arc::new(CoreIoEngine::new(64));

    type ValueType = u32;
    let mut t = LeafBuilderTests::<ValueType>::new(engine);

    let nr_entries = calc_max_entries::<ValueType>();
    let mappings = (0..nr_entries as u64)
        .zip(1234u32..1234u32 + nr_entries as u32)
        .collect::<Vec<(u64, ValueType)>>();

    let unflushed = nr_entries / 2;
    let def = t.build_def(&mappings[unflushed..]);

    t.push_values(&mappings[..unflushed]);
    t.add_ref(&def);
    t.complete();

    t.verify_residency([unflushed, nr_entries - unflushed]);
    t.verify_mappings(&mappings);
    t.verify_block_number(1, def[0].block);
}

// Push some regular nodes while there are insufficient numbers of entries unflushed.
// Only the first node is expected to be unpacked.
#[test]
fn unpack_the_first_regular_node() {
    let engine = Arc::new(CoreIoEngine::new(64));

    type ValueType = u32;
    let mut t = LeafBuilderTests::<ValueType>::new(engine);

    let max_entries = calc_max_entries::<ValueType>();
    let nr_entries = max_entries * 2;
    let mappings = (0..nr_entries as u64)
        .zip(1234u32..1234u32 + nr_entries as u32)
        .collect::<Vec<(u64, ValueType)>>();

    let unflushed = 10;
    let def = t.build_def(&mappings[unflushed..]);

    t.push_values(&mappings[..unflushed]);
    t.add_ref(&def);
    t.complete();

    let first = (unflushed + def[0].nr_entries) / 2;
    let second = (unflushed + def[0].nr_entries) - first;
    t.verify_residency([first, second, def[1].nr_entries]);
    t.verify_mappings(&mappings);
    t.verify_block_number(2, def[1].block);
}

// Push some underfull nodes while there are sufficient numbers of entries unflushed
#[test]
fn unpacking_underfull_nodes_requires_unshift() {
    let engine = Arc::new(CoreIoEngine::new(64));

    type ValueType = u32;
    let mut t = LeafBuilderTests::<ValueType>::new(engine);

    let nr_entries = calc_max_entries::<ValueType>() / 2 + 2;
    let mappings = (0..nr_entries as u64)
        .zip(1234u32..1234u32 + nr_entries as u32)
        .collect::<Vec<(u64, ValueType)>>();

    let unflushed = nr_entries - 2;
    let def1 = t.build_def(&mappings[unflushed..unflushed + 1]);
    let def2 = t.build_def(&mappings[unflushed + 1..unflushed + 2]);

    t.push_values(&mappings[..unflushed]);
    t.add_ref(&def1);
    t.add_ref(&def2);
    t.complete();

    t.verify_residency([nr_entries]);
    t.verify_mappings(&mappings);
}

// Push some underfull nodes while there are insufficient numbers of entries unflushed.
#[test]
fn unpack_underfull_nodes_without_unshift() {
    let engine = Arc::new(CoreIoEngine::new(64));

    type ValueType = u32;
    let mut t = LeafBuilderTests::<ValueType>::new(engine);

    let nr_entries = 12;
    let mappings = (0..nr_entries as u64)
        .zip(1234u32..1234u32 + nr_entries as u32)
        .collect::<Vec<(u64, ValueType)>>();

    let unflushed = 10;
    let def1 = t.build_def(&mappings[unflushed..unflushed + 1]);
    let def2 = t.build_def(&mappings[unflushed + 1..unflushed + 2]);

    t.push_values(&mappings[..unflushed]);
    t.add_ref(&def1);
    t.add_ref(&def2);
    t.complete();

    t.verify_residency([nr_entries]);
    t.verify_mappings(&mappings);
}

// A tree consists of a shared root
#[test]
fn push_an_underfull_root() {
    let engine = Arc::new(CoreIoEngine::new(64));

    type ValueType = u32;
    let mut t = LeafBuilderTests::<ValueType>::new(engine);

    let mappings = (0..10)
        .zip(1234u32..1244u32)
        .collect::<Vec<(u64, ValueType)>>();

    let def = t.build_def(&mappings);
    t.add_ref(&def);
    t.complete();

    t.verify_residency([10]);
    t.verify_mappings(&mappings);
    t.verify_block_number(0, def[0].block);
}

// Leaves built by a LeafBuilder must be half-full, except the roots.
// Therefore, pushing multiple underfull leaves at once is not allowed.
#[test]
#[should_panic]
fn pushing_multiple_underfull_nodes_at_once_should_fail() {
    let engine = Arc::new(CoreIoEngine::new(64));

    type ValueType = u32;
    let mut t = LeafBuilderTests::<ValueType>::new(engine);

    let mappings = (0..12)
        .zip(1234u32..1246u32)
        .collect::<Vec<(u64, ValueType)>>();

    // generate a leaf sequence containing multiple underfull nodes
    let mut def = t.build_def(&mappings[..6]);
    def.extend(t.build_def(&mappings[6..]));

    t.add_ref(&def);
}

// Pushing underfull roots one-by-one is allowed.
#[test]
fn push_underfull_nodes_one_by_one() {
    let engine = Arc::new(CoreIoEngine::new(64));

    type ValueType = u32;
    let mut t = LeafBuilderTests::<ValueType>::new(engine);

    let mappings = (0..12)
        .zip(1234u32..1246u32)
        .collect::<Vec<(u64, ValueType)>>();

    let def1 = t.build_def(&mappings[..6]);
    let def2 = t.build_def(&mappings[6..]);

    t.add_ref(&def1);
    t.add_ref(&def2);
    t.complete();

    t.verify_residency([12]);
    t.verify_mappings(&mappings);
}

// A half-full node followed by an extra mapping
#[test]
fn unshift_a_regular_node() {
    let engine = Arc::new(CoreIoEngine::new(64));

    type ValueType = u32;
    let mut t = LeafBuilderTests::<ValueType>::new(engine);

    let def_len = calc_max_entries::<ValueType>() / 2;
    let nr_entries = def_len + 1;
    let mappings = (0..nr_entries as u64)
        .zip(1234u32..1234u32 + nr_entries as u32)
        .collect::<Vec<(u64, ValueType)>>();

    let def = t.build_def(&mappings[..def_len]);

    t.add_ref(&def);
    t.push_values(&mappings[def_len..]);
    t.complete();

    t.verify_residency([nr_entries]);
    t.verify_mappings(&mappings);
}

// An underfull node followed by an extra mapping
#[test]
fn unshift_an_underfull_node() {
    let engine = Arc::new(CoreIoEngine::new(64));

    type ValueType = u32;
    let mut t = LeafBuilderTests::<ValueType>::new(engine);

    let mappings = (0..10)
        .zip(1234u32..1244u32)
        .collect::<Vec<(u64, ValueType)>>();

    let def = t.build_def(&mappings[..9]);

    t.add_ref(&def);
    t.push_values(&mappings[9..]);
    t.complete();

    t.verify_residency([10]);
    t.verify_mappings(&mappings);
}

// A special case of residency: three nodes are emitted while there are
// totally less than (2*max_entries) numbers of mappings
#[test]
fn push_an_underfull_node_while_unflushed_entries_just_above_max_entries() {
    let engine = Arc::new(CoreIoEngine::new(64));

    type ValueType = u32;
    let mut t = LeafBuilderTests::<ValueType>::new(engine);

    let half_full = calc_max_entries::<ValueType>() / 2;
    let nr_entries = half_full * 3 + 3;
    let mappings = (0..nr_entries as u64)
        .zip(1234u32..1234u32 + nr_entries as u32)
        .collect::<Vec<(u64, ValueType)>>();

    let unflushed = half_full * 2 + 4;
    let def = t.build_def(&mappings[unflushed..]);

    t.push_values(&mappings[..unflushed]);
    t.add_ref(&def);
    t.complete();

    t.verify_residency([half_full + 2, half_full, half_full + 1]);
    t.verify_mappings(&mappings);
}

// Pushing unrodered keys should panic
#[test]
#[should_panic]
fn push_unordered_keys() {
    let engine = Arc::new(CoreIoEngine::new(64));

    type ValueType = u32;
    let mut t = LeafBuilderTests::<ValueType>::new(engine);

    let mappings: Vec<(u64, u32)> = vec![(1, 100), (0, 0)];

    t.push_values(&mappings);
}

//------------------------------------------
