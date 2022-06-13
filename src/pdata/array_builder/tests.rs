use super::*;

use core::ops::Range;
use std::sync::{Arc, Mutex};

use crate::io_engine::core::CoreIoEngine;
use crate::io_engine::IoEngine;
use crate::pdata::space_map::CoreSpaceMap;

//------------------------------------------

struct ArrayBlockBuilderTests<V: Pack + Unpack + Clone> {
    w: WriteBatcher,
    nr_entries: u64,
    builder: Option<ArrayBlockBuilder<V>>,
    ablocks: Vec<u64>,
}

impl<V: Pack + Unpack + Default + Clone + PartialEq + std::fmt::Debug> ArrayBlockBuilderTests<V> {
    fn new(engine: Arc<dyn IoEngine + Send + Sync>, nr_entries: u64) -> ArrayBlockBuilderTests<V> {
        let sm = Arc::new(Mutex::new(CoreSpaceMap::<u8>::new(engine.get_nr_blocks())));
        let builder = ArrayBlockBuilder::<V>::new(nr_entries);

        ArrayBlockBuilderTests {
            w: WriteBatcher::new(engine, sm, 16),
            nr_entries,
            builder: Some(builder),
            ablocks: Vec::new(),
        }
    }

    fn push_value(&mut self, index: u64, value: V) -> bool {
        assert!(self.builder.is_some());
        let builder = self.builder.as_mut().unwrap();
        builder.push_value(&mut self.w, index, value).is_ok()
    }

    fn push_values(&mut self, indices: Range<u64>, values: &[V]) {
        assert!(self.builder.is_some());
        let builder = self.builder.as_mut().unwrap();
        for (i, v) in indices.zip(values) {
            assert!(builder.push_value(&mut self.w, i, v.clone()).is_ok());
        }
    }

    fn complete(&mut self) {
        assert!(self.builder.is_some());
        let builder = self.builder.take().unwrap();
        self.ablocks = builder.complete(&mut self.w).unwrap();
        assert!(self.w.flush().is_ok());
    }

    fn expect_nr_blocks(&self, nr_blocks: usize) {
        assert!(self.builder.is_none());
        assert_eq!(self.ablocks.len(), nr_blocks);
    }

    fn verify_nr_blocks(&self) {
        assert!(self.builder.is_none());
        let max_entries = calc_max_entries::<V>();
        let nr_blocks = div_up(self.nr_entries, max_entries as u64);
        assert_eq!(self.ablocks.len() as u64, nr_blocks);
    }

    fn verify_values(&self, values: &[V]) {
        assert!(self.builder.is_none());

        let max_entries = calc_max_entries::<V>() as u32;
        let nr_ablocks = self.ablocks.len();

        let mut m_iter = values.iter();
        for (idx, blk) in self.ablocks.iter().enumerate() {
            let blk = *blk;
            let b = self.w.engine.read(blk).unwrap();
            let ablock = unpack_array_block::<V>(&[blk], b.get_data()).unwrap();

            // verify the header
            assert_eq!(ablock.header.blocknr, blk);
            assert_eq!(ablock.header.max_entries, max_entries);
            assert_eq!(ablock.header.value_size, V::disk_size());

            if idx == nr_ablocks - 1 {
                let last_nr_entries = if values.len() % nr_ablocks == 0 {
                    max_entries
                } else {
                    (values.len() % nr_ablocks) as u32
                };
                assert_eq!(ablock.header.nr_entries, last_nr_entries);
            } else {
                assert_eq!(ablock.header.nr_entries, max_entries);
            }

            // verify values
            for v in ablock.values {
                assert_eq!(&v, m_iter.next().unwrap());
            }
        }
    }
}

//------------------------------------------

#[test]
fn build_an_array_block() {
    let engine = Arc::new(CoreIoEngine::new(64));

    type ValueType = u32;
    let mut t = ArrayBlockBuilderTests::<ValueType>::new(engine, 1);
    t.complete();

    t.expect_nr_blocks(1);
}

#[test]
fn build_multiple_array_blocks() {
    let engine = Arc::new(CoreIoEngine::new(64));

    type ValueType = u32;
    let array_size = calc_max_entries::<ValueType>() * 2 + 1;
    let mut t = ArrayBlockBuilderTests::<ValueType>::new(engine, array_size as u64);
    t.complete();

    t.expect_nr_blocks(3);
}

#[test]
fn build_fully_populated_blocks() {
    let engine = Arc::new(CoreIoEngine::new(64));

    type ValueType = u32;
    let array_size = calc_max_entries::<ValueType>() * 3;
    let mut t = ArrayBlockBuilderTests::<ValueType>::new(engine, array_size as u64);

    let values = (1234u32..1234u32 + array_size as u32).collect::<Vec<u32>>();
    t.push_values(0..array_size as u64, &values);
    t.complete();

    t.verify_nr_blocks();
    t.verify_values(&values);
}

#[test]
fn leading_default_values() {
    let engine = Arc::new(CoreIoEngine::new(64));

    type ValueType = u32;
    let array_size = calc_max_entries::<ValueType>() * 8;
    let mut t = ArrayBlockBuilderTests::<ValueType>::new(engine, array_size as u64);

    assert!(t.push_value(array_size as u64 - 1, 1234));
    t.complete();

    t.verify_nr_blocks();

    let mut expected = Vec::<ValueType>::new();
    expected.resize(array_size, 0);
    expected[array_size - 1] = 1234;
    t.verify_values(&expected);
}

#[test]
fn trailing_default_values() {
    let engine = Arc::new(CoreIoEngine::new(64));

    type ValueType = u32;
    let array_size = calc_max_entries::<ValueType>() * 8;
    let mut t = ArrayBlockBuilderTests::<ValueType>::new(engine, array_size as u64);

    assert!(t.push_value(0, 1234));
    t.complete();

    t.verify_nr_blocks();

    let mut expected = vec![1234u32];
    expected.resize(array_size, 0);
    t.verify_values(&expected);
}

#[test]
fn short_gaps_within_a_block() {
    let engine = Arc::new(CoreIoEngine::new(64));

    type ValueType = u32;
    let array_size = calc_max_entries::<ValueType>() * 2;
    let mut t = ArrayBlockBuilderTests::<ValueType>::new(engine, array_size as u64);

    assert!(t.push_value(0, 1234));
    assert!(t.push_value(100, 2345));
    assert!(t.push_value(200, 3456));
    assert!(t.push_value(300, 4567));
    assert!(t.push_value(400, 5678));

    t.complete();

    t.verify_nr_blocks();

    let mut expected = Vec::<ValueType>::new();
    expected.resize(array_size, 0);
    expected[0] = 1234;
    expected[100] = 2345;
    expected[200] = 3456;
    expected[300] = 4567;
    expected[400] = 5678;
    t.verify_values(&expected);
}

#[test]
fn long_stride_across_multiple_blocks() {
    let engine = Arc::new(CoreIoEngine::new(64));

    type ValueType = u32;
    let array_size = calc_max_entries::<ValueType>() * 8;
    let mut t = ArrayBlockBuilderTests::<ValueType>::new(engine, array_size as u64);

    assert!(t.push_value(10, 1234));
    assert!(t.push_value(array_size as u64 - 1, 2345));
    t.complete();

    t.verify_nr_blocks();

    let mut expected = Vec::<ValueType>::new();
    expected.resize(array_size, 0);
    expected[10] = 1234;
    expected[array_size - 1] = 2345;
    t.verify_values(&expected);
}

#[test]
fn out_of_order_indices_should_fail() {
    let engine = Arc::new(CoreIoEngine::new(64));

    type ValueType = u32;
    let array_size = calc_max_entries::<ValueType>() * 8;
    let mut t = ArrayBlockBuilderTests::<ValueType>::new(engine, array_size as u64);

    let index = calc_max_entries::<u32>() as u64;
    assert!(t.push_value(index, 1234));
    assert!(!t.push_value(index - 1, 2345));
    t.complete();
}

#[test]
fn out_of_order_access_could_be_recovered() {
    let engine = Arc::new(CoreIoEngine::new(64));

    type ValueType = u32;
    let array_size = calc_max_entries::<ValueType>() * 8;
    let mut t = ArrayBlockBuilderTests::<ValueType>::new(engine, array_size as u64);

    assert!(t.push_value(10, 1234));
    assert!(!t.push_value(9, 3456));
    assert!(t.push_value(11, 5678));
    t.complete();

    t.verify_nr_blocks();

    let mut expected = Vec::<ValueType>::new();
    expected.resize(array_size, 0);
    expected[10] = 1234;
    expected[11] = 5678;
    t.verify_values(&expected);
}

#[test]
fn out_of_bounds_access_should_fail() {
    let engine = Arc::new(CoreIoEngine::new(64));

    type ValueType = u32;
    let array_size = calc_max_entries::<ValueType>() * 8;
    let mut t = ArrayBlockBuilderTests::<ValueType>::new(engine, array_size as u64);

    assert!(t.push_value(10, 1234));
    assert!(!t.push_value(array_size as u64, 3456));
    t.complete();
}

#[test]
fn out_of_bounds_access_could_be_recovered() {
    let engine = Arc::new(CoreIoEngine::new(64));

    type ValueType = u32;
    let array_size = calc_max_entries::<ValueType>() * 8;
    let mut t = ArrayBlockBuilderTests::<ValueType>::new(engine, array_size as u64);

    assert!(t.push_value(10, 1234));
    assert!(!t.push_value(array_size as u64, 3456));
    assert!(t.push_value(11, 5678));
    t.complete();

    t.verify_nr_blocks();

    let mut expected = Vec::<ValueType>::new();
    expected.resize(array_size, 0);
    expected[10] = 1234;
    expected[11] = 5678;
    t.verify_values(&expected);
}

//------------------------------------------
