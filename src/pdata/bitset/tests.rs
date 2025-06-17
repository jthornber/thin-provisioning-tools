use super::*;

use anyhow::Result;

use crate::io_engine::core::CoreIoEngine;
use crate::pdata::array::{calc_max_entries, unpack_array_block, ArrayBlock};
use crate::pdata::array_builder::ArrayBlockBuilder;
use crate::write_batcher::WriteBatcher;

struct BitsetBuilder {
    w: WriteBatcher,
}

impl BitsetBuilder {
    fn new(engine: Arc<dyn IoEngine + Send + Sync>) -> Self {
        let sm = Arc::new(Mutex::new(CoreSpaceMap::<u8>::new(engine.get_nr_blocks())));
        Self {
            w: WriteBatcher::new(engine, sm, 16),
        }
    }

    fn build_bitset_blocks(&mut self, bits: &FixedBitSet) -> Result<Vec<u64>> {
        let nr_entries = div_up(bits.len(), 64);
        let mut builder = ArrayBlockBuilder::<u64>::new(nr_entries as u64);
        let mut cur_pos = 0;
        let mut entry = 0;

        for bit in bits.ones() {
            let pos = (bit >> 6) as u64;
            let mask = 1 << (bit & 63);
            if pos == cur_pos {
                entry |= mask;
            } else {
                builder.push_value(&mut self.w, cur_pos, entry)?;
                cur_pos = pos;
                entry = mask;
            }
        }
        builder.push_value(&mut self.w, cur_pos, entry)?;

        let ablocks = builder.complete(&mut self.w)?;
        self.w.flush()?;

        Ok(ablocks)
    }

    fn read_bitset_blocks(&mut self, blocks: &[u64]) -> Result<Vec<ArrayBlock<u64>>> {
        let mut ablocks = Vec::with_capacity(blocks.len());

        for &bn in blocks {
            let b = self.w.engine.read(bn)?;
            let ablock = unpack_array_block::<u64>(&[bn], b.get_data())?;
            ablocks.push(ablock);
        }

        Ok(ablocks)
    }
}

fn build_bitset_blocks(expected: &FixedBitSet) -> Result<Vec<ArrayBlock<u64>>> {
    let engine = Arc::new(CoreIoEngine::new(64));
    let mut t = BitsetBuilder::new(engine);
    let blocks = t.build_bitset_blocks(expected)?;
    t.read_bitset_blocks(&blocks)
}

fn test_bitset_conversion(expected: &FixedBitSet) -> Result<()> {
    let blocks = build_bitset_blocks(expected)?;

    let collector = BitsetCollector::new(expected.len());
    for (i, b) in blocks.into_iter().enumerate() {
        collector.visit(i as u64, b)?;
    }
    let actual = collector.get_bitset();
    assert_eq!(expected.as_slice(), actual.as_slice()); // compare slices to facilitate debugging

    Ok(())
}

#[test]
fn convert_to_single_entry() {
    let usize_bits = usize::BITS as usize;
    let bitset_len = usize_bits;
    let mut expected = FixedBitSet::with_capacity(bitset_len);
    expected.insert_range(0..2);
    expected.insert_range(bitset_len - 2..bitset_len);
    assert!(test_bitset_conversion(&expected).is_ok());
}

#[test]
fn convert_to_multiple_entries() {
    let usize_bits = usize::BITS as usize;
    let bitset_len = usize_bits * 2;
    let mut expected = FixedBitSet::with_capacity(bitset_len);
    expected.insert_range(0..2);
    expected.insert_range(usize_bits - 1..usize_bits + 1);
    assert!(test_bitset_conversion(&expected).is_ok());
}

#[test]
fn convert_from_multiple_blocks() {
    let bits_per_block = calc_max_entries::<u64>() * 64;
    let bitset_len = bits_per_block * 2 + 1;
    let mut expected = FixedBitSet::with_capacity(bitset_len);
    expected.insert_range(0..2);
    expected.insert_range(bits_per_block - 2..bits_per_block + 1);
    assert!(test_bitset_conversion(&expected).is_ok());
}

#[test]
fn insufficient_collector_size_should_fail() {
    let expected = FixedBitSet::with_capacity(128);
    let mut blocks = build_bitset_blocks(&expected).unwrap();

    let collector = BitsetCollector::new(64);
    let ret = collector.visit(0, blocks.remove(0));
    assert!(ret.is_err());
}
