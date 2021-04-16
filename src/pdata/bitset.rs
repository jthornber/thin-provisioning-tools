use fixedbitset::FixedBitSet;
use std::sync::{Arc, Mutex};

use crate::io_engine::IoEngine;
use crate::pdata::array::{self, ArrayBlock};
use crate::pdata::array_walker::{ArrayVisitor, ArrayWalker};

struct BitsetVisitor<'a> {
    nr_entries: u64,
    bits: Mutex<&'a mut FixedBitSet>,
}

impl<'a> BitsetVisitor<'a> {
    pub fn new(bitset: &'a mut FixedBitSet) -> Self {
        BitsetVisitor {
            nr_entries: bitset.len() as u64,
            bits: Mutex::new(bitset),
        }
    }
}

impl<'a> ArrayVisitor<u64> for BitsetVisitor<'a> {
    fn visit(&self, index: u64, b: ArrayBlock<u64>) -> array::Result<()> {
        let mut begin = index as usize * (b.header.max_entries as usize) << 6;

        for i in 0..b.header.nr_entries as usize {
            if begin > self.nr_entries as usize {
                return Err(array::value_err("bitset size exceeds expectation".to_string()));
            }

            let end: usize = std::cmp::min(begin + 64, self.nr_entries as usize);
            let mut mask = 1;
            let bits = b.values[i];

            for bi in begin..end {
                self.bits.lock().unwrap().set(bi, bits & mask != 0);
                mask <<= 1;
            }
            begin += 64;
        }
        Ok(())
    }
}

// TODO: multi-threaded is possible
pub fn read_bitset(
    engine: Arc<dyn IoEngine + Send + Sync>,
    root: u64,
    ignore_none_fatal: bool,
    bitset: &mut FixedBitSet,
)-> array::Result<()> {
    let w = ArrayWalker::new(engine.clone(), ignore_none_fatal);
    let mut v = BitsetVisitor::new(bitset);
    w.walk(&mut v, root)
}
