use fixedbitset::FixedBitSet;
use std::sync::{Arc, Mutex};

use crate::io_engine::IoEngine;
use crate::pdata::array;
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
    fn visit(&self, index: u64, bits: u64) -> array::Result<()> {
        let begin: u64 = index << 6;
        if begin > self.nr_entries {
            return Err(array::value_err("bitset size exceeds expectation".to_string()));
        }

        let end: u64 = std::cmp::min(begin + 64, self.nr_entries);
        let mut mask = 1;
        for i in begin..end {
            self.bits.lock().unwrap().set(i as usize, bits & mask != 0);
            mask <<= 1;
        }
        Ok(())
    }
}

// TODO: remap errors
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
