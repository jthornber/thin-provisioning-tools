use fixedbitset::FixedBitSet;
use std::sync::{Arc, Mutex};

use crate::io_engine::IoEngine;
use crate::pdata::array::{self, ArrayBlock};
use crate::pdata::array_walker::{ArrayVisitor, ArrayWalker};
use crate::pdata::space_map::*;

pub struct CheckedBitSet {
    bits: FixedBitSet,
}

impl CheckedBitSet {
    pub fn with_capacity(bits: usize) -> CheckedBitSet {
        CheckedBitSet {
            bits: FixedBitSet::with_capacity(bits << 1),
        }
    }

    pub fn set(&mut self, bit: usize, enabled: bool) {
        self.bits.set(bit << 1, true);
        self.bits.set((bit << 1) + 1, enabled);
    }

    pub fn contains(&self, bit: usize) -> Option<bool> {
        if !self.bits.contains(bit << 1) {
            return None;
        }
        Some(self.bits.contains((bit << 1) + 1))
    }
}

struct BitsetVisitor {
    nr_bits: usize,
    bits: Mutex<CheckedBitSet>,
}

impl BitsetVisitor {
    pub fn new(nr_bits: usize) -> Self {
        BitsetVisitor {
            nr_bits,
            bits: Mutex::new(CheckedBitSet::with_capacity(nr_bits)),
        }
    }

    pub fn get_bitset(self) -> CheckedBitSet {
        self.bits.into_inner().unwrap()
    }
}

impl ArrayVisitor<u64> for BitsetVisitor {
    fn visit(&self, index: u64, b: ArrayBlock<u64>) -> array::Result<()> {
        let mut begin = (index as usize * (b.header.max_entries as usize)) << 6;
        if begin >= self.nr_bits as usize {
            return Err(array::value_err(format!(
                "bitset size exceeds limit: {} bits",
                self.nr_bits
            )));
        }

        for bits in b.values.iter() {
            let end: usize = std::cmp::min(begin + 64, self.nr_bits as usize);
            let mut mask = 1;

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
    nr_bits: usize,
    ignore_none_fatal: bool,
) -> (CheckedBitSet, Option<array::ArrayError>) {
    let w = ArrayWalker::new(engine, ignore_none_fatal);
    let mut v = BitsetVisitor::new(nr_bits);
    let err = w.walk(&mut v, root);
    let e = match err {
        Ok(()) => None,
        Err(e) => Some(e),
    };
    (v.get_bitset(), e)
}

// TODO: multi-threaded is possible
pub fn read_bitset_with_sm(
    engine: Arc<dyn IoEngine + Send + Sync>,
    root: u64,
    nr_bits: usize,
    sm: Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    ignore_none_fatal: bool,
) -> array::Result<(CheckedBitSet, Option<array::ArrayError>)> {
    let w = ArrayWalker::new_with_sm(engine, sm, ignore_none_fatal)?;
    let mut v = BitsetVisitor::new(nr_bits);
    let err = w.walk(&mut v, root);
    let e = match err {
        Ok(()) => None,
        Err(e) => Some(e),
    };
    Ok((v.get_bitset(), e))
}
