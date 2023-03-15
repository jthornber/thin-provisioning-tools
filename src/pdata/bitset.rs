use fixedbitset::FixedBitSet;
use std::sync::{Arc, Mutex};

use crate::io_engine::IoEngine;
use crate::math::div_up;
use crate::pdata::array::{self, ArrayBlock};
use crate::pdata::array_walker::{ArrayVisitor, ArrayWalker};
use crate::pdata::space_map::*;

//------------------------------------------

pub struct CheckedBitSet {
    bits: FixedBitSet,
}

impl CheckedBitSet {
    pub fn with_capacity(bits: usize) -> CheckedBitSet {
        CheckedBitSet {
            bits: FixedBitSet::with_capacity(bits << 1),
        }
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.bits.len() / 2
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

//------------------------------------------

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
        if begin >= self.nr_bits {
            return Err(array::value_err(format!(
                "bitset size exceeds limit: {} bits",
                self.nr_bits
            )));
        }

        for bits in b.values.iter() {
            let end: usize = std::cmp::min(begin + 64, self.nr_bits);

            let mut mask = 1;
            let mut extracted_bits = self.bits.lock().unwrap();

            for bi in begin..end {
                extracted_bits.set(bi, bits & mask != 0);
                mask <<= 1;
            }
            begin += 64;
        }
        Ok(())
    }
}

//------------------------------------------

struct BitsetCollector {
    bits: Mutex<FixedBitSet>,
    nr_bits: usize,
}

impl BitsetCollector {
    fn new(nr_bits: usize) -> BitsetCollector {
        BitsetCollector {
            bits: Mutex::new(FixedBitSet::with_capacity(nr_bits)),
            nr_bits,
        }
    }

    pub fn get_bitset(self) -> FixedBitSet {
        self.bits.into_inner().unwrap()
    }
}

impl ArrayVisitor<u64> for BitsetCollector {
    fn visit(&self, index: u64, b: ArrayBlock<u64>) -> array::Result<()> {
        let mut bitset = self.bits.lock().unwrap();
        let mut idx = (index as usize * b.header.max_entries as usize) << 1; // index of u32 in bitset array
        let idx_end = div_up(self.nr_bits, 32);
        let mut dest = bitset.as_mut_slice().iter_mut().skip(idx);
        for entry in b.values.iter() {
            let lower = (*entry & (u32::MAX as u64)) as u32;
            *(dest.next().ok_or_else(|| {
                array::value_err(format!("bitset size exceeds limit: {} bits", self.nr_bits))
            })?) = lower;
            idx += 1;

            if idx == idx_end {
                break;
            }

            let upper = (*entry >> 32) as u32;
            *(dest.next().ok_or_else(|| {
                array::value_err(format!("bitset size exceeds limit: {} bits", self.nr_bits))
            })?) = upper;
            idx += 1;
        }
        Ok(())
    }
}

//------------------------------------------

// TODO: multi-threaded is possible
pub fn read_bitset_checked(
    engine: Arc<dyn IoEngine + Send + Sync>,
    root: u64,
    nr_bits: usize,
    ignore_none_fatal: bool,
) -> (CheckedBitSet, Option<array::ArrayError>) {
    let w = ArrayWalker::new(engine, ignore_none_fatal);
    let v = BitsetVisitor::new(nr_bits);
    let err = match w.walk(&v, root) {
        Ok(()) => None,
        Err(e) => Some(e),
    };
    (v.get_bitset(), err)
}

// TODO: multi-threaded is possible
pub fn read_bitset_checked_with_sm(
    engine: Arc<dyn IoEngine + Send + Sync>,
    root: u64,
    nr_bits: usize,
    sm: Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    ignore_none_fatal: bool,
) -> array::Result<(CheckedBitSet, Option<array::ArrayError>)> {
    let w = ArrayWalker::new_with_sm(engine, sm, ignore_none_fatal)?;
    let v = BitsetVisitor::new(nr_bits);
    let err = match w.walk(&v, root) {
        Ok(()) => None,
        Err(e) => Some(e),
    };
    Ok((v.get_bitset(), err))
}

pub fn read_bitset(
    engine: Arc<dyn IoEngine + Send + Sync>,
    root: u64,
    nr_bits: usize,
    ignore_none_fatal: bool,
) -> array::Result<FixedBitSet> {
    let w = ArrayWalker::new(engine, ignore_none_fatal);
    let v = BitsetCollector::new(nr_bits);
    w.walk(&v, root)?;
    Ok(v.get_bitset())
}
