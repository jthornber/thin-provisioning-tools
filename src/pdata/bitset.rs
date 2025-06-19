use fixedbitset::FixedBitSet;
use std::sync::{Arc, Mutex};

use crate::io_engine::IoEngine;
use crate::math::div_up;
use crate::pdata::array::{self, ArrayBlock};
use crate::pdata::array_walker::{ArrayVisitor, ArrayWalker};
use crate::pdata::space_map::*;

#[cfg(test)]
mod tests;

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
    nr_entries: usize,
}

impl BitsetCollector {
    fn new(nr_bits: usize) -> BitsetCollector {
        BitsetCollector {
            bits: Mutex::new(FixedBitSet::with_capacity(nr_bits)),
            nr_bits,
            nr_entries: div_up(nr_bits, fixedbitset::Block::BITS as usize),
        }
    }

    pub fn get_bitset(self) -> FixedBitSet {
        self.bits.into_inner().unwrap()
    }
}

#[cfg(target_pointer_width = "32")]
fn copy_to_usize_slice_le(dest: &mut [usize], src: &[u64]) {
    assert!(usize::BITS == 32);

    let mut pos = 0;
    for &val in src {
        if pos == dest.len() {
            break;
        }

        dest[pos] = (val & usize::MAX as u64) as usize;
        pos += 1;

        if pos < dest.len() {
            dest[pos] = (val >> usize::BITS as u64) as usize;
            pos += 1;
        }
    }
}

impl ArrayVisitor<u64> for BitsetCollector {
    #[cfg(target_pointer_width = "64")]
    fn visit(&self, index: u64, b: ArrayBlock<u64>) -> array::Result<()> {
        let mut bitset = self.bits.lock().unwrap();
        let begin = index as usize * b.header.max_entries as usize;
        let end = begin + b.values.len();

        if begin >= self.nr_entries || end > self.nr_entries {
            return Err(array::value_err(format!(
                "bitset size exceeds limit: {} bits",
                self.nr_bits
            )));
        }

        let src: &[usize] = unsafe {
            std::slice::from_raw_parts(b.values.as_ptr() as *const usize, b.values.len())
        };
        let dest: &mut [usize] = &mut bitset.as_mut_slice()[begin..end];
        dest.copy_from_slice(src);

        Ok(())
    }

    #[cfg(target_pointer_width = "32")]
    fn visit(&self, index: u64, b: ArrayBlock<u64>) -> array::Result<()> {
        let mut bitset = self.bits.lock().unwrap();
        let begin = (index as usize * b.header.max_entries as usize) * 2;
        let end = begin + b.values.len() * 2;

        /*
         * For 32-bit targets, FixedBitSet stores its bits as an array of 32-bit
         * usize entries, while the source ArrayBlock uses u64. When the number of
         * 32-bit usize entries required to represent the bitset is odd, the upper
         * half of the last u64 in the source is unused during conversion.
         * The check below ensures the source ArrayBlock fits the allocated
         * FixedBitSet, allowing at most one unused usize entry from the source.
         *
         * The check below is safe and correct when `self.nr_entries` is even,
         * since the estimated size `end` is even as well, making
         * `end == self.nr_entries + 1` impossible in this case.
         */
        if begin >= self.nr_entries || end > self.nr_entries + 1 {
            return Err(array::value_err(format!(
                "bitset size exceeds limit: {} bits",
                self.nr_bits
            )));
        }

        let end = std::cmp::min(end, self.nr_entries);
        let dest = &mut bitset.as_mut_slice()[begin..end];
        copy_to_usize_slice_le(dest, &b.values);

        Ok(())
    }
}

//------------------------------------------

// TODO: multi-threaded is possible
pub fn read_bitset_checked(
    engine: &dyn IoEngine,
    root: u64,
    nr_bits: usize,
    ignore_none_fatal: bool,
) -> (CheckedBitSet, Option<array::ArrayError>) {
    let w = ArrayWalker::new(engine, ignore_none_fatal);
    let v = BitsetVisitor::new(nr_bits);
    let err = w.walk(&v, root).err();
    (v.get_bitset(), err)
}

// TODO: multi-threaded is possible
pub fn read_bitset_checked_with_sm(
    engine: &dyn IoEngine,
    root: u64,
    nr_bits: usize,
    sm: Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    ignore_none_fatal: bool,
) -> array::Result<(CheckedBitSet, Option<array::ArrayError>)> {
    let w = ArrayWalker::new_with_sm(engine, sm, ignore_none_fatal)?;
    let v = BitsetVisitor::new(nr_bits);
    let err = w.walk(&v, root).err();
    Ok((v.get_bitset(), err))
}

pub fn read_bitset(
    engine: &dyn IoEngine,
    root: u64,
    nr_bits: usize,
    ignore_none_fatal: bool,
) -> array::Result<FixedBitSet> {
    let w = ArrayWalker::new(engine, ignore_none_fatal);
    let v = BitsetCollector::new(nr_bits);
    w.walk(&v, root)?;
    Ok(v.get_bitset())
}
