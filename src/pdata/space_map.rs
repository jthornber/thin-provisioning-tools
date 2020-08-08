use anyhow::{anyhow, Result};
use nom::{number::complete::*, IResult};
use std::collections::BTreeMap;

use crate::block_manager::*;
use crate::pdata::btree::Unpack;

//------------------------------------------

#[derive(Debug)]
pub struct SMRoot {
    pub nr_blocks: u64,
    pub nr_allocated: u64,
    pub bitmap_root: u64,
    pub ref_count_root: u64,
}

pub fn unpack_root(data: &[u8]) -> Result<SMRoot> {
    match SMRoot::unpack(data) {
        Err(_e) => Err(anyhow!("couldn't parse SMRoot")),
        Ok((_i, v)) => Ok(v),
    }
}

impl Unpack for SMRoot {
    fn disk_size() -> u32 {
        32
    }

    fn unpack(data: &[u8]) -> IResult<&[u8], SMRoot> {
        let (i, nr_blocks) = le_u64(data)?;
        let (i, nr_allocated) = le_u64(i)?;
        let (i, bitmap_root) = le_u64(i)?;
        let (i, ref_count_root) = le_u64(i)?;

        Ok((
            i,
            SMRoot {
                nr_blocks,
                nr_allocated,
                bitmap_root,
                ref_count_root,
            },
        ))
    }
}

//------------------------------------------

#[derive(Clone, Debug)]
pub struct IndexEntry {
    pub blocknr: u64,
    pub nr_free: u32,
    pub none_free_before: u32,
}

impl Unpack for IndexEntry {
    fn disk_size() -> u32 {
        16
    }

    fn unpack(data: &[u8]) -> IResult<&[u8], Self> {
        let (i, blocknr) = le_u64(data)?;
        let (i, nr_free) = le_u32(i)?;
        let (i, none_free_before) = le_u32(i)?;

        Ok((
            i,
            IndexEntry {
                blocknr,
                nr_free,
                none_free_before,
            },
        ))
    }
}

//------------------------------------------

#[derive(Debug)]
pub struct BitmapHeader {
    pub csum: u32,
    pub not_used: u32,
    pub blocknr: u64,
}

impl Unpack for BitmapHeader {
    fn disk_size() -> u32 {
        16
    }

    fn unpack(data: &[u8]) -> IResult<&[u8], Self> {
        let (i, csum) = le_u32(data)?;
        let (i, not_used) = le_u32(i)?;
        let (i, blocknr) = le_u64(i)?;

        Ok((
            i,
            BitmapHeader {
                csum,
                not_used,
                blocknr,
            },
        ))
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum BitmapEntry {
    Small(u8),
    Overflow,
}

#[derive(Debug)]
pub struct Bitmap {
    pub header: BitmapHeader,
    pub entries: Vec<BitmapEntry>,
}

impl Unpack for Bitmap {
    fn disk_size() -> u32 {
        BLOCK_SIZE as u32
    }

    fn unpack(data: &[u8]) -> IResult<&[u8], Self> {
        let (mut i, header) = BitmapHeader::unpack(data)?;

        let mut entries = Vec::new();
        let nr_words = (BLOCK_SIZE - BitmapHeader::disk_size() as usize) / 8;
        for _w in 0..nr_words {
            let (tmp, mut word) = le_u64(i)?;

            for _b in 0..32 {
                let val = word & 0x3;
                word >>= 2;

                if val < 3 {
                    entries.push(BitmapEntry::Small(val as u8));
                } else {
                    entries.push(BitmapEntry::Overflow);
                }
            }

            i = tmp;
        }

        Ok((i, Bitmap { header, entries }))
    }
}

//------------------------------------------

const ENTRIES_PER_WORD: u64 = 32;

pub struct CoreSpaceMap {
    nr_entries: u64,
    bits: Vec<u64>,
    overflow: BTreeMap<u64, u32>,
}

impl CoreSpaceMap {
    pub fn new(nr_entries: u64) -> CoreSpaceMap {
        let nr_words = (nr_entries + ENTRIES_PER_WORD - 1) / ENTRIES_PER_WORD;
        CoreSpaceMap {
            nr_entries,
            bits: vec![0; nr_words as usize],
            overflow: BTreeMap::new(),
        }
    }

    fn check_bounds(&self, b: u64) -> Result<()> {
        if b >= self.nr_entries {
            return Err(anyhow!("space map index out of bounds"));
        }
        Ok(())
    }

    fn get_index(b: u64) -> (usize, usize) {
        (
            (b / ENTRIES_PER_WORD) as usize,
            ((b & (ENTRIES_PER_WORD - 1)) as usize) * 2,
        )
    }

    fn get_bits(&self, b: u64) -> Result<u32> {
        self.check_bounds(b)?;

        let result;
        let (w, bit) = CoreSpaceMap::get_index(b);
        unsafe {
            let word = self.bits.get_unchecked(w);
            result = (*word >> bit) & 0x3;
        }

        Ok(result as u32)
    }

    pub fn get(&self, b: u64) -> Result<u32> {
        let result = self.get_bits(b)?;
        if result < 3 {
            Ok(result)
        } else {
            match self.overflow.get(&b) {
                None => Err(anyhow!(
                    "internal error: missing overflow entry in space map"
                )),
                Some(result) => Ok(*result),
            }
        }
    }

    pub fn inc(&mut self, b: u64) -> Result<()> {
        self.check_bounds(b)?;

        let (w, bit) = CoreSpaceMap::get_index(b);
        let count;

        unsafe {
            let word = self.bits.get_unchecked_mut(w);
            count = (*word >> bit) & 0x3;

            if count < 3 {
                // bump up the bits
                *word = (*word & !(0x3 << bit)) | (((count + 1) as u64) << bit);

                if count == 2 {
                    // insert overflow entry
                    self.overflow.insert(b, 1);
                }
            }
        }

        if count >= 3 {
            if let Some(count) = self.overflow.get_mut(&b) {
                *count = *count + 1;
            } else {
                return Err(anyhow!(
                    "internal error: missing overflow entry in space map"
                ));
            }
        }

        Ok(())
    }
}

//------------------------------------------
