use anyhow::{anyhow, Result};
use nom::{number::complete::*, IResult};

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
        Err(_e) => {
            Err(anyhow!("couldn't parse SMRoot"))
        },
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

        Ok  ((i, SMRoot {
            nr_blocks,
            nr_allocated,
            bitmap_root,
            ref_count_root,
        }))
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

        Ok((i, IndexEntry {blocknr, nr_free, none_free_before}))
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

        Ok((i, BitmapHeader {csum, not_used, blocknr} ))
    }
}

#[derive(Debug)]
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
                word = word >> 2;

                if val < 3 {
                    entries.push(BitmapEntry::Small(val as u8));
                } else {
                    entries.push(BitmapEntry::Overflow);
                }
            }

            i = tmp;
        }

        Ok((i, Bitmap {header, entries}))
    }
}

//------------------------------------------
