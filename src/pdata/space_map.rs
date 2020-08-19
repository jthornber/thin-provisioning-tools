use anyhow::{anyhow, Result};
use fixedbitset::FixedBitSet;
use nom::{multi::count, number::complete::*, IResult};
use std::sync::{Arc, Mutex};
use byteorder::{LittleEndian, WriteBytesExt};

use crate::io_engine::*;
use crate::pdata::unpack::{Pack, Unpack};

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

pub const MAX_METADATA_BITMAPS: usize = 255;

pub struct MetadataIndex {
    pub indexes: Vec<IndexEntry>,
}

impl Unpack for MetadataIndex {
    fn disk_size() -> u32 {
        BLOCK_SIZE as u32
    }

    fn unpack(data: &[u8]) -> IResult<&[u8], Self> {
        let (i, _csum) = le_u32(data)?;
        let (i, _padding) = le_u32(i)?;
        let (i, _blocknr) = le_u64(i)?;
        let (i, indexes) = count(IndexEntry::unpack, MAX_METADATA_BITMAPS)(i)?;

        Ok((i, MetadataIndex {indexes}))
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

impl Pack for BitmapHeader {
    fn pack<W: WriteBytesExt>(&self, out: &mut W) -> Result<()> {
        out.write_u32::<LittleEndian>(self.csum)?;
        out.write_u32::<LittleEndian>(self.not_used)?;
        out.write_u64::<LittleEndian>(self.blocknr)?;
        Ok(())
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

                // The bits are stored with the high bit at b * 2 + 1,
                // and low at b *2.  So we have to interpret this val.
                entries.push(match val {
                    0 => BitmapEntry::Small(0),
                    1 => BitmapEntry::Small(2),
                    2 => BitmapEntry::Small(1),
                    _ => BitmapEntry::Overflow,
                });
            }

            i = tmp;
        }

        Ok((i, Bitmap { header, entries }))
    }
}

impl Pack for Bitmap {
    fn pack<W: WriteBytesExt>(&self, out: &mut W) -> Result<()> {
        use BitmapEntry::*;
        BitmapHeader::pack(&self.header, out)?;

        for chunk in self.entries.chunks(32) {
            let mut w = 0u64;
            for e in chunk {
                w >>= 2;
                match e {
                    Small(0) => {
                    },
                    Small(1) => {
                        w |= 0x2 << 62;
                    },
                    Small(2) => {
                        w |= 0x1 << 62;
                    },
                    Small(_) => {
                        return Err(anyhow!("Bad small value in bitmap entry"));
                    },
                    Overflow => {
                        w |= 0x3 << 62;
                    }
                }
            }

            u64::pack(&w, out)?;
        }

        Ok(())
    }
}

//------------------------------------------

pub trait SpaceMap {
    fn get_nr_blocks(&self) -> Result<u64>;
    fn get_nr_allocated(&self) -> Result<u64>;
    fn get(&self, b: u64) -> Result<u32>;
    fn inc(&mut self, begin: u64, len: u64) -> Result<()>;
}

pub type ASpaceMap = Arc<Mutex<dyn SpaceMap + Sync + Send>>;

//------------------------------------------

pub struct CoreSpaceMap<T> {
    nr_allocated: u64,
    counts: Vec<T>,
}

impl<V> CoreSpaceMap<V>
where
    V: Copy + Default + std::ops::AddAssign + From<u8>,
{
    pub fn new(nr_entries: u64) -> CoreSpaceMap<V> {
        CoreSpaceMap {
            nr_allocated: 0,
            counts: vec![V::default(); nr_entries as usize],
        }
    }
}

impl<V> SpaceMap for CoreSpaceMap<V>
where
    V: Copy + Default + Eq + std::ops::AddAssign + From<u8> + Into<u32>,
{
    fn get_nr_blocks(&self) -> Result<u64> {
        Ok(self.counts.len() as u64)
    }

    fn get_nr_allocated(&self) -> Result<u64> {
        Ok(self.nr_allocated)
    }

    fn get(&self, b: u64) -> Result<u32> {
        Ok(self.counts[b as usize].into())
    }

    fn inc(&mut self, begin: u64, len: u64) -> Result<()> {
        for b in begin..(begin + len) {
            if self.counts[b as usize] == V::from(0u8) {
                // FIXME: can we get a ref to save dereferencing counts twice?
                self.nr_allocated += 1;
                self.counts[b as usize] = V::from(1u8);
            } else {
                self.counts[b as usize] += V::from(1u8);
            }
        }
        Ok(())
    }
}

pub fn core_sm(nr_entries: u64, max_count: u32) -> Arc<Mutex<dyn SpaceMap + Send + Sync>> {
    if max_count <= u8::MAX as u32 {
        Arc::new(Mutex::new(CoreSpaceMap::<u8>::new(nr_entries)))
    } else if max_count <= u16::MAX as u32 {
        Arc::new(Mutex::new(CoreSpaceMap::<u16>::new(nr_entries)))
    } else {
        Arc::new(Mutex::new(CoreSpaceMap::<u32>::new(nr_entries)))
    }
}

//------------------------------------------

// This in core space map can only count to one, useful when walking
// btrees when we want to avoid visiting a node more than once, but
// aren't interested in counting how many times we've visited.
pub struct RestrictedSpaceMap {
    nr_allocated: u64,
    counts: FixedBitSet,
}

impl RestrictedSpaceMap {
    pub fn new(nr_entries: u64) -> RestrictedSpaceMap {
        RestrictedSpaceMap {
            nr_allocated: 0,
            counts: FixedBitSet::with_capacity(nr_entries as usize),
        }
    }
}

impl SpaceMap for RestrictedSpaceMap {
    fn get_nr_blocks(&self) -> Result<u64> {
        Ok(self.counts.len() as u64)
    }

    fn get_nr_allocated(&self) -> Result<u64> {
        Ok(self.nr_allocated)
    }

    fn get(&self, b: u64) -> Result<u32> {
        if self.counts.contains(b as usize) {
            Ok(1)
        } else {
            Ok(0)
        }
    }

    fn inc(&mut self, begin: u64, len: u64) -> Result<()> {
        for b in begin..(begin + len) {
            if !self.counts.contains(b as usize) {
                self.nr_allocated += 1;
                self.counts.insert(b as usize);
            }
        }
        Ok(())
    }
}

//------------------------------------------
