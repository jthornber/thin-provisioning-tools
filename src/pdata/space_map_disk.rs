use anyhow::{anyhow, Result};
use byteorder::{LittleEndian, WriteBytesExt};
use nom::{number::complete::*, IResult};
use std::io::Cursor;
use std::collections::BTreeMap;

use crate::checksum;
use crate::io_engine::*;
use crate::math::*;
use crate::pdata::btree_builder::*;
use crate::pdata::space_map::*;
use crate::pdata::unpack::*;
use crate::write_batcher::*;

//--------------------------------

const MAX_METADATA_BITMAPS: usize = 255;
// const MAX_METADATA_BLOCKS: u64 = 255 * ((1 << 14) - 64);
const ENTRIES_PER_BYTE: usize = 4;
const ENTRIES_PER_BITMAP: usize = WORDS_PER_BITMAP * 8 * ENTRIES_PER_BYTE;

//--------------------------------

#[derive(Clone, Copy, Debug)]
pub struct IndexEntry {
    pub blocknr: u64,
    pub nr_free: u32,
    pub none_free_before: u32,
}

impl Unpack for IndexEntry {
    fn disk_size() -> u32 {
        16
    }

    fn unpack(i: &[u8]) -> IResult<&[u8], IndexEntry> {
        let (i, blocknr) = le_u64(i)?;
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

impl Pack for IndexEntry {
    fn pack<W: WriteBytesExt>(&self, w: &mut W) -> Result<()> {
        w.write_u64::<LittleEndian>(self.blocknr)?;
        w.write_u32::<LittleEndian>(self.nr_free)?;
        w.write_u32::<LittleEndian>(self.none_free_before)?;
        Ok(())
    }
}

//--------------------------------

pub struct MetadataIndex {
    pub blocknr: u64,
    pub indexes: Vec<IndexEntry>,
}

impl Unpack for MetadataIndex {
    fn disk_size() -> u32 {
        BLOCK_SIZE as u32
    }

    fn unpack(i: &[u8]) -> IResult<&[u8], MetadataIndex> {
        // FIXME: check the checksum
        let (i, _csum) = le_u32(i)?;
        let (i, _padding) = le_u32(i)?;
        let (i, blocknr) = le_u64(i)?;
        let (i, indexes) = nom::multi::count(IndexEntry::unpack, MAX_METADATA_BITMAPS)(i)?;

        Ok((i, MetadataIndex { blocknr, indexes }))
    }
}

impl Pack for MetadataIndex {
    fn pack<W: WriteBytesExt>(&self, w: &mut W) -> Result<()> {
        w.write_u32::<LittleEndian>(0)?; // csum
        w.write_u32::<LittleEndian>(0)?; // padding
        w.write_u64::<LittleEndian>(self.blocknr)?;

        assert!(self.indexes.len() <= MAX_METADATA_BITMAPS);

        for ie in &self.indexes {
            ie.pack(w)?;
        }

        Ok(())
    }
}

//--------------------------------

const WORDS_PER_BITMAP: usize = (BLOCK_SIZE - 16) / 8;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BitmapEntry {
    Small(u8),
    Overflow,
}

#[derive(Debug)]
pub struct Bitmap {
    pub blocknr: u64,
    pub entries: Vec<BitmapEntry>,
}

impl Unpack for Bitmap {
    fn disk_size() -> u32 {
        BLOCK_SIZE as u32
    }

    fn unpack(data: &[u8]) -> IResult<&[u8], Self> {
        let (i, _csum) = le_u32(data)?;
        let (i, _not_used) = le_u32(i)?;
        let (mut i, blocknr) = le_u64(i)?;

        let header_size = 16;
        let nr_words = (BLOCK_SIZE - header_size) / 8;
        let mut entries = Vec::with_capacity(nr_words * 32);
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

        Ok((i, Bitmap { blocknr, entries }))
    }
}

impl Pack for Bitmap {
    fn pack<W: WriteBytesExt>(&self, out: &mut W) -> Result<()> {
        use BitmapEntry::*;

        out.write_u32::<LittleEndian>(0)?;
        out.write_u32::<LittleEndian>(0)?;
        out.write_u64::<LittleEndian>(self.blocknr)?;

        for chunk in self.entries.chunks(32) {
            let mut w = 0u64;
            for e in chunk {
                w >>= 2;
                match e {
                    Small(0) => {}
                    Small(1) => {
                        w |= 0x2 << 62;
                    }
                    Small(2) => {
                        w |= 0x1 << 62;
                    }
                    Small(_) => {
                        return Err(anyhow!("Bad small value in bitmap entry"));
                    }
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

//--------------------------------

#[derive(Debug)]
pub struct SMRoot {
    pub nr_blocks: u64,
    pub nr_allocated: u64,
    pub bitmap_root: u64,
    pub ref_count_root: u64,
}

impl Unpack for SMRoot {
    fn disk_size() -> u32 {
        32
    }

    fn unpack(i: &[u8]) -> IResult<&[u8], Self> {
        let (i, nr_blocks) = le_u64(i)?;
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

pub fn unpack_root(data: &[u8]) -> Result<SMRoot> {
    match SMRoot::unpack(data) {
        Err(_e) => Err(anyhow!("couldn't parse SMRoot")),
        Ok((_i, v)) => Ok(v),
    }
}

impl Pack for SMRoot {
    fn pack<W: WriteBytesExt>(&self, w: &mut W) -> Result<()> {
        w.write_u64::<LittleEndian>(self.nr_blocks)?;
        w.write_u64::<LittleEndian>(self.nr_allocated)?;
        w.write_u64::<LittleEndian>(self.bitmap_root)?;
        w.write_u64::<LittleEndian>(self.ref_count_root)?;

        Ok(())
    }
}

//--------------------------------

pub fn write_common(w: &mut WriteBatcher, sm: &dyn SpaceMap) -> Result<(Vec<IndexEntry>, u64)> {
    use BitmapEntry::*;

    let mut index_entries = Vec::new();
    let mut overflow_builder: Builder<u32> = Builder::new(Box::new(NoopRC {}));

    // how many bitmaps do we need?
    for bm in 0..div_up(sm.get_nr_blocks()? as usize, ENTRIES_PER_BITMAP) {
        let mut entries = Vec::with_capacity(ENTRIES_PER_BITMAP);
        let mut first_free: Option<u32> = None;
        let mut nr_free: u32 = 0;
        for i in 0..ENTRIES_PER_BITMAP {
            let b: u64 = ((bm * ENTRIES_PER_BITMAP) as u64) + i as u64;
            if b > sm.get_nr_blocks()? {
                break;
            }
            let rc = sm.get(b)?;
            let e = match rc {
                0 => {
                    nr_free += 1;
                    if first_free.is_none() {
                        first_free = Some(i as u32);
                    }
                    Small(0)
                }
                1 => Small(1),
                2 => Small(2),
                _ => {
                    overflow_builder.push_value(w, b as u64, rc)?;
                    Overflow
                }
            };
            entries.push(e);
        }

        // allocate a new block
        let b = w.alloc()?;
        let mut cursor = Cursor::new(b.get_data());

        // write the bitmap to it
        let blocknr = b.loc;
        let bitmap = Bitmap { blocknr, entries };
        bitmap.pack(&mut cursor)?;
        w.write(b, checksum::BT::BITMAP)?;

        // Insert into the index tree
        let ie = IndexEntry {
            blocknr,
            nr_free,
            none_free_before: first_free.unwrap_or(ENTRIES_PER_BITMAP as u32),
        };
        index_entries.push(ie);
    }

    let ref_count_root = overflow_builder.complete(w)?;
    Ok((index_entries, ref_count_root))
}

pub fn write_disk_sm(w: &mut WriteBatcher, sm: &dyn SpaceMap) -> Result<SMRoot> {
    let (index_entries, ref_count_root) = write_common(w, sm)?;
  
    let mut index_builder: Builder<IndexEntry> = Builder::new(Box::new(NoopRC {}));
    for (i, ie) in index_entries.iter().enumerate() {
        index_builder.push_value(w, i as u64, *ie)?;
    }

    let bitmap_root = index_builder.complete(w)?;

    Ok(SMRoot {
        nr_blocks: sm.get_nr_blocks()?,
        nr_allocated: sm.get_nr_allocated()?,
        bitmap_root,
        ref_count_root,
    })
}

//----------------------------

fn block_to_bitmap(b: u64) -> usize {
    (b / ENTRIES_PER_BITMAP as u64) as usize
}

fn adjust_counts(w: &mut WriteBatcher, ie: &IndexEntry, allocs: &[u64]) -> Result<IndexEntry> {
    use BitmapEntry::*;

    let mut first_free = ie.none_free_before;
    let mut nr_free = ie.nr_free - allocs.len() as u32;

    // Read the bitmap
    let bitmap_block = w.engine.read(ie.blocknr)?;
    let (_, mut bitmap) = Bitmap::unpack(bitmap_block.get_data())?;

    // Update all the entries
    for a in allocs {
        if first_free == *a as u32 {
            first_free = *a as u32 + 1;
        }

        if bitmap.entries[*a as usize] == Small(0) {
            nr_free -= 1;
        }
        
        bitmap.entries[*a as usize] = Small(1);
    }

    // Write the bitmap
    let mut cur = Cursor::new(bitmap_block.get_data());
    bitmap.pack(&mut cur)?;
    w.write(bitmap_block, checksum::BT::BITMAP)?;

    // Return the adjusted index entry
    Ok  (IndexEntry {
        blocknr: ie.blocknr,
        nr_free,
        none_free_before: first_free,
    })
}

pub fn write_metadata_sm(w: &mut WriteBatcher, sm: &dyn SpaceMap) -> Result<SMRoot> {
    w.clear_allocations();
    let (mut indexes, ref_count_root) = write_common(w, sm)?;

    let bitmap_root = w.alloc()?;

    // Now we need to patch up the counts for the metadata that was used for storing
    // the space map itself.  These ref counts all went from 0 to 1.
    let allocations = w.clear_allocations();

    // Sort the allocations by bitmap
    let mut by_bitmap = BTreeMap::new();
    for b in allocations {
        let bitmap = block_to_bitmap(b);
        (*by_bitmap.entry(bitmap).or_insert(Vec::new())).push(b % ENTRIES_PER_BITMAP as u64);
    }

    for (bitmap, allocs) in by_bitmap {
        indexes[bitmap] = adjust_counts(w, &indexes[bitmap], &allocs)?;
    }

    // Write out the metadata index
    let metadata_index = MetadataIndex {
        blocknr: bitmap_root.loc,
        indexes
    };
    let mut cur = Cursor::new(bitmap_root.get_data());
    metadata_index.pack(&mut cur)?;
    let loc = bitmap_root.loc;
    w.write(bitmap_root, checksum::BT::INDEX)?;

    Ok(SMRoot {
        nr_blocks: sm.get_nr_blocks()?,
        nr_allocated: sm.get_nr_allocated()?,
        bitmap_root: loc,
        ref_count_root,
    })
}

//--------------------------------
