use anyhow::{anyhow, Result};
use byteorder::{LittleEndian, WriteBytesExt};
use nom::{number::complete::*, IResult};
use std::io::{self, Cursor};
use std::sync::{Arc, Mutex};

use crate::checksum;
use crate::io_engine::*;
use crate::math::div_up;
use crate::pdata::space_map::common::*;
use crate::pdata::space_map::*;
use crate::pdata::unpack::*;
use crate::write_batcher::*;

//------------------------------------------

const MAX_METADATA_BITMAPS: usize = 255;
pub const MAX_METADATA_BLOCKS: usize = MAX_METADATA_BITMAPS * ENTRIES_PER_BITMAP;

//------------------------------------------

pub struct MetadataIndex {
    pub blocknr: u64,
    pub indexes: Vec<IndexEntry>,
}

impl Unpack for MetadataIndex {
    fn disk_size() -> u32 {
        BLOCK_SIZE as u32
    }

    fn unpack(i: &[u8]) -> IResult<&[u8], MetadataIndex> {
        let (i, _csum) = le_u32(i)?;
        let (i, _padding) = le_u32(i)?;
        let (i, blocknr) = le_u64(i)?;
        let (i, mut indexes) = nom::multi::count(IndexEntry::unpack, MAX_METADATA_BITMAPS)(i)?;

        // Drop unused entries that point to block 0
        let nr_bitmaps = indexes.iter().take_while(|e| e.blocknr != 0).count();
        indexes.truncate(nr_bitmaps);

        Ok((i, MetadataIndex { blocknr, indexes }))
    }
}

impl Pack for MetadataIndex {
    fn pack<W: WriteBytesExt>(&self, w: &mut W) -> io::Result<()> {
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

fn verify_checksum(b: &Block) -> Result<()> {
    match checksum::metadata_block_type(b.get_data()) {
        checksum::BT::INDEX => Ok(()),
        checksum::BT::UNKNOWN => Err(anyhow!("bad checksum in index block")),
        _ => Err(anyhow!("not an index block")),
    }
}

pub fn load_metadata_index(b: &Block, nr_blocks: u64) -> Result<MetadataIndex> {
    verify_checksum(b)?;
    let mut entries = unpack::<MetadataIndex>(b.get_data())?;
    if entries.blocknr != b.loc {
        return Err(anyhow!("blocknr mismatch"));
    }
    let nr_bitmaps = div_up(nr_blocks, ENTRIES_PER_BITMAP as u64) as usize;
    entries.indexes.truncate(nr_bitmaps);
    Ok(entries)
}

//------------------------------------------

pub fn block_to_bitmap(b: u64) -> usize {
    (b / ENTRIES_PER_BITMAP as u64) as usize
}

pub fn blocks_to_bitmaps(blocks: &[u64]) -> Vec<usize> {
    let mut bitmaps = Vec::new();

    let mut last = MAX_METADATA_BITMAPS;
    for b in blocks {
        let bitmap = block_to_bitmap(*b);
        assert!(bitmap < MAX_METADATA_BITMAPS);

        if bitmap != last {
            bitmaps.push(bitmap);
            last = bitmap;
        }
    }

    bitmaps
}

fn adjust_counts(
    w: &mut WriteBatcher,
    ie: &IndexEntry,
    begin: u64,
    end: u64,
) -> Result<IndexEntry> {
    use BitmapEntry::*;

    let mut first_free = ie.none_free_before;
    let nr_free = ie.nr_free - (end - begin) as u32;

    // Read the bitmap
    let bitmap_block = w.read(ie.blocknr)?;
    let (_, mut bitmap) = Bitmap::unpack(bitmap_block.get_data())?;

    // Update all the entries
    for a in begin..end {
        if first_free == a as u32 {
            first_free = a as u32 + 1;
        }

        bitmap.entries[a as usize] = Small(1);
    }

    // Write the bitmap
    let mut cur = Cursor::new(bitmap_block.get_data());
    bitmap.pack(&mut cur)?;
    w.write(bitmap_block, checksum::BT::BITMAP)?;

    // Return the adjusted index entry
    Ok(IndexEntry {
        blocknr: ie.blocknr,
        nr_free,
        none_free_before: first_free,
    })
}

//------------------------------------------

pub fn core_metadata_sm(nr_blocks: u64, max_count: u32) -> Arc<Mutex<dyn SpaceMap + Send + Sync>> {
    core_sm(
        std::cmp::min(nr_blocks, MAX_METADATA_BLOCKS as u64),
        max_count,
    )
}

pub fn write_metadata_sm(w: &mut WriteBatcher) -> Result<SMRoot> {
    let (mut indexes, ref_count_root) = write_metadata_common(w)?;
    let bitmap_root = w.alloc_zeroed()?;

    // Now we need to patch up the counts for the metadata that was used for storing
    // the space map itself.  These ref counts all went from 0 to 1.
    let allocations = w.clear_allocations();
    for range in allocations {
        let bi_begin = block_to_bitmap(range.start);
        let bi_end = block_to_bitmap(range.end) + 1;
        for (bm, ie) in indexes.iter_mut().enumerate().take(bi_end).skip(bi_begin) {
            let begin = if bm == bi_begin {
                range.start % ENTRIES_PER_BITMAP as u64
            } else {
                0
            };
            let end = if bm == bi_end - 1 {
                range.end % ENTRIES_PER_BITMAP as u64
            } else {
                ENTRIES_PER_BITMAP as u64
            };
            *ie = adjust_counts(w, ie, begin, end)?
        }
    }

    // Write out the metadata index
    let metadata_index = MetadataIndex {
        blocknr: bitmap_root.loc,
        indexes,
    };
    let mut cur = Cursor::new(bitmap_root.get_data());
    metadata_index.pack(&mut cur)?;
    let loc = bitmap_root.loc;
    w.write(bitmap_root, checksum::BT::INDEX)?;
    w.flush()?;

    let sm = w.sm.lock().unwrap();
    Ok(SMRoot {
        nr_blocks: sm.get_nr_blocks()?,
        nr_allocated: sm.get_nr_allocated()?,
        bitmap_root: loc,
        ref_count_root,
    })
}

//------------------------------------------
