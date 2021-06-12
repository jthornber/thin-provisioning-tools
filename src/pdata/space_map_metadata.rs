use anyhow::Result;
use byteorder::{LittleEndian, WriteBytesExt};
use nom::{number::complete::*, IResult};
use std::collections::BTreeMap;
use std::io::Cursor;

use crate::checksum;
use crate::io_engine::*;
use crate::pdata::space_map_common::*;
use crate::pdata::unpack::*;
use crate::write_batcher::*;

//------------------------------------------

const MAX_METADATA_BITMAPS: usize = 255;

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
        // FIXME: check the checksum
        let (i, _csum) = le_u32(i)?;
        let (i, _padding) = le_u32(i)?;
        let (i, blocknr) = le_u64(i)?;
        let (i, indexes) = nom::multi::count(IndexEntry::unpack, MAX_METADATA_BITMAPS)(i)?;

        // Filter out unused entries
        let indexes: Vec<IndexEntry> = indexes
            .iter()
            .take_while(|e| e.blocknr != 0)
            .cloned()
            .collect();

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

//------------------------------------------

fn block_to_bitmap(b: u64) -> usize {
    (b / ENTRIES_PER_BITMAP as u64) as usize
}

fn adjust_counts(w: &mut WriteBatcher, ie: &IndexEntry, allocs: &[u64]) -> Result<IndexEntry> {
    use BitmapEntry::*;

    let mut first_free = ie.none_free_before;
    let nr_free = ie.nr_free - allocs.len() as u32;

    // Read the bitmap
    let bitmap_block = w.engine.read(ie.blocknr)?;
    let (_, mut bitmap) = Bitmap::unpack(bitmap_block.get_data())?;

    // Update all the entries
    for a in allocs {
        if first_free == *a as u32 {
            first_free = *a as u32 + 1;
        }

        bitmap.entries[*a as usize] = Small(1);
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

pub fn write_metadata_sm(w: &mut WriteBatcher) -> Result<SMRoot> {
    w.clear_allocations();

    let (mut indexes, ref_count_root) = write_metadata_common(w)?;

    let bitmap_root = w.alloc_zeroed()?;

    // Now we need to patch up the counts for the metadata that was used for storing
    // the space map itself.  These ref counts all went from 0 to 1.
    let allocations = w.clear_allocations();

    // Sort the allocations by bitmap
    let mut by_bitmap = BTreeMap::new();
    for b in allocations {
        let bitmap = block_to_bitmap(b);
        (*by_bitmap.entry(bitmap).or_insert_with(Vec::new)).push(b % ENTRIES_PER_BITMAP as u64);
    }

    for (bitmap, allocs) in by_bitmap {
        indexes[bitmap] = adjust_counts(w, &indexes[bitmap], &allocs)?;
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
