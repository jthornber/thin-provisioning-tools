use byteorder::{LittleEndian, WriteBytesExt};
use nom::{number::complete::*, IResult};
use std::io::{self, Cursor};

use crate::checksum;
use crate::io_engine::*;
use crate::math::*;
use crate::pdata::btree_builder::*;
use crate::pdata::space_map::*;
use crate::pdata::unpack::*;
use crate::write_batcher::*;

//------------------------------------------

pub const ENTRIES_PER_BITMAP: usize = WORDS_PER_BITMAP * 8 * ENTRIES_PER_BYTE;
const WORDS_PER_BITMAP: usize = (BLOCK_SIZE - 16) / 8;
const ENTRIES_PER_BYTE: usize = 4;

//------------------------------------------

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
    fn pack<W: WriteBytesExt>(&self, w: &mut W) -> io::Result<()> {
        w.write_u64::<LittleEndian>(self.blocknr)?;
        w.write_u32::<LittleEndian>(self.nr_free)?;
        w.write_u32::<LittleEndian>(self.none_free_before)
    }
}

//------------------------------------------

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
    fn pack<W: WriteBytesExt>(&self, out: &mut W) -> io::Result<()> {
        use BitmapEntry::*;

        out.write_u32::<LittleEndian>(0)?; // csum
        out.write_u32::<LittleEndian>(0)?; // padding
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
                        return Err(io::Error::from(io::ErrorKind::InvalidData));
                    }
                    Overflow => {
                        w |= 0x3 << 62;
                    }
                }
            }
            w >>= 64 - chunk.len() * 2;

            u64::pack(&w, out)?;
        }

        Ok(())
    }
}

//------------------------------------------

#[derive(Clone, Debug)]
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

impl Pack for SMRoot {
    fn pack<W: WriteBytesExt>(&self, w: &mut W) -> io::Result<()> {
        w.write_u64::<LittleEndian>(self.nr_blocks)?;
        w.write_u64::<LittleEndian>(self.nr_allocated)?;
        w.write_u64::<LittleEndian>(self.bitmap_root)?;
        w.write_u64::<LittleEndian>(self.ref_count_root)
    }
}

pub fn pack_root(root: &SMRoot, size: usize) -> anyhow::Result<Vec<u8>> {
    let mut sm_root = vec![0u8; size];
    let mut cur = Cursor::new(&mut sm_root);
    root.pack(&mut cur)?;
    Ok(sm_root)
}

//------------------------------------------

pub fn write_common(
    w: &mut WriteBatcher,
    sm: &dyn SpaceMap,
) -> anyhow::Result<(Vec<IndexEntry>, u64)> {
    use BitmapEntry::*;

    let mut index_entries = Vec::new();
    let mut overflow_builder: BTreeBuilder<u32> = BTreeBuilder::new(Box::new(NoopRC {}));

    // how many bitmaps do we need?
    let nr_blocks = sm.get_nr_blocks()?;
    let nr_bitmaps = div_up(nr_blocks, ENTRIES_PER_BITMAP as u64) as usize;

    for bm in 0..nr_bitmaps {
        let begin = bm as u64 * ENTRIES_PER_BITMAP as u64;
        let len = std::cmp::min(nr_blocks - begin, ENTRIES_PER_BITMAP as u64);
        let mut entries = Vec::with_capacity(ENTRIES_PER_BITMAP);
        let mut first_free: Option<u32> = None;
        let mut nr_free = ENTRIES_PER_BITMAP as u32; // do not truncate to the sm size boundary

        for i in 0..len {
            let b = begin + i;
            let rc = sm.get(b)?;
            let e = match rc {
                0 => {
                    if first_free.is_none() {
                        first_free = Some(i as u32);
                    }
                    Small(0)
                }
                1 => {
                    nr_free -= 1;
                    Small(1)
                }
                2 => {
                    nr_free -= 1;
                    Small(2)
                }
                _ => {
                    nr_free -= 1;
                    overflow_builder.push_value(w, b, rc)?;
                    Overflow
                }
            };
            entries.push(e);
        }

        let blocknr = write_bitmap(w, entries)?;

        // Insert into the index list
        let ie = IndexEntry {
            blocknr,
            nr_free,
            none_free_before: first_free.unwrap_or(len as u32),
        };
        index_entries.push(ie);
    }

    let ref_count_root = overflow_builder.complete(w)?;
    Ok((index_entries, ref_count_root))
}

pub fn write_metadata_common(w: &mut WriteBatcher) -> anyhow::Result<(Vec<IndexEntry>, u64)> {
    use BitmapEntry::*;

    let mut index_entries = Vec::new();
    let mut overflow_builder: BTreeBuilder<u32> = BTreeBuilder::new(Box::new(NoopRC {}));

    let mut bm = 0;
    let mut nr_free = ENTRIES_PER_BITMAP as u32;
    let mut none_free_before: u32 = 0;
    let mut entries = Vec::with_capacity(ENTRIES_PER_BITMAP);
    entries.resize(ENTRIES_PER_BITMAP, Small(0));

    // Write the bitmaps in corresponding to the allocations
    let allocations = w.clear_allocations();
    for range in allocations.iter() {
        for b in range.start..range.end {
            if b / ENTRIES_PER_BITMAP as u64 != bm {
                // write out bitmap
                let mut out = Vec::new();
                std::mem::swap(&mut out, &mut entries);
                let blocknr = write_bitmap(w, out)?;

                // Insert into the index list
                let ie = IndexEntry {
                    blocknr,
                    nr_free,
                    none_free_before,
                };
                index_entries.push(ie);

                // reset buffers
                bm = b / ENTRIES_PER_BITMAP as u64;
                nr_free = ENTRIES_PER_BITMAP as u32;
                none_free_before = 0;
                entries.resize(ENTRIES_PER_BITMAP, Small(0));
            }

            let rc = w.sm.lock().unwrap().get(b)?;
            let i = (b % ENTRIES_PER_BITMAP as u64) as u32;
            let e = match rc {
                0 => Small(0),
                1 => {
                    nr_free -= 1;
                    if i == none_free_before {
                        none_free_before = i + 1;
                    }
                    Small(1)
                }
                2 => {
                    nr_free -= 1;
                    if i == none_free_before {
                        none_free_before = i + 1;
                    }
                    Small(2)
                }
                _ => {
                    nr_free -= 1;
                    if i == none_free_before {
                        none_free_before = i + 1;
                    }
                    overflow_builder.push_value(w, b, rc)?;
                    Overflow
                }
            };
            entries[i as usize] = e;
        }
    }

    // Write out the last non-empty bitmap
    let mut out = Vec::new();
    std::mem::swap(&mut out, &mut entries);
    let blocknr = write_bitmap(w, out)?;

    // Insert into the index list
    let ie = IndexEntry {
        blocknr,
        nr_free,
        none_free_before,
    };
    index_entries.push(ie);

    // Fill the rest of the bitmaps with zeros
    let nr_blocks = w.sm.lock().unwrap().get_nr_blocks()?;
    let nr_bitmaps = div_up(nr_blocks, ENTRIES_PER_BITMAP as u64) as usize;
    let nr_used_bitmaps = index_entries.len();
    for _bm in nr_used_bitmaps..nr_bitmaps {
        let entries = vec![BitmapEntry::Small(0); ENTRIES_PER_BITMAP];
        let blocknr = write_bitmap(w, entries)?;

        // Insert into the index list
        let ie = IndexEntry {
            blocknr,
            nr_free: ENTRIES_PER_BITMAP as u32, // do not truncate to the sm size boundary
            none_free_before: 0,
        };
        index_entries.push(ie);
    }

    let ref_count_root = overflow_builder.complete(w)?;
    Ok((index_entries, ref_count_root))
}

fn write_bitmap(w: &mut WriteBatcher, entries: Vec<BitmapEntry>) -> anyhow::Result<u64> {
    // allocate a new block
    let b = w.alloc_zeroed()?;
    let mut cursor = Cursor::new(b.get_data());

    // write the bitmap to it
    let blocknr = b.loc;
    let bitmap = Bitmap { blocknr, entries };
    bitmap.pack(&mut cursor)?;
    w.write(b, checksum::BT::BITMAP)?;

    Ok(blocknr)
}

//------------------------------------------
