use anyhow::{anyhow, Result};
use byteorder::{LittleEndian, WriteBytesExt};
use nom::{bytes::complete::*, number::complete::*, IResult};
use std::fmt;
use std::io::Cursor;

use crate::checksum::*;
use crate::io_engine::*;

//----------------------------------------

pub const MAGIC: u64 = 27022010;
pub const SUPERBLOCK_LOCATION: u64 = 0;
const UUID_SIZE: usize = 16;
pub const SPACE_MAP_ROOT_SIZE: usize = 128;

#[derive(Debug, Clone)]
pub struct SuperblockFlags {
    pub needs_check: bool,
}

impl fmt::Display for SuperblockFlags {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.needs_check {
            write!(f, "NEEDS_CHECK")
        } else {
            write!(f, "-")
        }
    }
}

#[derive(Debug, Clone)]
pub struct Superblock {
    pub flags: SuperblockFlags,
    pub block: u64,
    //uuid: [u8; UUID_SIZE],
    pub version: u32,
    pub time: u32,
    pub transaction_id: u64,
    pub metadata_snap: u64,
    pub data_sm_root: Vec<u8>,
    pub metadata_sm_root: Vec<u8>,
    pub mapping_root: u64,
    pub details_root: u64,
    pub data_block_size: u32,
    pub nr_metadata_blocks: u64,
}

fn unpack(data: &[u8]) -> IResult<&[u8], Superblock> {
    let (i, _csum) = le_u32(data)?;
    let (i, flags) = le_u32(i)?;
    let (i, block) = le_u64(i)?;
    let (i, _uuid) = take(16usize)(i)?;
    let (i, _magic) = le_u64(i)?;
    let (i, version) = le_u32(i)?;
    let (i, time) = le_u32(i)?;
    let (i, transaction_id) = le_u64(i)?;
    let (i, metadata_snap) = le_u64(i)?;
    let (i, data_sm_root) = take(SPACE_MAP_ROOT_SIZE)(i)?;
    let (i, metadata_sm_root) = take(SPACE_MAP_ROOT_SIZE)(i)?;
    let (i, mapping_root) = le_u64(i)?;
    let (i, details_root) = le_u64(i)?;
    let (i, data_block_size) = le_u32(i)?;
    let (i, _metadata_block_size) = le_u32(i)?;
    let (i, nr_metadata_blocks) = le_u64(i)?;

    Ok((
        i,
        Superblock {
            flags: SuperblockFlags {
                needs_check: (flags & 0x1) != 0,
            },
            block,
            //uuid: uuid[0..UUID_SIZE],
            version,
            time,
            transaction_id,
            metadata_snap,
            data_sm_root: data_sm_root.to_vec(),
            metadata_sm_root: metadata_sm_root.to_vec(),
            mapping_root,
            details_root,
            data_block_size,
            nr_metadata_blocks,
        },
    ))
}

pub fn read_superblock(engine: &dyn IoEngine, loc: u64) -> Result<Superblock> {
    let b = engine.read(loc)?;

    if metadata_block_type(b.get_data()) != BT::THIN_SUPERBLOCK {
        return Err(anyhow!("bad checksum in superblock"));
    }

    if let Ok((_, sb)) = unpack(b.get_data()) {
        Ok(sb)
    } else {
        Err(anyhow!("couldn't unpack superblock"))
    }
}

pub fn read_superblock_snap(engine: &dyn IoEngine) -> Result<Superblock> {
    let actual_sb = read_superblock(engine, SUPERBLOCK_LOCATION)?;
    if actual_sb.metadata_snap == 0 {
        return Err(anyhow!("no current metadata snap"));
    }
    read_superblock(engine, actual_sb.metadata_snap)
}

//------------------------------

fn pack_superblock<W: WriteBytesExt>(sb: &Superblock, w: &mut W) -> Result<()> {
    // checksum, which we don't know yet
    w.write_u32::<LittleEndian>(0)?;

    // flags
    if sb.flags.needs_check {
        w.write_u32::<LittleEndian>(0x1)?;
    } else {
        w.write_u32::<LittleEndian>(0)?;
    }

    w.write_u64::<LittleEndian>(sb.block)?;
    w.write_all(&[0; UUID_SIZE])?;
    w.write_u64::<LittleEndian>(MAGIC)?;
    w.write_u32::<LittleEndian>(sb.version)?;
    w.write_u32::<LittleEndian>(sb.time)?;
    w.write_u64::<LittleEndian>(sb.transaction_id)?;
    w.write_u64::<LittleEndian>(sb.metadata_snap)?;
    w.write_all(&sb.data_sm_root)?;
    w.write_all(&sb.metadata_sm_root)?;
    w.write_u64::<LittleEndian>(sb.mapping_root)?;
    w.write_u64::<LittleEndian>(sb.details_root)?;
    w.write_u32::<LittleEndian>(sb.data_block_size)?;
    w.write_u32::<LittleEndian>((BLOCK_SIZE >> SECTOR_SHIFT) as u32)?; // metadata block size
    w.write_u64::<LittleEndian>(sb.nr_metadata_blocks)?;

    Ok(())
}

pub fn write_superblock(engine: &dyn IoEngine, _loc: u64, sb: &Superblock) -> Result<()> {
    let b = Block::zeroed(SUPERBLOCK_LOCATION);

    // pack the superblock
    {
        let mut cursor = Cursor::new(b.get_data());
        pack_superblock(sb, &mut cursor)?;
    }

    // calculate the checksum
    write_checksum(b.get_data(), BT::THIN_SUPERBLOCK)?;

    // write
    engine.write(&b)?;
    Ok(())
}

//------------------------------
