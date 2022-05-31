use anyhow::{anyhow, Result};
use byteorder::{LittleEndian, WriteBytesExt};
use nom::{bytes::complete::*, number::complete::*, IResult};
use std::io::Cursor;

use crate::checksum::*;
use crate::era::writeset::Writeset;
use crate::io_engine::*;

//------------------------------------------

pub const SPACE_MAP_ROOT_SIZE: usize = 128;
pub const SUPERBLOCK_LOCATION: u64 = 0;

const MAGIC: u64 = 0o17660203573; // 0x7EC1077B in hex
const UUID_SIZE: usize = 16;

//------------------------------------------

#[derive(Debug, Clone)]
pub struct SuperblockFlags {
    pub clean_shutdown: bool,
}

#[derive(Debug, Clone)]
pub struct Superblock {
    pub flags: SuperblockFlags,
    pub block: u64,
    pub version: u32,

    pub metadata_sm_root: Vec<u8>,

    pub data_block_size: u32,
    pub nr_blocks: u32,

    pub current_era: u32,
    pub current_writeset: Writeset,

    pub writeset_tree_root: u64,
    pub era_array_root: u64,

    pub metadata_snap: u64,
}

fn unpack(data: &[u8]) -> IResult<&[u8], Superblock> {
    let (i, _csum) = le_u32(data)?;
    let (i, flags) = le_u32(i)?;
    let (i, block) = le_u64(i)?;
    let (i, _uuid) = take(16usize)(i)?;
    let (i, _magic) = le_u64(i)?;
    let (i, version) = le_u32(i)?;

    let (i, metadata_sm_root) = take(SPACE_MAP_ROOT_SIZE)(i)?;
    let (i, data_block_size) = le_u32(i)?;
    let (i, _metadata_block_size) = le_u32(i)?;
    let (i, nr_blocks) = le_u32(i)?;

    let (i, current_era) = le_u32(i)?;
    let (i, nr_bits) = le_u32(i)?;
    let (i, root) = le_u64(i)?;

    let (i, writeset_tree_root) = le_u64(i)?;
    let (i, era_array_root) = le_u64(i)?;
    let (i, metadata_snap) = le_u64(i)?;

    Ok((
        i,
        Superblock {
            flags: SuperblockFlags {
                clean_shutdown: (flags & 0x1) != 0,
            },
            block,
            version,
            metadata_sm_root: metadata_sm_root.to_vec(),
            data_block_size,
            nr_blocks,
            current_era,
            current_writeset: Writeset { nr_bits, root },
            writeset_tree_root,
            era_array_root,
            metadata_snap,
        },
    ))
}

pub fn read_superblock(engine: &dyn IoEngine, loc: u64) -> Result<Superblock> {
    let b = engine.read(loc)?;

    if metadata_block_type(b.get_data()) != BT::ERA_SUPERBLOCK {
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

//------------------------------------------

fn pack_superblock<W: WriteBytesExt>(sb: &Superblock, w: &mut W) -> Result<()> {
    // checksum, which we don't know yet
    w.write_u32::<LittleEndian>(0)?;

    // flags
    let mut flags: u32 = 0;
    if sb.flags.clean_shutdown {
        flags |= 0x1;
    }
    w.write_u32::<LittleEndian>(flags)?;
    w.write_u64::<LittleEndian>(sb.block)?;

    w.write_all(&[0; UUID_SIZE])?;
    w.write_u64::<LittleEndian>(MAGIC)?;
    w.write_u32::<LittleEndian>(sb.version)?;

    w.write_all(&sb.metadata_sm_root)?;
    w.write_u32::<LittleEndian>(sb.data_block_size)?;
    // metadata block size
    w.write_u32::<LittleEndian>((BLOCK_SIZE >> SECTOR_SHIFT) as u32)?;
    w.write_u32::<LittleEndian>(sb.nr_blocks)?;

    w.write_u32::<LittleEndian>(sb.current_era)?;
    w.write_u32::<LittleEndian>(sb.current_writeset.nr_bits)?;
    w.write_u64::<LittleEndian>(sb.current_writeset.root)?;

    w.write_u64::<LittleEndian>(sb.writeset_tree_root)?;
    w.write_u64::<LittleEndian>(sb.era_array_root)?;

    w.write_u64::<LittleEndian>(sb.metadata_snap)?;

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
    write_checksum(b.get_data(), BT::ERA_SUPERBLOCK)?;

    // write
    engine.write(&b)?;
    Ok(())
}

//------------------------------------------
