use anyhow::{anyhow, Result};
use byteorder::{LittleEndian, WriteBytesExt};
use nom::{bytes::complete::*, number::complete::*, IResult};
use std::io::Cursor;

use crate::checksum::*;
use crate::io_engine::*;

//------------------------------------------

pub const SPACE_MAP_ROOT_SIZE: usize = 128;
pub const SUPERBLOCK_LOCATION: u64 = 0;

const MAGIC: u64 = 0o6142003; // 0x18c403 in hex
const POLICY_NAME_SIZE: usize = 16;
const UUID_SIZE: usize = 16;

//------------------------------------------

#[derive(Debug, Clone)]
pub struct SuperblockFlags {
    pub clean_shutdown: bool,
    pub needs_check: bool,
}

#[derive(Debug, Clone)]
pub struct Superblock {
    pub flags: SuperblockFlags,
    pub block: u64,
    pub version: u32,

    pub policy_name: Vec<u8>,
    pub policy_version: Vec<u32>,
    pub policy_hint_size: u32,

    pub metadata_sm_root: Vec<u8>,
    pub mapping_root: u64,
    pub dirty_root: Option<u64>, // format 2 only
    pub hint_root: u64,

    pub discard_root: u64,
    pub discard_block_size: u64,
    pub discard_nr_blocks: u64,

    pub data_block_size: u32,
    pub cache_blocks: u32,

    pub compat_flags: u32,
    pub compat_ro_flags: u32,
    pub incompat_flags: u32,

    pub read_hits: u32,
    pub read_misses: u32,
    pub write_hits: u32,
    pub write_misses: u32,
}

fn unpack(data: &[u8]) -> IResult<&[u8], Superblock> {
    let (i, _csum) = le_u32(data)?;
    let (i, flags) = le_u32(i)?;
    let (i, block) = le_u64(i)?;
    let (i, _uuid) = take(16usize)(i)?;
    let (i, _magic) = le_u64(i)?;
    let (i, version) = le_u32(i)?;

    let (i, policy_name) = take(POLICY_NAME_SIZE)(i)?;
    let (i, policy_hint_size) = le_u32(i)?;

    let (i, metadata_sm_root) = take(SPACE_MAP_ROOT_SIZE)(i)?;
    let (i, mapping_root) = le_u64(i)?;
    let (i, hint_root) = le_u64(i)?;

    let (i, discard_root) = le_u64(i)?;
    let (i, discard_block_size) = le_u64(i)?;
    let (i, discard_nr_blocks) = le_u64(i)?;

    let (i, data_block_size) = le_u32(i)?;
    let (i, _metadata_block_size) = le_u32(i)?;
    let (i, cache_blocks) = le_u32(i)?;

    let (i, compat_flags) = le_u32(i)?;
    let (i, compat_ro_flags) = le_u32(i)?;
    let (i, incompat_flags) = le_u32(i)?;

    let (i, read_hits) = le_u32(i)?;
    let (i, read_misses) = le_u32(i)?;
    let (i, write_hits) = le_u32(i)?;
    let (i, write_misses) = le_u32(i)?;

    let (i, vsn_major) = le_u32(i)?;
    let (i, vsn_minor) = le_u32(i)?;
    let (i, vsn_patch) = le_u32(i)?;

    let (i, dirty_root) = if version >= 2 {
        let (m, root) = le_u64(i)?;
        (m, Some(root))
    } else {
        (i, None)
    };

    Ok((
        i,
        Superblock {
            flags: SuperblockFlags {
                clean_shutdown: (flags & 0x1) != 0,
                needs_check: (flags & 0x2) != 0,
            },
            block,
            version,
            policy_name: policy_name.splitn(2, |c| *c == 0).next().unwrap().to_vec(),
            policy_version: vec![vsn_major, vsn_minor, vsn_patch],
            policy_hint_size,
            metadata_sm_root: metadata_sm_root.to_vec(),
            mapping_root,
            dirty_root,
            hint_root,
            discard_root,
            discard_block_size,
            discard_nr_blocks,
            data_block_size,
            cache_blocks,
            compat_flags,
            compat_ro_flags,
            incompat_flags,
            read_hits,
            read_misses,
            write_hits,
            write_misses,
        },
    ))
}

pub fn read_superblock(engine: &dyn IoEngine, loc: u64) -> Result<Superblock> {
    let b = engine.read(loc)?;

    if metadata_block_type(b.get_data()) != BT::CACHE_SUPERBLOCK {
        return Err(anyhow!("bad checksum in superblock"));
    }

    if let Ok((_, sb)) = unpack(b.get_data()) {
        Ok(sb)
    } else {
        Err(anyhow!("couldn't unpack superblock"))
    }
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
    if sb.flags.needs_check {
        flags |= 0x2;
    }
    w.write_u32::<LittleEndian>(flags)?;

    w.write_u64::<LittleEndian>(sb.block)?;
    w.write_all(&[0; UUID_SIZE])?;
    w.write_u64::<LittleEndian>(MAGIC)?;
    w.write_u32::<LittleEndian>(sb.version)?;

    let mut policy_name = [0u8; POLICY_NAME_SIZE];
    policy_name[..sb.policy_name.len()].copy_from_slice(&sb.policy_name[..]);
    w.write_all(&policy_name)?;

    w.write_u32::<LittleEndian>(sb.policy_hint_size)?;
    w.write_all(&sb.metadata_sm_root)?;
    w.write_u64::<LittleEndian>(sb.mapping_root)?;
    w.write_u64::<LittleEndian>(sb.hint_root)?;

    w.write_u64::<LittleEndian>(sb.discard_root)?;
    w.write_u64::<LittleEndian>(sb.discard_block_size)?;
    w.write_u64::<LittleEndian>(sb.discard_nr_blocks)?;

    w.write_u32::<LittleEndian>(sb.data_block_size)?;
    // metadata block size
    w.write_u32::<LittleEndian>((BLOCK_SIZE >> SECTOR_SHIFT) as u32)?;
    w.write_u32::<LittleEndian>(sb.cache_blocks)?;

    w.write_u32::<LittleEndian>(sb.compat_flags)?;
    w.write_u32::<LittleEndian>(sb.compat_ro_flags)?;
    w.write_u32::<LittleEndian>(sb.incompat_flags)?;

    w.write_u32::<LittleEndian>(sb.read_hits)?;
    w.write_u32::<LittleEndian>(sb.read_misses)?;
    w.write_u32::<LittleEndian>(sb.write_hits)?;
    w.write_u32::<LittleEndian>(sb.write_misses)?;

    w.write_u32::<LittleEndian>(sb.policy_version[0])?;
    w.write_u32::<LittleEndian>(sb.policy_version[1])?;
    w.write_u32::<LittleEndian>(sb.policy_version[2])?;

    if sb.dirty_root.is_some() {
        w.write_u64::<LittleEndian>(sb.dirty_root.unwrap())?;
    }

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
    write_checksum(b.get_data(), BT::CACHE_SUPERBLOCK)?;

    // write
    engine.write(&b)?;
    Ok(())
}

//------------------------------------------
