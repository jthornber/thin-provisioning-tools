use anyhow::{anyhow, Result};
use nom::{bytes::complete::*, number::complete::*, IResult};

use crate::io_engine::*;

//------------------------------------------

pub const SUPERBLOCK_LOCATION: u64 = 0;

const POLICY_NAME_SIZE: usize = 16;
const SPACE_MAP_ROOT_SIZE: usize = 128;

//------------------------------------------

#[derive(Debug, Clone)]
pub struct SuperblockFlags {
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
    pub metadata_block_size: u32,
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
    let (i, metadata_block_size) = le_u32(i)?;
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

    let mut i = i;
    let mut dirty_root = None;
    if version >= 2 {
        let (m, root) = le_u64(i)?;
        dirty_root = Some(root);
        i = &m;
    }

    Ok((
        i,
        Superblock {
            flags: SuperblockFlags {
                needs_check: (flags & 0x1) != 0,
            },
            block,
            version,
            policy_name: policy_name.to_vec(),
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
            metadata_block_size,
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

    if let Ok((_, sb)) = unpack(&b.get_data()) {
        Ok(sb)
    } else {
        Err(anyhow!("couldn't unpack superblock"))
    }
}

//------------------------------------------
