use crate::io_engine::*;
use anyhow::{anyhow, Result};
use nom::{bytes::complete::*, number::complete::*, IResult};

pub const SUPERBLOCK_LOCATION: u64 = 0;
//const UUID_SIZE: usize = 16;
const SPACE_MAP_ROOT_SIZE: usize = 128;

#[derive(Debug)]
pub struct SuperblockFlags {
    pub needs_check: bool,
}

#[derive(Debug)]
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
}

/*
pub enum CheckSeverity {
    Fatal,
    NonFatal,
}

pub trait CheckError {
    fn severity(&self) -> CheckSeverity;
    fn block(&self) -> u64;
    fn sub_errors(&self) -> Vec<Box<dyn CheckError>>;
}

enum ErrorType {
    BadChecksum,
    BadBlockType(&'static str),
    BadBlock(u64),
    BadVersion(u32),
    MetadataSnapOutOfBounds(u64),
    MappingRootOutOfBounds(u64),
    DetailsRootOutOfBounds(u64),
}

struct SuperblockError {
    severity: CheckSeverity,
    kind: ErrorType,
}
*/

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
    let (i, _metadata_nr_blocks) = le_u64(i)?;

    Ok((
        i,
        Superblock {
            flags: SuperblockFlags {needs_check: (flags & 0x1) != 0},
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

//------------------------------
