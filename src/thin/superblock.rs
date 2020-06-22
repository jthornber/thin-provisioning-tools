use anyhow::Result;
use crate::block_manager::*;
use crate::checksum::*;

const SPACE_MAP_ROOT_SIZE: usize = 128;

pub struct Superblock {
    block: u64,
    uuid: String,
    version: u32,
    time: u32,
    transaction_id: u64,
    metadata_snap: u64,
    data_sm_root: [u8; SPACE_MAP_ROOT_SIZE],
    metadata_sn_root: [u8; SPACE_MAP_ROOT_SIZE],
    mapping_root: u64,
    details_root: u64,
    data_block_size: u32,
}

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

use SuperblockDamage::*;

//------------------------------

pub fn check_type(b: &Block) -> Result<()> {
    match metadata_block_type(&b.data[0..]) {
        SUPERBLOCK => Ok(()),
        NODE => Err(Box::new(BadBlockType("BTree Node"))),
        INDEX => Err(Box::new(BadBlockType("Space Map Index"))),
        BITMAP => Err(Box::new(BadBlockType("Space Map Bitmap"))),
        UNKNOWN => Err(Box::new(BadChecksum)),
    }
}

//------------------------------
