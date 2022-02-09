use anyhow::Result;

//------------------------------------------

#[derive(Clone)]
pub struct Superblock {
    pub uuid: String,
    pub time: u32,
    pub transaction: u64,
    pub flags: Option<u32>,
    pub version: Option<u32>,
    pub data_block_size: u32,
    pub nr_data_blocks: u64,
    pub metadata_snap: Option<u64>,
}

#[derive(Clone)]
pub struct Device {
    pub dev_id: u32,
    pub mapped_blocks: u64,
    pub transaction: u64,
    pub creation_time: u32,
    pub snap_time: u32,
}

#[derive(Clone)]
pub struct Map {
    pub thin_begin: u64,
    pub data_begin: u64,
    pub time: u32,
    pub len: u64,
}

//------------------------------------------

#[derive(Clone)]
pub enum Visit {
    Continue,
    Stop,
}

pub trait MetadataVisitor {
    fn superblock_b(&mut self, sb: &Superblock) -> Result<Visit>;
    fn superblock_e(&mut self) -> Result<Visit>;

    // Defines a shared sub tree.  May only contain a 'map' (no 'ref' allowed).
    fn def_shared_b(&mut self, name: &str) -> Result<Visit>;
    fn def_shared_e(&mut self) -> Result<Visit>;

    // A device contains a number of 'map' or 'ref' items.
    fn device_b(&mut self, d: &Device) -> Result<Visit>;
    fn device_e(&mut self) -> Result<Visit>;

    fn map(&mut self, m: &Map) -> Result<Visit>;
    fn ref_shared(&mut self, name: &str) -> Result<Visit>;

    fn eof(&mut self) -> Result<Visit>;
}

//------------------------------------------
