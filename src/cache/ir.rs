use anyhow::Result;

//------------------------------------------

#[derive(Clone)]
pub struct Superblock {
    pub uuid: String,
    pub block_size: u32,
    pub nr_cache_blocks: u32,
    pub policy: String,
    pub hint_width: u32,
}

#[derive(Clone)]
pub struct Map {
    pub cblock: u32,
    pub oblock: u64,
    pub dirty: bool,
}

#[derive(Clone)]
pub struct Hint {
    pub cblock: u32,
    pub data: Vec<u8>,
}

#[derive(Clone)]
pub struct Discard {
    pub begin: u64,
    pub end: u64,
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

    fn mappings_b(&mut self) -> Result<Visit>;
    fn mappings_e(&mut self) -> Result<Visit>;
    fn mapping(&mut self, m: &Map) -> Result<Visit>;

    fn hints_b(&mut self) -> Result<Visit>;
    fn hints_e(&mut self) -> Result<Visit>;
    fn hint(&mut self, h: &Hint) -> Result<Visit>;

    fn discards_b(&mut self) -> Result<Visit>;
    fn discards_e(&mut self) -> Result<Visit>;
    fn discard(&mut self, d: &Discard) -> Result<Visit>;

    fn eof(&mut self) -> Result<Visit>;
}

//------------------------------------------
