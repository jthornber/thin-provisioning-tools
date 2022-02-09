use anyhow::Result;

//------------------------------------------

#[derive(Clone)]
pub struct Superblock {
    pub uuid: String,
    pub block_size: u32,
    pub nr_blocks: u32,
    pub current_era: u32,
}

#[derive(Clone)]
pub struct Writeset {
    pub era: u32,
    pub nr_bits: u32,
}

#[derive(Clone)]
pub struct MarkedBlocks {
    pub begin: u32,
    pub len: u32,
}

#[derive(Clone)]
pub struct Era {
    pub block: u32,
    pub era: u32,
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

    fn writeset_b(&mut self, ws: &Writeset) -> Result<Visit>;
    fn writeset_e(&mut self) -> Result<Visit>;
    fn writeset_blocks(&mut self, blocks: &MarkedBlocks) -> Result<Visit>;

    fn era_b(&mut self) -> Result<Visit>;
    fn era_e(&mut self) -> Result<Visit>;
    fn era(&mut self, era: &Era) -> Result<Visit>;

    fn eof(&mut self) -> Result<Visit>;
}

//------------------------------------------
