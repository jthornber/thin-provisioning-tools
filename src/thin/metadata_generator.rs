use anyhow::Result;
use std::path::Path;
use std::sync::Arc;

use crate::commands::engine::*;
use crate::io_engine::*;
use crate::pdata::space_map::metadata::core_metadata_sm;
use crate::report::mk_quiet_report;
use crate::thin::ir::MetadataVisitor;
use crate::thin::restore::Restorer;
use crate::write_batcher::WriteBatcher;

//------------------------------------------

pub trait MetadataGenerator {
    fn generate_metadata(&self, v: &mut dyn MetadataVisitor) -> Result<()>;
}

struct ThinGenerator;

impl MetadataGenerator for ThinGenerator {
    fn generate_metadata(&self, _v: &mut dyn MetadataVisitor) -> Result<()> {
        Ok(()) // TODO
    }
}

//------------------------------------------

fn format(engine: Arc<dyn IoEngine + Send + Sync>, gen: ThinGenerator) -> Result<()> {
    let sm = core_metadata_sm(engine.get_nr_blocks(), u32::MAX);
    let batch_size = engine.get_batch_size();
    let mut w = WriteBatcher::new(engine, sm, batch_size);
    let mut restorer = Restorer::new(&mut w, Arc::new(mk_quiet_report()));

    gen.generate_metadata(&mut restorer)
}

fn set_needs_check(engine: Arc<dyn IoEngine + Send + Sync>, flag: bool) -> Result<()> {
    use crate::thin::superblock::*;

    let mut sb = read_superblock(engine.as_ref(), SUPERBLOCK_LOCATION)?;
    sb.flags.needs_check = flag;
    write_superblock(engine.as_ref(), SUPERBLOCK_LOCATION, &sb)
}

//------------------------------------------

pub struct ThinFormatOpts {
    pub data_block_size: u32,
    pub nr_data_blocks: u64,
}

pub enum MetadataOp {
    Format(ThinFormatOpts),
    SetNeedsCheck(bool),
}

pub struct ThinGenerateOpts<'a> {
    pub engine_opts: EngineOptions,
    pub op: MetadataOp,
    pub output: &'a Path,
}

pub fn generate_metadata(opts: ThinGenerateOpts) -> Result<()> {
    let engine = EngineBuilder::new(opts.output, &opts.engine_opts)
        .write(true)
        .build()?;
    match opts.op {
        // FIXME: parameterize ThinGenerator
        MetadataOp::Format(_op) => format(engine, ThinGenerator),
        MetadataOp::SetNeedsCheck(flag) => set_needs_check(engine, flag),
    }
}

//------------------------------------------
