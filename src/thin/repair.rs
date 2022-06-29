use anyhow::Result;
use std::path::Path;
use std::sync::Arc;

use crate::commands::engine::*;
use crate::io_engine::*;
use crate::pdata::space_map::metadata::*;
use crate::report::*;
use crate::thin::dump::*;
use crate::thin::metadata::*;
use crate::thin::metadata_repair::*;
use crate::thin::restore::*;
use crate::thin::superblock::*;
use crate::write_batcher::*;

//------------------------------------------

pub struct ThinRepairOptions<'a> {
    pub input: &'a Path,
    pub output: &'a Path,
    pub engine_opts: EngineOptions,
    pub report: Arc<Report>,
    pub overrides: SuperblockOverrides,
}

struct Context {
    report: Arc<Report>,
    engine_in: Arc<dyn IoEngine + Send + Sync>,
    engine_out: Arc<dyn IoEngine + Send + Sync>,
}

fn new_context(opts: &ThinRepairOptions) -> Result<Context> {
    let engine_in = EngineBuilder::new(opts.input, &opts.engine_opts).build()?;
    let engine_out = EngineBuilder::new(opts.output, &opts.engine_opts)
        .write(true)
        .build()?;

    Ok(Context {
        report: opts.report.clone(),
        engine_in,
        engine_out,
    })
}

//------------------------------------------

pub fn repair(opts: ThinRepairOptions) -> Result<()> {
    let ctx = new_context(&opts)?;

    let sb = read_or_rebuild_superblock(
        ctx.engine_in.clone(),
        ctx.report.clone(),
        SUPERBLOCK_LOCATION,
        &opts.overrides,
    )?;
    let md = build_metadata(ctx.engine_in.clone(), &sb)?;
    let md = optimise_metadata(md)?;

    let sm = core_metadata_sm(ctx.engine_out.get_nr_blocks(), u32::MAX);
    let batch_size = ctx.engine_out.get_batch_size();
    let mut w = WriteBatcher::new(ctx.engine_out, sm.clone(), batch_size);
    let mut restorer = Restorer::new(&mut w, ctx.report);

    dump_metadata(ctx.engine_in, &mut restorer, &sb, &md)
}

//------------------------------------------
