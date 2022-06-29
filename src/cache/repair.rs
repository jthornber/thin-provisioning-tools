use anyhow::Result;
use std::path::Path;
use std::sync::Arc;

use crate::cache::dump::*;
use crate::cache::restore::*;
use crate::cache::superblock::*;
use crate::commands::engine::*;
use crate::io_engine::*;
use crate::pdata::space_map::metadata::*;
use crate::report::*;
use crate::write_batcher::*;

//------------------------------------------

pub struct CacheRepairOptions<'a> {
    pub input: &'a Path,
    pub output: &'a Path,
    pub engine_opts: EngineOptions,
    pub report: Arc<Report>,
}

struct Context {
    _report: Arc<Report>,
    engine_in: Arc<dyn IoEngine + Send + Sync>,
    engine_out: Arc<dyn IoEngine + Send + Sync>,
}

fn new_context(opts: &CacheRepairOptions) -> Result<Context> {
    let engine_in = EngineBuilder::new(opts.input, &opts.engine_opts).build()?;
    let engine_out = EngineBuilder::new(opts.output, &opts.engine_opts)
        .write(true)
        .build()?;

    Ok(Context {
        _report: opts.report.clone(),
        engine_in,
        engine_out,
    })
}

//------------------------------------------

pub fn repair(opts: CacheRepairOptions) -> Result<()> {
    let ctx = new_context(&opts)?;

    let sb = read_superblock(ctx.engine_in.as_ref(), SUPERBLOCK_LOCATION)?;

    let sm = core_metadata_sm(ctx.engine_out.get_nr_blocks(), u32::MAX);
    let batch_size = ctx.engine_out.get_batch_size();
    let mut w = WriteBatcher::new(ctx.engine_out, sm.clone(), batch_size);
    let mut restorer = Restorer::new(&mut w, sb.version as u8);

    dump_metadata(ctx.engine_in, &mut restorer, &sb, true)
}

//------------------------------------------
