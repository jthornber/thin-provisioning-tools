use anyhow::Result;
use std::path::Path;
use std::sync::Arc;

use crate::era::dump::*;
use crate::era::restore::*;
use crate::era::superblock::*;
use crate::io_engine::*;
use crate::pdata::space_map::metadata::*;
use crate::report::*;
use crate::write_batcher::*;

//------------------------------------------

pub struct EraRepairOptions<'a> {
    pub input: &'a Path,
    pub output: &'a Path,
    pub async_io: bool,
    pub report: Arc<Report>,
}

struct Context {
    _report: Arc<Report>,
    engine_in: Arc<dyn IoEngine + Send + Sync>,
    engine_out: Arc<dyn IoEngine + Send + Sync>,
}

fn new_context(opts: &EraRepairOptions) -> Result<Context> {
    let engine_in: Arc<dyn IoEngine + Send + Sync>;
    let engine_out: Arc<dyn IoEngine + Send + Sync>;

    if opts.async_io {
        engine_in = Arc::new(AsyncIoEngine::new(opts.input, false)?);
        engine_out = Arc::new(AsyncIoEngine::new(opts.output, true)?);
    } else {
        engine_in = Arc::new(SyncIoEngine::new(opts.input, false)?);
        engine_out = Arc::new(SyncIoEngine::new(opts.output, true)?);
    }

    Ok(Context {
        _report: opts.report.clone(),
        engine_in,
        engine_out,
    })
}

//------------------------------------------

pub fn repair(opts: EraRepairOptions) -> Result<()> {
    let ctx = new_context(&opts)?;

    let sb = read_superblock(ctx.engine_in.as_ref(), SUPERBLOCK_LOCATION)?;

    let sm = core_metadata_sm(ctx.engine_out.get_nr_blocks(), u32::MAX);
    let batch_size = ctx.engine_out.get_batch_size();
    let mut w = WriteBatcher::new(ctx.engine_out, sm.clone(), batch_size);
    let mut restorer = Restorer::new(&mut w);

    dump_metadata(ctx.engine_in, &mut restorer, &sb, true)
}

//------------------------------------------
