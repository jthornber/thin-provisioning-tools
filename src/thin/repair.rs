use anyhow::Result;
use std::path::Path;
use std::sync::Arc;

use crate::io_engine::*;
use crate::pdata::space_map::*;
use crate::report::*;
use crate::thin::dump::*;
use crate::thin::metadata::*;
use crate::thin::restore::*;
use crate::thin::superblock::*;
use crate::write_batcher::*;

//------------------------------------------

pub struct ThinRepairOptions<'a> {
    pub input: &'a Path,
    pub output: &'a Path,
    pub async_io: bool,
    pub report: Arc<Report>,
}

struct Context {
    report: Arc<Report>,
    engine_in: Arc<dyn IoEngine + Send + Sync>,
    engine_out: Arc<dyn IoEngine + Send + Sync>,
}

const MAX_CONCURRENT_IO: u32 = 1024;

fn new_context(opts: &ThinRepairOptions) -> Result<Context> {
    let engine_in: Arc<dyn IoEngine + Send + Sync>;
    let engine_out: Arc<dyn IoEngine + Send + Sync>;

    if opts.async_io {
        engine_in = Arc::new(AsyncIoEngine::new(opts.input, MAX_CONCURRENT_IO, true)?);
        engine_out = Arc::new(AsyncIoEngine::new(opts.output, MAX_CONCURRENT_IO, true)?);
    } else {
        let nr_threads = std::cmp::max(8, num_cpus::get() * 2);
        engine_in = Arc::new(SyncIoEngine::new(opts.input, nr_threads, true)?);
        engine_out = Arc::new(SyncIoEngine::new(opts.output, nr_threads, true)?);
    }

    Ok(Context {
        report: opts.report.clone(),
        engine_in,
        engine_out,
    })
}

//------------------------------------------

pub fn repair(opts: ThinRepairOptions) -> Result<()> {
    let ctx = new_context(&opts)?;

    let sb = read_superblock(ctx.engine_in.as_ref(), SUPERBLOCK_LOCATION)?;
    let md = build_metadata(ctx.engine_in.clone(), &sb)?;
    let md = optimise_metadata(md)?;

    let sm = core_sm(ctx.engine_out.get_nr_blocks(), u32::MAX);
    let mut w = WriteBatcher::new(
        ctx.engine_out.clone(),
        sm.clone(),
        ctx.engine_out.get_batch_size(),
    );
    let mut restorer = Restorer::new(&mut w, ctx.report);

    dump_metadata(ctx.engine_in, &mut restorer, &sb, &md)
}

//------------------------------------------
