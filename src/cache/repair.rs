use anyhow::Result;
use std::path::Path;
use std::sync::Arc;

use crate::cache::dump::*;
use crate::cache::restore::*;
use crate::cache::superblock::*;
use crate::io_engine::*;
use crate::pdata::space_map_metadata::*;
use crate::report::*;
use crate::write_batcher::*;

//------------------------------------------

pub struct CacheRepairOptions<'a> {
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

const MAX_CONCURRENT_IO: u32 = 1024;

fn new_context(opts: &CacheRepairOptions) -> Result<Context> {
    let engine_in: Arc<dyn IoEngine + Send + Sync>;
    let engine_out: Arc<dyn IoEngine + Send + Sync>;

    if opts.async_io {
        engine_in = Arc::new(AsyncIoEngine::new(opts.input, MAX_CONCURRENT_IO, false)?);
        engine_out = Arc::new(AsyncIoEngine::new(opts.output, MAX_CONCURRENT_IO, true)?);
    } else {
        let nr_threads = std::cmp::max(8, num_cpus::get() * 2);
        engine_in = Arc::new(SyncIoEngine::new(opts.input, nr_threads, false)?);
        engine_out = Arc::new(SyncIoEngine::new(opts.output, nr_threads, true)?);
    }

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
    let mut restorer = Restorer::new(&mut w);

    dump_metadata(ctx.engine_in, &mut restorer, &sb, true)
}

//------------------------------------------
