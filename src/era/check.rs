use anyhow::{anyhow, Result};
use std::path::Path;
use std::sync::Arc;

use crate::commands::engine::*;
use crate::era::superblock::*;
use crate::era::writeset::*;
use crate::io_engine::*;
use crate::pdata::array::{self, ArrayBlock, ArrayError};
use crate::pdata::array_walker::*;
use crate::pdata::bitset::*;
use crate::pdata::btree_walker::*;
use crate::pdata::space_map::*;
use crate::report::*;

//------------------------------------------

fn inc_superblock(sm: &ASpaceMap) -> anyhow::Result<()> {
    let mut sm = sm.lock().unwrap();
    sm.inc(SUPERBLOCK_LOCATION, 1)?;
    Ok(())
}

//------------------------------------------

struct EraChecker {
    current_era: u32,
}

impl EraChecker {
    pub fn new(current_era: u32) -> EraChecker {
        EraChecker { current_era }
    }
}

impl ArrayVisitor<u32> for EraChecker {
    fn visit(&self, index: u64, b: ArrayBlock<u32>) -> array::Result<()> {
        let mut errs: Vec<ArrayError> = Vec::new();

        let dbegin = index as u32 * b.header.max_entries;
        let dend = dbegin + b.header.max_entries;
        for (era, dblock) in b.values.iter().zip(dbegin..dend) {
            if era > &self.current_era {
                errs.push(array::value_err(format!(
                    "invalid era value at data block {}: {}",
                    dblock, era
                )));
            }
        }

        match errs.len() {
            0 => Ok(()),
            1 => Err(errs[0].clone()),
            _ => Err(array::aggregate_error(errs)),
        }
    }
}

//------------------------------------------

pub struct EraCheckOptions<'a> {
    pub dev: &'a Path,
    pub engine_opts: EngineOptions,
    pub sb_only: bool,
    pub ignore_non_fatal: bool,
    pub report: Arc<Report>,
}

struct Context {
    report: Arc<Report>,
    engine: Arc<dyn IoEngine + Send + Sync>,
}

fn mk_context(opts: &EraCheckOptions) -> anyhow::Result<Context> {
    let engine = EngineBuilder::new(opts.dev, &opts.engine_opts)
        .exclusive(!opts.engine_opts.use_metadata_snap)
        .build()?;

    Ok(Context {
        report: opts.report.clone(),
        engine,
    })
}

fn check_superblock(sb: &Superblock) -> anyhow::Result<()> {
    if sb.version > 1 {
        return Err(anyhow!("unknown superblock version"));
    }
    Ok(())
}

pub fn check(opts: &EraCheckOptions) -> Result<()> {
    let ctx = mk_context(opts)?;
    let engine = &ctx.engine;
    let report = &ctx.report;
    let mut fatal = false;

    report.set_title("Checking era metadata");

    let metadata_sm = core_sm(engine.get_nr_blocks(), u8::MAX as u32);
    inc_superblock(&metadata_sm)?;

    let sb = read_superblock(engine.as_ref(), SUPERBLOCK_LOCATION)?;
    check_superblock(&sb)?;

    if opts.sb_only {
        return Ok(());
    }

    let mut path = vec![0];
    let writesets = btree_to_map::<Writeset>(
        &mut path,
        engine.clone(),
        opts.ignore_non_fatal,
        sb.writeset_tree_root,
    )?;

    for ws in writesets.values() {
        let (_bs, err) = read_bitset_checked_with_sm(
            engine.clone(),
            ws.root,
            ws.nr_bits as usize,
            metadata_sm.clone(),
            opts.ignore_non_fatal,
        )?;
        if err.is_some() {
            ctx.report.fatal(&format!("{}", err.unwrap()));
            fatal = true;
        }
    }

    let w = ArrayWalker::new_with_sm(engine.clone(), metadata_sm.clone(), opts.ignore_non_fatal)?;
    let c = EraChecker::new(sb.current_era);
    if let Err(e) = w.walk(&c, sb.era_array_root) {
        ctx.report.fatal(&format!("{}", e));
        fatal = true;
    }

    if fatal {
        Err(anyhow!("fatal errors in metadata"))
    } else {
        Ok(())
    }
}
