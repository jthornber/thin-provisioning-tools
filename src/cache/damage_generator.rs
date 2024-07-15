use anyhow::Result;
use std::path::Path;

use crate::cache::superblock::*;
use crate::commands::engine::*;
use crate::devtools::damage_generator::*;
use crate::pdata::space_map::common::*;
use crate::pdata::unpack::unpack;

//------------------------------------------

pub enum DamageOp {
    CreateMetadataLeaks {
        nr_blocks: usize,
        expected_rc: u32,
        actual_rc: u32,
    },
}

pub struct CacheDamageOpts<'a> {
    pub engine_opts: EngineOptions,
    pub op: DamageOp,
    pub output: &'a Path,
}

pub fn damage_metadata(opts: CacheDamageOpts) -> Result<()> {
    let engine = EngineBuilder::new(opts.output, &opts.engine_opts)
        .write(true)
        .build()?;
    let sb = read_superblock(engine.as_ref(), SUPERBLOCK_LOCATION)?;
    let sm_root = unpack::<SMRoot>(&sb.metadata_sm_root)?;

    match opts.op {
        DamageOp::CreateMetadataLeaks {
            nr_blocks,
            expected_rc,
            actual_rc,
        } => create_metadata_leaks(engine, sm_root, nr_blocks, expected_rc, actual_rc),
    }
}

//------------------------------------------
