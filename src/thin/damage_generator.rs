use anyhow::Result;

use std::path::Path;
use std::sync::Arc;

use crate::commands::engine::*;
use crate::devtools::damage_generator::*;
use crate::io_engine::IoEngine;

use crate::pdata::space_map::common::*;

use crate::pdata::unpack::unpack;
use crate::thin::superblock::*;

//------------------------------------------

pub struct SuperblockOverrides {
    pub mapping_root: Option<u64>,
    pub details_root: Option<u64>,
    pub metadata_snapshot: Option<u64>,
}

pub fn override_superblock(
    engine: Arc<dyn IoEngine + Send + Sync>,
    opts: &SuperblockOverrides,
) -> Result<()> {
    let mut sb = read_superblock(engine.as_ref(), SUPERBLOCK_LOCATION)?;
    if let Some(v) = opts.mapping_root {
        sb.mapping_root = v;
    }
    if let Some(v) = opts.details_root {
        sb.details_root = v;
    }
    if let Some(v) = opts.metadata_snapshot {
        sb.metadata_snap = v;
    }
    write_superblock(engine.as_ref(), 0, &sb)
}

//------------------------------------------

pub enum DamageOp {
    CreateMetadataLeaks {
        nr_blocks: usize,
        expected_rc: u32,
        actual_rc: u32,
    },
    OverrideSuperblock(SuperblockOverrides),
}

pub struct ThinDamageOpts<'a> {
    pub engine_opts: EngineOptions,
    pub op: DamageOp,
    pub output: &'a Path,
}

pub fn damage_metadata(opts: ThinDamageOpts) -> Result<()> {
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
        DamageOp::OverrideSuperblock(opts) => override_superblock(engine, &opts),
    }
}

//------------------------------------------
