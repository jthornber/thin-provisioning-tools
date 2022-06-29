use anyhow::{anyhow, Result};
use clap::ArgMatches;
use roaring::*;
use std::path::Path;
use std::sync::Arc;

use crate::io_engine::*;
use crate::pdata::space_map::allocated_blocks::*;
use crate::pdata::space_map::common::*;
use crate::pdata::unpack::*;
use crate::thin::superblock::*;

//------------------------------------------

pub enum EngineType {
    #[cfg(feature = "io_uring")]
    Async,
    Sync,
    Spindle,
}

#[derive(PartialEq, Eq)]
pub enum ToolType {
    Thin,
    Cache,
    Era,
    Other,
}

pub struct EngineOptions {
    pub tool: ToolType,
    pub engine_type: EngineType,
    pub exclusive: bool,
    pub write: bool,
    pub use_metadata_snap: bool,
}

//------------------------------------------

// Add in the standard engine choice flags
pub fn engine_args(cmd: clap::Command) -> clap::Command {
    use clap::Arg;

    cmd.arg(
        Arg::new("IO_ENGINE")
            .help("Select an io engine to use")
            .long("io-engine")
            .value_name("IO_ENGINE")
            .takes_value(true)
            .hide(true),
    )
}

//------------------------------------------

fn parse_type(matches: &ArgMatches) -> Result<EngineType> {
    let engine_type = if let Some(engine) = matches.value_of("IO_ENGINE") {
        match engine {
            "sync" => EngineType::Sync,
            "spindle" => EngineType::Spindle,
            #[cfg(feature = "io_uring")]
            "async" => EngineType::Async,
            #[cfg(not(feature = "io_uring"))]
            "async" => {
                return Err(anyhow!(
                    "This tool has not been compiled with async engine support"
                ));
            }
            _ => {
                return Err(anyhow!(format!("unknown io engine type '{}'", engine)));
            }
        }
    } else {
        EngineType::Sync
    };

    Ok(engine_type)
}

fn metadata_snap_flag(matches: &ArgMatches) -> bool {
    let ms = matches.try_contains_id("METADATA_SNAPSHOT");
    if ms.is_err() {
        // This tool doesn't use metadata snaps
        return false;
    }
    ms.unwrap()
}

pub fn parse_engine_opts(
    tool: ToolType,
    write: bool,
    matches: &ArgMatches,
) -> Result<EngineOptions> {
    let engine_type = parse_type(matches)?;
    let use_metadata_snap = (tool == ToolType::Thin) && metadata_snap_flag(matches);
    let exclusive = match (write, use_metadata_snap) {
        (false, true) => false,
        (false, false) => true,
        (true, _) => true,
    };

    Ok(EngineOptions {
        tool,
        engine_type,
        exclusive,
        write,
        use_metadata_snap,
    })
}

//------------------------------------------

fn all_blocks(nr_blocks: u32) -> RoaringBitmap {
    let mut r = RoaringBitmap::new();
    r.insert_range(0..nr_blocks);
    r
}

fn thin_read_sb(
    engine: Arc<dyn IoEngine + Sync + Send>,
    use_metadata_snap: bool,
) -> Result<Superblock> {
    // superblock
    let sb = if use_metadata_snap {
        read_superblock_snap(engine.as_ref())?
    } else {
        read_superblock(engine.as_ref(), SUPERBLOCK_LOCATION)?
    };
    Ok(sb)
}

// use a Sync engine to read the metadata space map, if this fails we
// assume all blocks are valid.
fn thin_valid_blocks<P: AsRef<Path>>(path: P, opts: &EngineOptions) -> RoaringBitmap {
    let e = Arc::new(SyncIoEngine::new(path, opts.exclusive).expect("unable to open input file"));
    let sb = thin_read_sb(e.clone(), opts.use_metadata_snap);
    if sb.is_err() {
        return all_blocks(e.get_nr_blocks() as u32);
    }
    let sb = sb.unwrap();

    let metadata_root = unpack::<SMRoot>(&sb.metadata_sm_root[0..]);
    if metadata_root.is_err() {
        return all_blocks(e.get_nr_blocks() as u32);
    }
    let metadata_root = metadata_root.unwrap();
    let valid_blocks = allocated_blocks(e.clone(), metadata_root.bitmap_root);
    if valid_blocks.is_err() {
        all_blocks(e.get_nr_blocks() as u32)
    } else {
        valid_blocks.unwrap()
    }
}

fn cache_valid_blocks<P: AsRef<Path>>(_path: P, _opts: &EngineOptions) -> Result<RoaringBitmap> {
    todo!();
}

fn era_valid_blocks<P: AsRef<Path>>(_path: P, _opts: &EngineOptions) -> Result<RoaringBitmap> {
    todo!();
}

pub fn build_io_engine<P: AsRef<Path>>(
    path: P,
    opts: &EngineOptions,
) -> Result<Arc<dyn IoEngine + Send + Sync>> {
    let engine: Arc<dyn IoEngine + Send + Sync> = match opts.engine_type {
        #[cfg(feature = "io_uring")]
        EngineType::Async => Arc::new(AsyncIoEngine::new_with(path, opts.write, opts.exclusive)?),
        EngineType::Sync => Arc::new(SyncIoEngine::new(path, opts.exclusive)?),
        EngineType::Spindle => {
            let valid_blocks = match opts.tool {
                ToolType::Thin => thin_valid_blocks(path.as_ref(), opts),
                ToolType::Cache => cache_valid_blocks(path.as_ref(), opts)?,
                ToolType::Era => era_valid_blocks(path.as_ref(), opts)?,
                ToolType::Other => {
                    let nr_blocks = get_nr_blocks(path.as_ref())?;
                    all_blocks(nr_blocks as u32)
                }
            };

            Arc::new(SpindleIoEngine::new(path, valid_blocks, opts.exclusive)?)
        }
    };
    Ok(engine)
}

//------------------------------------------
