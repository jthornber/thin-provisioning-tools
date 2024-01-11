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

#[derive(Clone, PartialEq, Eq)]
pub enum EngineType {
    #[cfg(feature = "io_uring")]
    Async,
    Sync,
    Spindle,
}

#[derive(Clone, PartialEq, Eq)]
pub enum ToolType {
    Thin,
    Cache,
    Era,
    Other,
}

#[derive(Clone)]
pub struct EngineOptions {
    pub tool: ToolType,
    pub engine_type: EngineType,
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
            .hide(true),
    )
}

//------------------------------------------

fn parse_type(matches: &ArgMatches) -> Result<EngineType> {
    let engine_type = if let Some(engine) = matches.get_one::<String>("IO_ENGINE") {
        match engine.as_str() {
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
    if !matches!(matches.try_contains_id("METADATA_SNAPSHOT"), Ok(true)) {
        // This tool doesn't use metadata snaps, or the METADATA_SNAPSHOT
        // option is not specified.
        return false;
    }

    matches!(
        matches.value_source("METADATA_SNAPSHOT"),
        Some(clap::parser::ValueSource::CommandLine)
    )
}

pub fn parse_engine_opts(tool: ToolType, matches: &ArgMatches) -> Result<EngineOptions> {
    let engine_type = parse_type(matches)?;
    let use_metadata_snap =
        (tool == ToolType::Thin || tool == ToolType::Era) && metadata_snap_flag(matches);

    Ok(EngineOptions {
        tool,
        engine_type,
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
    let e = Arc::new(SyncIoEngine::new(path, false).expect("unable to open input file"));
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
    let valid_blocks = allocated_blocks(
        e.clone(),
        metadata_root.bitmap_root,
        metadata_root.nr_blocks,
    );
    valid_blocks.unwrap_or_else(|_| all_blocks(e.get_nr_blocks() as u32))
}

fn cache_valid_blocks<P: AsRef<Path>>(_path: P, _opts: &EngineOptions) -> Result<RoaringBitmap> {
    todo!();
}

fn era_valid_blocks<P: AsRef<Path>>(_path: P, _opts: &EngineOptions) -> Result<RoaringBitmap> {
    todo!();
}

pub struct EngineBuilder<'a, P: AsRef<Path>> {
    path: P,
    opts: &'a EngineOptions,
    write: bool,
    exclusive: bool,
}

impl<'a, P: AsRef<Path>> EngineBuilder<'a, P> {
    pub fn new(path: P, opts: &'a EngineOptions) -> Self {
        Self {
            path,
            opts,
            write: false,
            exclusive: true,
        }
    }

    pub fn write(self, flag: bool) -> Self {
        Self {
            path: self.path,
            opts: self.opts,
            write: flag,
            exclusive: self.exclusive,
        }
    }

    pub fn exclusive(self, flag: bool) -> Self {
        Self {
            path: self.path,
            opts: self.opts,
            write: self.write,
            exclusive: flag,
        }
    }

    pub fn build(self) -> Result<Arc<dyn IoEngine + Send + Sync>> {
        let engine: Arc<dyn IoEngine + Send + Sync> = match self.opts.engine_type {
            #[cfg(feature = "io_uring")]
            EngineType::Async => Arc::new(AsyncIoEngine::new_with(
                self.path,
                self.write,
                self.exclusive,
            )?),
            EngineType::Sync => Arc::new(SyncIoEngine::new_with(
                self.path,
                self.write,
                self.exclusive,
            )?),
            EngineType::Spindle => {
                let valid_blocks = match self.opts.tool {
                    ToolType::Thin => thin_valid_blocks(self.path.as_ref(), self.opts),
                    ToolType::Cache => cache_valid_blocks(self.path.as_ref(), self.opts)?,
                    ToolType::Era => era_valid_blocks(self.path.as_ref(), self.opts)?,
                    ToolType::Other => {
                        let nr_blocks = get_nr_blocks(self.path.as_ref())?;
                        all_blocks(nr_blocks as u32)
                    }
                };

                Arc::new(SpindleIoEngine::new(self.path, valid_blocks, self.write)?)
            }
        };
        Ok(engine)
    }
}

//------------------------------------------
