use anyhow::{anyhow, Result};
use rand::prelude::*;
use std::path::Path;
use std::sync::Arc;

use crate::cache::ir;
use crate::cache::ir::MetadataVisitor;
use crate::cache::restore::Restorer;
use crate::commands::engine::*;
use crate::io_engine::*;
use crate::pdata::space_map::metadata::core_metadata_sm;
use crate::write_batcher::WriteBatcher;

//------------------------------------------

pub trait MetadataGenerator {
    fn generate_metadata(&self, v: &mut dyn MetadataVisitor) -> Result<()>;
}

pub struct CacheGenerator {
    pub block_size: u32,
    pub nr_cache_blocks: u32,
    pub nr_origin_blocks: u64,
    pub percent_resident: u8,
    pub percent_dirty: u8,
    pub metadata_version: u8,
    pub hotspot_size: usize,
}

impl CacheGenerator {
    pub fn new(
        block_size: u32,
        nr_cache_blocks: u32,
        nr_origin_blocks: u64,
        percent_resident: u8,
        percent_dirty: u8,
        metadata_version: u8,
        hotspot_size: usize,
    ) -> Self {
        CacheGenerator {
            block_size,
            nr_cache_blocks,
            nr_origin_blocks,
            percent_resident,
            percent_dirty,
            metadata_version,
            hotspot_size,
        }
    }
}

impl MetadataGenerator for CacheGenerator {
    fn generate_metadata(&self, v: &mut dyn MetadataVisitor) -> Result<()> {
        if self.nr_origin_blocks > usize::MAX as u64 {
            return Err(anyhow!("number of origin blocks exceeds limits"));
        }
        let nr_origin_blocks = self.nr_origin_blocks as usize;

        let sb = ir::Superblock {
            uuid: String::new(),
            block_size: self.block_size,
            nr_cache_blocks: self.nr_cache_blocks,
            policy: String::from("smq"),
            hint_width: 4,
        };

        v.superblock_b(&sb)?;

        let nr_resident = std::cmp::min(
            (self.nr_cache_blocks as u64 * self.percent_resident as u64) / 100,
            self.nr_origin_blocks,
        ) as u32;

        // cblocks are chosen at random, with no locality
        // FIXME: slow & memory demanding
        let mut cblocks = (0..self.nr_cache_blocks).collect::<Vec<u32>>();
        cblocks.shuffle(&mut rand::thread_rng());
        cblocks.truncate(nr_resident as usize);

        // The origin blocks are allocated in randomly positioned runs.
        let mut oblocks = roaring::RoaringBitmap::new();
        let mut total_allocated = 0;
        let mut rng = rand::thread_rng();
        let mut dirty_rng = rand::thread_rng();
        'top: loop {
            let oblock = rng.gen_range(0..nr_origin_blocks);

            'run: for i in 0..self.hotspot_size {
                if total_allocated >= nr_resident {
                    break 'top;
                }

                if oblock + i >= nr_origin_blocks {
                    break 'run;
                }

                if !oblocks.contains((oblock + i) as u32) {
                    oblocks.insert((oblock + i) as u32);
                    total_allocated += 1;
                }
            }
        }

        let mut maps: Vec<(u32, u32)> = Vec::new();
        for (oblock, cblock) in oblocks.iter().zip(cblocks.iter()) {
            maps.push((oblock, *cblock));
        }
        maps.sort_by(|lhs, rhs| lhs.1.cmp(&rhs.1));

        v.mappings_b()?;
        for (oblock, cblock) in maps {
            v.mapping(&ir::Map {
                cblock,
                oblock: oblock as u64,
                dirty: dirty_rng.gen_ratio(self.percent_dirty as u32, 100),
            })?;
        }
        v.mappings_e()?;

        let mut hint = ir::Hint {
            cblock: 0,
            data: crate::cache::hint::Hint::default().hint.to_vec(),
        };

        cblocks.sort_unstable();
        v.hints_b()?;
        for cblock in cblocks {
            hint.cblock = cblock;
            v.hint(&hint)?;
        }
        v.hints_e()?;

        v.superblock_e()?;
        v.eof()?;

        Ok(())
    }
}

//------------------------------------------

fn format(engine: Arc<dyn IoEngine + Send + Sync>, gen: &CacheGenerator) -> Result<()> {
    let sm = core_metadata_sm(engine.get_nr_blocks(), u32::MAX);
    let batch_size = engine.get_batch_size();
    let mut w = WriteBatcher::new(engine, sm, batch_size);
    let mut restorer = Restorer::new(&mut w, gen.metadata_version);

    gen.generate_metadata(&mut restorer)
}

fn set_needs_check(engine: Arc<dyn IoEngine + Send + Sync>, flag: bool) -> Result<()> {
    use crate::cache::superblock::*;

    let mut sb = read_superblock(engine.as_ref(), SUPERBLOCK_LOCATION)?;
    sb.flags.needs_check = flag;
    write_superblock(engine.as_ref(), SUPERBLOCK_LOCATION, &sb)
}

fn set_clean_shutdown(engine: Arc<dyn IoEngine + Send + Sync>, flag: bool) -> Result<()> {
    use crate::cache::superblock::*;

    let mut sb = read_superblock(engine.as_ref(), SUPERBLOCK_LOCATION)?;
    sb.flags.clean_shutdown = flag;
    write_superblock(engine.as_ref(), SUPERBLOCK_LOCATION, &sb)
}

// This function does not convert formats.
// Setting the version from 2 to 1 drops the dirty bitset field in superblock,
// results in metadata leaks.
// Setting the version from 1 to 2 does not generate the dirty bitset, hence the
// superblock becomes invalid.
fn set_superblock_version(engine: Arc<dyn IoEngine + Send + Sync>, version: u32) -> Result<()> {
    use crate::cache::superblock::*;

    let mut sb = read_superblock(engine.as_ref(), SUPERBLOCK_LOCATION)?;
    sb.version = version;
    write_superblock(engine.as_ref(), SUPERBLOCK_LOCATION, &sb)
}

//------------------------------------------

#[derive(Debug)]
pub struct CacheFormatOpts {
    pub block_size: u32,
    pub nr_cache_blocks: u32,
    pub nr_origin_blocks: u64,
    pub percent_resident: u8,
    pub percent_dirty: u8,
    pub metadata_version: u8,
    pub hotspot_size: usize,
}

#[derive(Debug)]
pub enum MetadataOp {
    Format(CacheFormatOpts),
    SetNeedsCheck(bool),
    SetCleanShutdown(bool),
    SetSuperblockVersion(u32),
}

pub struct CacheGenerateOpts<'a> {
    pub op: MetadataOp,
    pub engine_opts: EngineOptions,
    pub output: &'a Path,
}

pub fn generate_metadata(opts: CacheGenerateOpts) -> Result<()> {
    let engine = EngineBuilder::new(opts.output, &opts.engine_opts)
        .write(true)
        .build()?;

    match opts.op {
        MetadataOp::Format(op) => {
            let cache_gen = CacheGenerator {
                block_size: op.block_size,
                nr_cache_blocks: op.nr_cache_blocks,
                nr_origin_blocks: op.nr_origin_blocks,
                percent_resident: op.percent_resident,
                percent_dirty: op.percent_dirty,
                metadata_version: op.metadata_version,
                hotspot_size: op.hotspot_size,
            };
            format(engine, &cache_gen)?;
        }
        MetadataOp::SetNeedsCheck(flag) => {
            set_needs_check(engine, flag)?;
        }
        MetadataOp::SetCleanShutdown(flag) => {
            set_clean_shutdown(engine, flag)?;
        }
        MetadataOp::SetSuperblockVersion(version) => {
            set_superblock_version(engine, version)?;
        }
    }

    Ok(())
}

//------------------------------------------
