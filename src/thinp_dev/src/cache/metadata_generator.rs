use anyhow::{anyhow, Result};
use fixedbitset::FixedBitSet;
use rand::prelude::*;
use std::sync::Arc;

use thinp::cache::ir;
use thinp::cache::ir::MetadataVisitor;
use thinp::cache::restore::Restorer;
use thinp::io_engine::IoEngine;
use thinp::pdata::space_map_metadata::core_metadata_sm;
use thinp::write_batcher::WriteBatcher;

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
        // FIXME: slow & memory demanding
        let mut cblocks = (0..self.nr_cache_blocks).collect::<Vec<u32>>();
        cblocks.shuffle(&mut rand::thread_rng());
        cblocks.truncate(nr_resident as usize);
        cblocks.sort_unstable();

        v.mappings_b()?;
        {
            let mut used = FixedBitSet::with_capacity(nr_origin_blocks);
            let mut rng = rand::thread_rng();
            let mut dirty_rng = rand::thread_rng();
            for cblock in cblocks {
                // FIXME: getting slower as the collision rate raised
                let mut oblock = rng.gen_range(0..nr_origin_blocks);
                while used.contains(oblock) {
                    oblock = rng.gen_range(0..nr_origin_blocks);
                }

                used.set(oblock, true);
                v.mapping(&ir::Map {
                    cblock,
                    oblock: oblock as u64,
                    dirty: dirty_rng.gen_ratio(self.percent_dirty as u32, 100),
                })?;
            }
        }
        v.mappings_e()?;

        v.superblock_e()?;
        v.eof()?;

        Ok(())
    }
}

//------------------------------------------

pub fn format(engine: Arc<dyn IoEngine + Send + Sync>, gen: &CacheGenerator) -> Result<()> {
    let sm = core_metadata_sm(engine.get_nr_blocks(), u32::MAX);
    let batch_size = engine.get_batch_size();
    let mut w = WriteBatcher::new(engine, sm, batch_size);
    let mut restorer = Restorer::new(&mut w);

    gen.generate_metadata(&mut restorer)
}

pub fn set_needs_check(engine: Arc<dyn IoEngine + Send + Sync>) -> Result<()> {
    use thinp::cache::superblock::*;

    let mut sb = read_superblock(engine.as_ref(), SUPERBLOCK_LOCATION)?;
    sb.flags.needs_check = true;
    write_superblock(engine.as_ref(), SUPERBLOCK_LOCATION, &sb)
}

pub fn commit_new_transaction() -> Result<()> {
    // stub
    Ok(())
}

//------------------------------------------
