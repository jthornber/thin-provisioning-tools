use anyhow::Result;
use rand::prelude::*;
use std::collections::HashSet;
use std::fs::OpenOptions;
use std::path::Path;
use thinp::cache::ir::{self, MetadataVisitor};
use thinp::cache::xml;

//------------------------------------------

pub trait XmlGen {
    fn generate_xml(&mut self, v: &mut dyn MetadataVisitor) -> Result<()>;
}

pub fn write_xml(path: &Path, g: &mut dyn XmlGen) -> Result<()> {
    let xml_out = OpenOptions::new()
        .read(false)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;
    let mut w = xml::XmlWriter::new(xml_out);

    g.generate_xml(&mut w)
}

pub struct CacheGen {
    block_size: u32,
    nr_cache_blocks: u32,
    nr_origin_blocks: u64,
    percent_resident: u8,
    percent_dirty: u8,
}

impl CacheGen {
    pub fn new(
        block_size: u32,
        nr_cache_blocks: u32,
        nr_origin_blocks: u64,
        percent_resident: u8,
        percent_dirty: u8,
    ) -> Self {
        CacheGen {
            block_size,
            nr_cache_blocks,
            nr_origin_blocks,
            percent_resident,
            percent_dirty,
        }
    }
}

impl XmlGen for CacheGen {
    fn generate_xml(&mut self, v: &mut dyn MetadataVisitor) -> Result<()> {
        v.superblock_b(&ir::Superblock {
            uuid: "".to_string(),
            block_size: self.block_size,
            nr_cache_blocks: self.nr_cache_blocks,
            policy: "smq".to_string(),
            hint_width: 4,
        })?;

        let nr_resident = (self.nr_cache_blocks * self.percent_resident as u32) / 100u32;
        let mut cblocks = (0..self.nr_cache_blocks).collect::<Vec<u32>>();
        cblocks.shuffle(&mut rand::thread_rng());
        cblocks.truncate(nr_resident as usize);
        cblocks.sort_unstable();

        v.mappings_b()?;
        {
            let mut used = HashSet::new();
            let mut rng = rand::thread_rng();
            for cblock in cblocks {
                let mut oblock = 0u64;
                while used.contains(&oblock) {
                    oblock = rng.gen_range(0..self.nr_origin_blocks);
                }

                used.insert(oblock);
                // FIXME: dirty should vary
                v.mapping(&ir::Map {
                    cblock,
                    oblock,
                    dirty: false,
                })?;
            }
        }
        v.mappings_e()?;

        v.superblock_e()?;
        Ok(())
    }
}

//------------------------------------------
