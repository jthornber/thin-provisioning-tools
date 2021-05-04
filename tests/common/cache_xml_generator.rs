use anyhow::Result;
use rand::prelude::*;
use std::collections::HashSet;
use std::fs::OpenOptions;
use std::path::Path;
use thinp::cache::xml;

//------------------------------------------

pub trait XmlGen {
    fn generate_xml(&mut self, v: &mut dyn xml::MetadataVisitor) -> Result<()>;
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
    block_size: u64,
    nr_cache_blocks: u64,
    nr_origin_blocks: u64,
    percent_resident: u8,
    percent_dirty: u8,
}

impl CacheGen {
    pub fn new(
        block_size: u64,
        nr_cache_blocks: u64,
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
    fn generate_xml(&mut self, v: &mut dyn xml::MetadataVisitor) -> Result<()> {
        v.superblock_b(&xml::Superblock {
            uuid: "".to_string(),
            block_size: self.block_size,
            nr_cache_blocks: self.nr_cache_blocks,
            policy: "smq".to_string(),
            hint_width: 4,
        })?;

        let mut cblocks = Vec::new();
        for n in 0..self.nr_cache_blocks {
            cblocks.push(n);
        }
        cblocks.shuffle(&mut rand::thread_rng());

        v.mappings_b()?;
        {
            let nr_resident = (self.nr_cache_blocks * 100 as u64) / (self.percent_resident as u64);
            let mut used = HashSet::new();
            for n in 0..nr_resident {
                let mut oblock = 0u64;
                while used.contains(&oblock) {
                    oblock = rand::thread_rng().gen();
                }

                used.insert(oblock);
                // FIXME: dirty should vary
                v.mapping(&xml::Map {
                    cblock: cblocks[n as usize],
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
