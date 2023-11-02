use anyhow::{anyhow, Result};
use rand::prelude::*;
use std::path::Path;
use std::sync::Arc;

use crate::commands::engine::*;
use crate::era::ir::{self, MetadataVisitor};
use crate::era::restore::Restorer;
use crate::io_engine::*;
use crate::pdata::space_map::metadata::core_metadata_sm;
use crate::write_batcher::WriteBatcher;

//------------------------------------------

pub trait MetadataGenerator {
    fn generate_metadata(&self, v: &mut dyn MetadataVisitor) -> Result<()>;
}

//------------------------------------------

// Ordered sequence generator where each element has an independent probability
// of being present.
struct IndependentSequence {
    begin: u32,
    end: u32,
    prob: u32,
    rng: ThreadRng,
}

impl IndependentSequence {
    fn new(begin: u32, end: u32, prob: u32) -> IndependentSequence {
        IndependentSequence {
            begin,
            end,
            prob,
            rng: rand::thread_rng(),
        }
    }
}

impl Iterator for IndependentSequence {
    type Item = std::ops::Range<u32>;

    // FIXME: reduce complexity
    fn next(&mut self) -> Option<std::ops::Range<u32>> {
        if self.begin >= self.end {
            return None;
        }

        let mut b = self.begin;
        while b < self.end && self.rng.gen_range(0..100) >= self.prob {
            b += 1;
        }

        if b == self.end {
            return None;
        }

        let mut e = b + 1;
        while e < self.end && self.rng.gen_range(0..100) < self.prob {
            e += 1;
        }
        self.begin = e + 1;

        Some(std::ops::Range { start: b, end: e })
    }
}

//------------------------------------------

fn create_superblock(block_size: u32, nr_blocks: u32, current_era: u32) -> ir::Superblock {
    ir::Superblock {
        uuid: "".to_string(),
        block_size,
        nr_blocks,
        current_era,
    }
}

pub struct CleanShutdownMeta {
    block_size: u32,
    nr_blocks: u32,
    current_era: u32,
    nr_writesets: u32,
}

impl CleanShutdownMeta {
    pub fn new(block_size: u32, nr_blocks: u32, current_era: u32, nr_writesets: u32) -> Self {
        CleanShutdownMeta {
            block_size,
            nr_blocks,
            current_era,
            nr_writesets,
        }
    }

    fn generate_writeset(v: &mut dyn MetadataVisitor, ws: &ir::Writeset) -> Result<()> {
        v.writeset_b(ws)?;
        let gen = IndependentSequence::new(0, ws.nr_bits, 10);
        for seq in gen {
            v.writeset_blocks(&ir::MarkedBlocks {
                begin: seq.start,
                len: seq.end - seq.start,
            })?;
        }
        v.writeset_e()?;

        Ok(())
    }

    fn generate_era_array(v: &mut dyn MetadataVisitor, nr_blocks: u32, max_era: u32) -> Result<()> {
        let mut rng = rand::thread_rng();
        v.era_b()?;
        for b in 0..nr_blocks {
            let era = rng.gen_range(0..max_era);
            v.era(&ir::Era { block: b, era })?;
        }
        v.era_e()?;

        Ok(())
    }
}

impl MetadataGenerator for CleanShutdownMeta {
    fn generate_metadata(&self, v: &mut dyn MetadataVisitor) -> Result<()> {
        if self.current_era < 1 {
            return Err(anyhow!("current era must be greater than 0"));
        }

        if self.current_era < self.nr_writesets {
            return Err(anyhow!("number of writesets exceeds the current era"));
        }

        v.superblock_b(&create_superblock(
            self.block_size,
            self.nr_blocks,
            self.current_era,
        ))?;

        let era_low = self.current_era - self.nr_writesets + 1;
        for era in era_low..self.current_era + 1 {
            Self::generate_writeset(
                v,
                &ir::Writeset {
                    era,
                    nr_bits: self.nr_blocks,
                },
            )?;
        }

        Self::generate_era_array(v, self.nr_blocks, era_low)?;

        v.superblock_e()?;
        Ok(())
    }
}

//------------------------------------------

fn format(engine: Arc<dyn IoEngine + Send + Sync>, gen: &dyn MetadataGenerator) -> Result<()> {
    let sm = core_metadata_sm(engine.get_nr_blocks(), u32::MAX);
    let batch_size = engine.get_batch_size();
    let mut w = WriteBatcher::new(engine, sm, batch_size);
    let mut restorer = Restorer::new(&mut w);

    gen.generate_metadata(&mut restorer)
}

//------------------------------------------

#[derive(Debug)]
pub struct EraFormatOpts {
    pub block_size: u32,
    pub nr_blocks: u32,
    pub current_era: u32,
    pub nr_writesets: u32,
}

#[derive(Debug)]
pub enum MetadataOp {
    Format(EraFormatOpts),
}

pub struct EraGenerateOpts<'a> {
    pub op: MetadataOp,
    pub engine_opts: EngineOptions,
    pub output: &'a Path,
}

pub fn generate_metadata(opts: EraGenerateOpts) -> Result<()> {
    let engine = EngineBuilder::new(opts.output, &opts.engine_opts)
        .write(true)
        .build()?;

    match opts.op {
        MetadataOp::Format(op) => {
            let era_meta = CleanShutdownMeta {
                block_size: op.block_size,
                nr_blocks: op.nr_blocks,
                current_era: op.current_era,
                nr_writesets: op.nr_writesets,
            };
            format(engine, &era_meta)?;
        }
    }

    Ok(())
}

//------------------------------------------
