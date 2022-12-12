use anyhow::{anyhow, Result};

use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::path::Path;
use std::sync::Arc;

use crate::commands::engine::*;
use crate::era::ir::{self, MetadataVisitor, Visit};
use crate::era::superblock::*;
use crate::era::writeset::Writeset;
use crate::era::xml;
use crate::io_engine::*;
use crate::math::*;
use crate::pdata::array_builder::*;
use crate::pdata::btree_builder::*;
use crate::pdata::space_map::common::pack_root;
use crate::pdata::space_map::metadata::*;
use crate::report::*;
use crate::write_batcher::*;

//------------------------------------------

pub struct EraRestoreOptions<'a> {
    pub input: &'a Path,
    pub output: &'a Path,
    pub engine_opts: EngineOptions,
    pub report: Arc<Report>,
}

struct Context {
    _report: Arc<Report>,
    engine: Arc<dyn IoEngine + Send + Sync>,
}

fn mk_context(opts: &EraRestoreOptions) -> anyhow::Result<Context> {
    let engine = EngineBuilder::new(opts.output, &opts.engine_opts)
        .write(true)
        .build()?;

    Ok(Context {
        _report: opts.report.clone(),
        engine,
    })
}

//------------------------------------------

#[derive(PartialEq)]
enum Section {
    None,
    Superblock,
    Writeset,
    EraArray,
    Finalized,
}

pub struct Restorer<'a> {
    w: &'a mut WriteBatcher,
    sb: Option<ir::Superblock>,
    writesets: BTreeMap<u32, Writeset>,
    writeset_builder: Option<ArrayBuilder<u64>>, // bitset
    current_writeset: Option<ir::Writeset>,
    era_array_builder: Option<ArrayBuilder<u32>>,
    writeset_entry: u64,
    entry_index: u32,
    in_section: Section,
}

impl<'a> Restorer<'a> {
    pub fn new(w: &'a mut WriteBatcher) -> Restorer<'a> {
        Restorer {
            w,
            sb: None,
            writesets: BTreeMap::new(),
            writeset_builder: None,
            current_writeset: None,
            era_array_builder: None,
            writeset_entry: 0,
            entry_index: 0,
            in_section: Section::None,
        }
    }

    fn finalize(&mut self) -> Result<()> {
        let src_sb = if let Some(sb) = self.sb.take() {
            sb
        } else {
            return Err(anyhow!("not in superblock"));
        };

        // build the writeset tree
        let mut tree_builder = BTreeBuilder::<Writeset>::new(Box::new(NoopRC {}));
        let mut writesets = BTreeMap::<u32, Writeset>::new();
        std::mem::swap(&mut self.writesets, &mut writesets);
        for (era, ws) in writesets {
            tree_builder.push_value(self.w, era as u64, ws)?;
        }
        let writeset_tree_root = tree_builder.complete(self.w)?;

        // complete the era array
        let era_array_root = if let Some(builder) = self.era_array_builder.take() {
            builder.complete(self.w)?
        } else {
            return Err(anyhow!("internal error. couldn't find era array"));
        };

        // build metadata space map
        let metadata_sm_root = build_metadata_sm(self.w)?;

        let sb = Superblock {
            flags: SuperblockFlags {
                clean_shutdown: true,
            },
            block: SUPERBLOCK_LOCATION,
            version: 1,
            metadata_sm_root,
            data_block_size: src_sb.block_size,
            nr_blocks: src_sb.nr_blocks,
            current_era: src_sb.current_era,
            current_writeset: Writeset {
                nr_bits: src_sb.nr_blocks,
                root: 0,
            },
            writeset_tree_root,
            era_array_root,
            metadata_snap: 0,
        };
        write_superblock(self.w.engine.as_ref(), SUPERBLOCK_LOCATION, &sb)?;

        self.in_section = Section::Finalized;
        Ok(())
    }
}

impl<'a> MetadataVisitor for Restorer<'a> {
    fn superblock_b(&mut self, sb: &ir::Superblock) -> Result<Visit> {
        if self.in_section != Section::None {
            return Err(anyhow!("duplicated superblock"));
        }

        self.sb = Some(sb.clone());
        let b = self.w.alloc()?;
        if b.loc != SUPERBLOCK_LOCATION {
            return Err(anyhow!("superblock was occupied"));
        }

        self.writeset_builder = None;
        self.era_array_builder = Some(ArrayBuilder::new(sb.nr_blocks as u64));
        self.in_section = Section::Superblock;

        Ok(Visit::Continue)
    }

    fn superblock_e(&mut self) -> Result<Visit> {
        self.finalize()?;
        Ok(Visit::Continue)
    }

    fn writeset_b(&mut self, ws: &ir::Writeset) -> Result<Visit> {
        if self.in_section != Section::Superblock {
            return Err(anyhow!("not in superblock"));
        }
        self.writeset_builder = Some(ArrayBuilder::new(div_up(ws.nr_bits as u64, 64)));
        self.entry_index = 0;
        self.writeset_entry = 0;
        self.current_writeset = Some(ws.clone());
        self.in_section = Section::Writeset;
        Ok(Visit::Continue)
    }

    fn writeset_e(&mut self) -> Result<Visit> {
        if self.in_section != Section::Writeset {
            return Err(anyhow!("not in writeset"));
        }

        if let Some(mut builder) = self.writeset_builder.take() {
            if let Some(ws) = self.current_writeset.take() {
                // push the trailing bits
                builder.push_value(self.w, self.entry_index as u64, self.writeset_entry)?;

                let root = builder.complete(self.w)?;
                self.writesets.insert(
                    ws.era,
                    Writeset {
                        root,
                        nr_bits: ws.nr_bits,
                    },
                );
                self.in_section = Section::Superblock;
            } else {
                return Err(anyhow!("internal error. couldn't find writeset"));
            }
        } else {
            return Err(anyhow!("internal error. couldn't find writeset"));
        }

        Ok(Visit::Continue)
    }

    fn writeset_blocks(&mut self, blocks: &ir::MarkedBlocks) -> Result<Visit> {
        let first = blocks.begin;
        let last = first + blocks.len - 1; // inclusive
        let mut idx = first >> 6;
        let last_idx = last >> 6; // inclusive
        let builder = self.writeset_builder.as_mut().unwrap();

        // emit the buffered bits
        if idx > self.entry_index {
            builder.push_value(self.w, self.entry_index as u64, self.writeset_entry)?;
            self.entry_index = idx;
            self.writeset_entry = 0;
        }

        // buffer the bits of the first entry
        let bi_first = first & 63;
        if idx == last_idx {
            let bi_last = last & 63;
            let mask = 1u64 << bi_last;
            self.writeset_entry |= (mask ^ mask.wrapping_sub(1)) & (u64::MAX << bi_first);

            return Ok(Visit::Continue);
        }

        self.writeset_entry |= u64::MAX << bi_first;

        // emit the all-1 entries if necessary
        while idx < last_idx {
            builder.push_value(self.w, self.entry_index as u64, self.writeset_entry)?;
            self.entry_index += 1;
            self.writeset_entry = u64::MAX;
            idx += 1;
        }

        // buffer the bits of the last entry
        builder.push_value(self.w, self.entry_index as u64, self.writeset_entry)?;
        let bi_last = last & 63;
        let mask = 1u64 << bi_last;
        self.entry_index += 1;
        self.writeset_entry |= mask ^ mask.wrapping_sub(1);

        Ok(Visit::Continue)
    }

    fn era_b(&mut self) -> Result<Visit> {
        if self.in_section != Section::Superblock {
            return Err(anyhow!("not in superblock"));
        }
        self.in_section = Section::EraArray;
        Ok(Visit::Continue)
    }

    fn era_e(&mut self) -> Result<Visit> {
        if self.in_section != Section::EraArray {
            return Err(anyhow!("not in era array"));
        }
        self.in_section = Section::Superblock;
        Ok(Visit::Continue)
    }

    fn era(&mut self, era: &ir::Era) -> Result<Visit> {
        let builder = self.era_array_builder.as_mut().unwrap();
        builder.push_value(self.w, era.block as u64, era.era)?;
        Ok(Visit::Continue)
    }

    fn eof(&mut self) -> Result<Visit> {
        if self.in_section != Section::Finalized {
            return Err(anyhow!("incompleted source metadata"));
        }
        Ok(Visit::Continue)
    }
}

//------------------------------------------

fn build_metadata_sm(w: &mut WriteBatcher) -> Result<Vec<u8>> {
    let r = write_metadata_sm(w)?;
    let sm_root = pack_root(&r, SPACE_MAP_ROOT_SIZE)?;

    Ok(sm_root)
}

//------------------------------------------

pub fn restore(opts: EraRestoreOptions) -> Result<()> {
    let input = OpenOptions::new()
        .read(true)
        .write(false)
        .open(opts.input)?;

    let ctx = mk_context(&opts)?;

    let sm = core_metadata_sm(ctx.engine.get_nr_blocks(), u32::MAX);
    let mut w = WriteBatcher::new(ctx.engine.clone(), sm.clone(), ctx.engine.get_batch_size());

    let mut restorer = Restorer::new(&mut w);
    xml::read(input, &mut restorer)?;

    Ok(())
}

//------------------------------------------
