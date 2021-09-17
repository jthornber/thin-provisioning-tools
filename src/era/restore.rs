use anyhow::{anyhow, Result};

use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::path::Path;
use std::sync::Arc;

use crate::era::ir::{self, MetadataVisitor, Visit};
use crate::era::superblock::*;
use crate::era::writeset::Writeset;
use crate::era::xml;
use crate::io_engine::*;
use crate::math::*;
use crate::pdata::array_builder::*;
use crate::pdata::btree_builder::*;
use crate::pdata::space_map_common::pack_root;
use crate::pdata::space_map_metadata::*;
use crate::report::*;
use crate::write_batcher::*;

//------------------------------------------

const MAX_CONCURRENT_IO: u32 = 1024;

//------------------------------------------

pub struct EraRestoreOptions<'a> {
    pub input: &'a Path,
    pub output: &'a Path,
    pub async_io: bool,
    pub report: Arc<Report>,
}

struct Context {
    _report: Arc<Report>,
    engine: Arc<dyn IoEngine + Send + Sync>,
}

fn mk_context(opts: &EraRestoreOptions) -> anyhow::Result<Context> {
    let engine: Arc<dyn IoEngine + Send + Sync>;

    if opts.async_io {
        engine = Arc::new(AsyncIoEngine::new(opts.output, MAX_CONCURRENT_IO, true)?);
    } else {
        let nr_threads = std::cmp::max(8, num_cpus::get() * 2);
        engine = Arc::new(SyncIoEngine::new(opts.output, nr_threads, true)?);
    }

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
    writeset_buf: (u32, u64), // (index in u64 array, value)
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
            writeset_buf: (0, 0),
            in_section: Section::None,
        }
    }

    fn finalize(&mut self) -> Result<()> {
        let src_sb;
        if let Some(sb) = self.sb.take() {
            src_sb = sb;
        } else {
            return Err(anyhow!("not in superblock"));
        }

        // build the writeset tree
        let mut tree_builder = BTreeBuilder::<Writeset>::new(Box::new(NoopRC {}));
        let mut writesets = BTreeMap::<u32, Writeset>::new();
        std::mem::swap(&mut self.writesets, &mut writesets);
        for (era, ws) in writesets {
            tree_builder.push_value(self.w, era as u64, ws)?;
        }
        let writeset_tree_root = tree_builder.complete(self.w)?;

        // complete the era array
        let era_array_root;
        if let Some(builder) = self.era_array_builder.take() {
            era_array_root = builder.complete(self.w)?;
        } else {
            return Err(anyhow!("internal error. couldn't find era array"));
        }

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
        self.writeset_buf.0 = 0;
        self.writeset_buf.1 = 0;
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
                builder.push_value(self.w, self.writeset_buf.0 as u64, self.writeset_buf.1)?;

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

    fn writeset_bit(&mut self, wbit: &ir::WritesetBit) -> Result<Visit> {
        if wbit.value {
            let index = wbit.block >> 6;
            let mask = 1 << (wbit.block & 63);
            if index == self.writeset_buf.0 {
                self.writeset_buf.1 |= mask;
            } else {
                let builder = self.writeset_builder.as_mut().unwrap();
                builder.push_value(self.w, self.writeset_buf.0 as u64, self.writeset_buf.1)?;
                self.writeset_buf.0 = index;
                self.writeset_buf.1 = mask;
            }
        }

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
