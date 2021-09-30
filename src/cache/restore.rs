use anyhow::{anyhow, Result};

use std::convert::TryInto;
use std::fs::OpenOptions;
use std::path::Path;
use std::sync::Arc;

use crate::cache::hint::Hint;
use crate::cache::ir::{self, MetadataVisitor, Visit};
use crate::cache::mapping::{Mapping, MappingFlags};
use crate::cache::superblock::*;
use crate::cache::xml;
use crate::io_engine::*;
use crate::math::*;
use crate::pdata::array_builder::*;
use crate::pdata::space_map_common::pack_root;
use crate::pdata::space_map_metadata::*;
use crate::report::*;
use crate::write_batcher::*;

//------------------------------------------

const MAX_CONCURRENT_IO: u32 = 1024;

//------------------------------------------

pub struct CacheRestoreOptions<'a> {
    pub input: &'a Path,
    pub output: &'a Path,
    pub async_io: bool,
    pub report: Arc<Report>,
}

struct Context {
    _report: Arc<Report>,
    engine: Arc<dyn IoEngine + Send + Sync>,
}

fn mk_context(opts: &CacheRestoreOptions) -> anyhow::Result<Context> {
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
    Mappings,
    Hints,
    Finalized,
}

pub struct Restorer<'a> {
    write_batcher: &'a mut WriteBatcher,
    sb: Option<ir::Superblock>,
    mapping_builder: Option<ArrayBuilder<Mapping>>,
    dirty_builder: Option<ArrayBuilder<u64>>,
    hint_builder: Option<ArrayBuilder<Hint>>,
    mapping_root: Option<u64>,
    dirty_root: Option<u64>,
    hint_root: Option<u64>,
    discard_root: Option<u64>,
    dirty_bits: (u32, u64), // (index in u64 array, value)
    in_section: Section,
}

impl<'a> Restorer<'a> {
    pub fn new(w: &'a mut WriteBatcher) -> Restorer<'a> {
        Restorer {
            write_batcher: w,
            sb: None,
            mapping_builder: None,
            dirty_builder: None,
            hint_builder: None,
            mapping_root: None,
            dirty_root: None,
            hint_root: None,
            discard_root: None,
            dirty_bits: (0, 0),
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

        // complete the mapping array
        if let Some(builder) = self.mapping_builder.take() {
            self.mapping_root = Some(builder.complete(self.write_batcher)?);
        }

        // complete the dirty array
        if let Some(mut builder) = self.dirty_builder.take() {
            // push the bufferred trailing bits
            builder.push_value(
                self.write_batcher,
                self.dirty_bits.0 as u64,
                self.dirty_bits.1,
            )?;

            self.dirty_root = Some(builder.complete(self.write_batcher)?);
        }

        // complete the hint array
        if let Some(builder) = self.hint_builder.take() {
            self.hint_root = Some(builder.complete(self.write_batcher)?);
        }

        // build metadata space map
        let metadata_sm_root = build_metadata_sm(self.write_batcher)?;

        let mapping_root = self.mapping_root.as_ref().unwrap();
        let hint_root = self.hint_root.as_ref().unwrap();
        let discard_root = self.discard_root.as_ref().unwrap();
        let sb = Superblock {
            flags: SuperblockFlags {
                clean_shutdown: true,
                needs_check: false,
            },
            block: SUPERBLOCK_LOCATION,
            version: 2,
            policy_name: src_sb.policy.as_bytes().to_vec(),
            policy_version: vec![2, 0, 0],
            policy_hint_size: src_sb.hint_width,
            metadata_sm_root,
            mapping_root: *mapping_root,
            dirty_root: self.dirty_root, // dirty_root is optional
            hint_root: *hint_root,
            discard_root: *discard_root,
            discard_block_size: 0,
            discard_nr_blocks: 0,
            data_block_size: src_sb.block_size,
            cache_blocks: src_sb.nr_cache_blocks,
            compat_flags: 0,
            compat_ro_flags: 0,
            incompat_flags: 0,
            read_hits: 0,
            read_misses: 9,
            write_hits: 0,
            write_misses: 0,
        };
        write_superblock(self.write_batcher.engine.as_ref(), SUPERBLOCK_LOCATION, &sb)?;

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
        let b = self.write_batcher.alloc()?;
        if b.loc != SUPERBLOCK_LOCATION {
            return Err(anyhow!("superblock was occupied"));
        }

        self.mapping_builder = Some(ArrayBuilder::new(sb.nr_cache_blocks as u64));
        self.dirty_builder = Some(ArrayBuilder::new(div_up(sb.nr_cache_blocks as u64, 64)));
        self.hint_builder = Some(ArrayBuilder::new(sb.nr_cache_blocks as u64));

        let discard_builder = ArrayBuilder::<u64>::new(0); // discard bitset is optional
        self.discard_root = Some(discard_builder.complete(self.write_batcher)?);
        self.in_section = Section::Superblock;

        Ok(Visit::Continue)
    }

    fn superblock_e(&mut self) -> Result<Visit> {
        self.finalize()?;
        Ok(Visit::Continue)
    }

    fn mappings_b(&mut self) -> Result<Visit> {
        if self.in_section != Section::Superblock {
            return Err(anyhow!("not in superblock"));
        }
        self.in_section = Section::Mappings;
        Ok(Visit::Continue)
    }

    fn mappings_e(&mut self) -> Result<Visit> {
        if self.in_section != Section::Mappings {
            return Err(anyhow!("not in mappings"));
        }
        self.in_section = Section::Superblock;
        Ok(Visit::Continue)
    }

    fn mapping(&mut self, m: &ir::Map) -> Result<Visit> {
        let map = Mapping {
            oblock: m.oblock,
            flags: MappingFlags::Valid as u32,
        };
        let mapping_builder = self.mapping_builder.as_mut().unwrap();
        mapping_builder.push_value(self.write_batcher, m.cblock as u64, map)?;

        if m.dirty {
            let index = m.cblock >> 6;
            let bi = m.cblock & 63;
            if index == self.dirty_bits.0 {
                self.dirty_bits.1 |= 1 << bi;
            } else {
                let dirty_builder = self.dirty_builder.as_mut().unwrap();
                dirty_builder.push_value(
                    self.write_batcher,
                    self.dirty_bits.0 as u64,
                    self.dirty_bits.1,
                )?;
                self.dirty_bits.0 = index;
                self.dirty_bits.1 = 0;
            }
        }

        Ok(Visit::Continue)
    }

    fn hints_b(&mut self) -> Result<Visit> {
        if self.in_section != Section::Superblock {
            return Err(anyhow!("not in superblock"));
        }
        self.in_section = Section::Hints;
        Ok(Visit::Continue)
    }

    fn hints_e(&mut self) -> Result<Visit> {
        if self.in_section != Section::Hints {
            return Err(anyhow!("not in hints"));
        }
        self.in_section = Section::Superblock;
        Ok(Visit::Continue)
    }

    fn hint(&mut self, h: &ir::Hint) -> Result<Visit> {
        let hint = Hint {
            hint: h.data[..].try_into().unwrap(),
        };
        let hint_builder = self.hint_builder.as_mut().unwrap();
        hint_builder.push_value(self.write_batcher, h.cblock as u64, hint)?;
        Ok(Visit::Continue)
    }

    fn discards_b(&mut self) -> Result<Visit> {
        Ok(Visit::Continue)
    }

    fn discards_e(&mut self) -> Result<Visit> {
        Ok(Visit::Continue)
    }

    fn discard(&mut self, _d: &ir::Discard) -> Result<Visit> {
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

pub fn restore(opts: CacheRestoreOptions) -> Result<()> {
    let input = OpenOptions::new()
        .read(true)
        .write(false)
        .open(opts.input)?;

    let ctx = mk_context(&opts)?;

    let sm = core_metadata_sm(ctx.engine.get_nr_blocks(), u32::MAX);
    let mut w = WriteBatcher::new(ctx.engine.clone(), sm.clone(), ctx.engine.get_batch_size());

    // build cache mappings
    let mut restorer = Restorer::new(&mut w);
    xml::read(input, &mut restorer)?;

    Ok(())
}

//------------------------------------------
