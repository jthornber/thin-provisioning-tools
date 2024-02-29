use anyhow::{anyhow, Context};
use fixedbitset::FixedBitSet;
use std::fs::File;
use std::io::BufWriter;
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::cache::hint::Hint;
use crate::cache::ir::{self, MetadataVisitor};
use crate::cache::mapping::Mapping;
use crate::cache::superblock::*;
use crate::cache::xml;
use crate::commands::engine::*;
use crate::dump_utils::{self, *};
use crate::io_engine::*;
use crate::pdata::array::ArrayBlock;
use crate::pdata::array_walker::*;
use crate::pdata::bitset::{read_bitset_checked, CheckedBitSet};

//------------------------------------------

mod format1 {
    use super::*;

    struct Inner<'a> {
        visitor: &'a mut dyn MetadataVisitor,
        valid_mappings: FixedBitSet,
    }

    pub struct MappingEmitter<'a> {
        inner: Mutex<Inner<'a>>,
    }

    impl<'a> MappingEmitter<'a> {
        pub fn new(nr_entries: usize, visitor: &'a mut dyn MetadataVisitor) -> MappingEmitter<'a> {
            MappingEmitter {
                inner: Mutex::new(Inner {
                    visitor,
                    valid_mappings: FixedBitSet::with_capacity(nr_entries),
                }),
            }
        }

        pub fn get_valid(self) -> FixedBitSet {
            let inner = self.inner.into_inner().unwrap();
            inner.valid_mappings
        }
    }

    impl<'a> dump_utils::ArrayVisitor<Mapping> for MappingEmitter<'a> {
        fn visit(&self, index: u64, b: ArrayBlock<Mapping>) -> anyhow::Result<()> {
            let cbegin = index as u32 * b.header.max_entries;
            let cend = cbegin + b.header.nr_entries;
            for (map, cblock) in b.values.iter().zip(cbegin..cend) {
                if !map.is_valid() {
                    continue;
                }

                let m = ir::Map {
                    cblock,
                    oblock: map.oblock,
                    dirty: map.is_dirty(),
                };

                let mut inner = self.inner.lock().unwrap();
                inner.valid_mappings.set(cblock as usize, true);
                inner.visitor.mapping(&m)?;
            }

            Ok(())
        }
    }
}

//------------------------------------------

mod format2 {
    use super::*;

    //-------------------
    // Mapping visitor

    struct Inner<'a> {
        visitor: &'a mut dyn MetadataVisitor,
        dirty_bits: CheckedBitSet,
        valid_mappings: FixedBitSet,
    }

    pub struct MappingEmitter<'a> {
        inner: Mutex<Inner<'a>>,
    }

    impl<'a> MappingEmitter<'a> {
        pub fn new(
            nr_entries: usize,
            dirty_bits: CheckedBitSet,
            visitor: &'a mut dyn MetadataVisitor,
        ) -> MappingEmitter<'a> {
            MappingEmitter {
                inner: Mutex::new(Inner {
                    visitor,
                    dirty_bits,
                    valid_mappings: FixedBitSet::with_capacity(nr_entries),
                }),
            }
        }

        pub fn get_valid(self) -> FixedBitSet {
            let inner = self.inner.into_inner().unwrap();
            inner.valid_mappings
        }
    }

    impl<'a> dump_utils::ArrayVisitor<Mapping> for MappingEmitter<'a> {
        fn visit(&self, index: u64, b: ArrayBlock<Mapping>) -> anyhow::Result<()> {
            let cbegin = index as u32 * b.header.max_entries;
            let cend = cbegin + b.header.nr_entries;
            for (map, cblock) in b.values.iter().zip(cbegin..cend) {
                if !map.is_valid() {
                    continue;
                }

                let mut inner = self.inner.lock().unwrap();
                let dirty = if let Some(bit) = inner.dirty_bits.contains(cblock as usize) {
                    bit
                } else {
                    // default to dirty if the bitset is damaged
                    true
                };
                let m = ir::Map {
                    cblock,
                    oblock: map.oblock,
                    dirty,
                };

                inner.valid_mappings.set(cblock as usize, true);
                inner.visitor.mapping(&m)?;
            }
            Ok(())
        }
    }
}

//-----------------------------------------

struct HintEmitter<'a> {
    emitter: Mutex<&'a mut dyn MetadataVisitor>,
    valid_mappings: FixedBitSet,
}

impl<'a> HintEmitter<'a> {
    pub fn new(emitter: &'a mut dyn MetadataVisitor, valid_mappings: FixedBitSet) -> HintEmitter {
        HintEmitter {
            emitter: Mutex::new(emitter),
            valid_mappings,
        }
    }
}

impl<'a> dump_utils::ArrayVisitor<Hint> for HintEmitter<'a> {
    fn visit(&self, index: u64, b: ArrayBlock<Hint>) -> anyhow::Result<()> {
        let cbegin = index as u32 * b.header.max_entries;
        let cend = cbegin + b.header.nr_entries;
        for (hint, cblock) in b.values.iter().zip(cbegin..cend) {
            if !self.valid_mappings.contains(cblock as usize) {
                continue;
            }

            let h = ir::Hint {
                cblock,
                data: hint.hint.to_vec(),
            };

            self.emitter.lock().unwrap().hint(&h)?;
        }

        Ok(())
    }
}

//------------------------------------------

fn dump_v1_mappings(
    engine: Arc<dyn IoEngine + Send + Sync>,
    out: &mut dyn MetadataVisitor,
    mapping_root: u64,
    cache_blocks: u32,
    repair: bool,
) -> anyhow::Result<FixedBitSet> {
    let ablocks = collect_array_blocks_with_path(engine.clone(), repair, mapping_root)?;
    let emitter = format1::MappingEmitter::new(cache_blocks as usize, out);
    walk_array_blocks(engine, ablocks, &emitter)?;

    Ok(emitter.get_valid())
}

fn dump_v2_mappings(
    engine: Arc<dyn IoEngine + Send + Sync>,
    out: &mut dyn MetadataVisitor,
    mapping_root: u64,
    cache_blocks: u32,
    dirty_root: Option<u64>,
    repair: bool,
) -> anyhow::Result<FixedBitSet> {
    // We need to walk the dirty bitset first.
    let dirty_bits = if let Some(root) = dirty_root {
        let (bits, errs) = read_bitset_checked(engine.clone(), root, cache_blocks as usize, repair);
        if errs.is_some() && !repair {
            return Err(anyhow!("errors in bitset {}", errs.unwrap()));
        }
        bits
    } else {
        // FIXME: is there a way this can legally happen?  eg,
        // a crash of a freshly created cache?
        return Err(anyhow!("format 2 selected, but no dirty bitset present"));
    };

    let ablocks = collect_array_blocks_with_path(engine.clone(), repair, mapping_root)?;
    let emitter = format2::MappingEmitter::new(cache_blocks as usize, dirty_bits, out);
    walk_array_blocks(engine, ablocks, &emitter)?;

    Ok(emitter.get_valid())
}

fn dump_hint_array(
    engine: Arc<dyn IoEngine + Send + Sync>,
    out: &mut dyn MetadataVisitor,
    hint_root: u64,
    valid_mappings: FixedBitSet,
    repair: bool,
) -> anyhow::Result<()> {
    let ablocks = collect_array_blocks_with_path(engine.clone(), repair, hint_root)?;
    let emitter = HintEmitter::new(out, valid_mappings);
    walk_array_blocks(engine, ablocks, &emitter)
}

//------------------------------------------

struct OutputVisitor<'a> {
    out: &'a mut dyn MetadataVisitor,
}

impl<'a> OutputVisitor<'a> {
    fn new(out: &'a mut dyn MetadataVisitor) -> Self {
        Self { out }
    }
}

impl<'a> MetadataVisitor for OutputVisitor<'a> {
    fn superblock_b(&mut self, sb: &ir::Superblock) -> anyhow::Result<ir::Visit> {
        output_context(self.out.superblock_b(sb))
    }

    fn superblock_e(&mut self) -> anyhow::Result<ir::Visit> {
        output_context(self.out.superblock_e())
    }

    fn mappings_b(&mut self) -> anyhow::Result<ir::Visit> {
        output_context(self.out.mappings_b())
    }

    fn mappings_e(&mut self) -> anyhow::Result<ir::Visit> {
        output_context(self.out.mappings_e())
    }

    fn mapping(&mut self, m: &ir::Map) -> anyhow::Result<ir::Visit> {
        output_context(self.out.mapping(m))
    }

    fn hints_b(&mut self) -> anyhow::Result<ir::Visit> {
        output_context(self.out.hints_b())
    }

    fn hints_e(&mut self) -> anyhow::Result<ir::Visit> {
        output_context(self.out.hints_e())
    }

    fn hint(&mut self, h: &ir::Hint) -> anyhow::Result<ir::Visit> {
        output_context(self.out.hint(h))
    }

    fn discards_b(&mut self) -> anyhow::Result<ir::Visit> {
        output_context(self.out.discards_b())
    }

    fn discards_e(&mut self) -> anyhow::Result<ir::Visit> {
        output_context(self.out.discards_e())
    }

    fn discard(&mut self, d: &ir::Discard) -> anyhow::Result<ir::Visit> {
        output_context(self.out.discard(d))
    }

    fn eof(&mut self) -> anyhow::Result<ir::Visit> {
        output_context(self.out.eof())
    }
}

//------------------------------------------

pub struct CacheDumpOptions<'a> {
    pub input: &'a Path,
    pub output: Option<&'a Path>,
    pub engine_opts: EngineOptions,
    pub repair: bool,
}

struct CacheDumpContext {
    engine: Arc<dyn IoEngine + Send + Sync>,
}

fn mk_context(opts: &CacheDumpOptions) -> anyhow::Result<CacheDumpContext> {
    let engine = EngineBuilder::new(opts.input, &opts.engine_opts).build()?;
    Ok(CacheDumpContext { engine })
}

pub fn dump_metadata(
    engine: Arc<dyn IoEngine + Send + Sync>,
    out: &mut dyn MetadataVisitor,
    sb: &Superblock,
    repair: bool,
) -> anyhow::Result<()> {
    let out: &mut dyn MetadataVisitor = &mut OutputVisitor::new(out);

    let xml_sb = ir::Superblock {
        uuid: "".to_string(),
        block_size: sb.data_block_size,
        nr_cache_blocks: sb.cache_blocks,
        policy: std::str::from_utf8(&sb.policy_name)?.to_string(),
        hint_width: sb.policy_hint_size,
    };
    out.superblock_b(&xml_sb)?;

    out.mappings_b()?;
    let valid_mappings = match sb.version {
        1 => dump_v1_mappings(
            engine.clone(),
            out,
            sb.mapping_root,
            sb.cache_blocks,
            repair,
        )?,
        2 => dump_v2_mappings(
            engine.clone(),
            out,
            sb.mapping_root,
            sb.cache_blocks,
            sb.dirty_root,
            repair,
        )?,
        v => {
            return Err(anyhow!("unsupported metadata version: {}", v));
        }
    };
    out.mappings_e()?;

    out.hints_b()?;
    dump_hint_array(engine, out, sb.hint_root, valid_mappings, repair)?;
    out.hints_e()?;

    out.superblock_e()?;
    out.eof()?;

    Ok(())
}

pub fn dump(opts: CacheDumpOptions) -> anyhow::Result<()> {
    let ctx = mk_context(&opts)?;
    let sb = read_superblock(ctx.engine.as_ref(), SUPERBLOCK_LOCATION)?;

    let writer: Box<dyn Write> = if opts.output.is_some() {
        let f = File::create(opts.output.unwrap()).context(OutputError)?;
        Box::new(BufWriter::new(f))
    } else {
        Box::new(BufWriter::new(std::io::stdout()))
    };
    let mut out = xml::XmlWriter::new(writer);

    dump_metadata(ctx.engine, &mut out, &sb, opts.repair)
}

//------------------------------------------
