use anyhow::anyhow;
use fixedbitset::FixedBitSet;
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::cache::hint::Hint;
use crate::cache::mapping::Mapping;
use crate::cache::superblock::*;
use crate::cache::xml::{self, MetadataVisitor};
use crate::io_engine::{AsyncIoEngine, IoEngine, SyncIoEngine};
use crate::pdata::array::{self, ArrayBlock};
use crate::pdata::array_walker::*;

//------------------------------------------

const MAX_CONCURRENT_IO: u32 = 1024;

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

    impl<'a> ArrayVisitor<Mapping> for MappingEmitter<'a> {
        fn visit(&self, index: u64, b: ArrayBlock<Mapping>) -> array::Result<()> {
            let cbegin = index as u32 * b.header.max_entries;
            let cend = cbegin + b.header.nr_entries;
            for (map, cblock) in b.values.iter().zip(cbegin..cend) {
                if !map.is_valid() {
                    continue;
                }

                let m = xml::Map {
                    cblock,
                    oblock: map.oblock,
                    dirty: map.is_dirty(),
                };

                let mut inner = self.inner.lock().unwrap();
                inner.valid_mappings.set(cblock as usize, true);
                inner
                    .visitor
                    .mapping(&m)
                    .map_err(|e| array::value_err(format!("{}", e)))?;
            }

            Ok(())
        }
    }
}

//------------------------------------------

mod format2 {
    use super::*;

    //-------------------
    // Dirty bitset visitor
    pub struct DirtyVisitor {
        nr_entries: usize,
        bits: Mutex<FixedBitSet>,
    }

    impl DirtyVisitor {
        pub fn new(nr_entries: usize) -> Self {
            DirtyVisitor {
                nr_entries, // number of bits
                bits: Mutex::new(FixedBitSet::with_capacity(nr_entries)),
            }
        }

        pub fn get_bits(self) -> FixedBitSet {
            self.bits.into_inner().unwrap()
        }
    }

    impl ArrayVisitor<u64> for DirtyVisitor {
        fn visit(&self, index: u64, b: ArrayBlock<u64>) -> array::Result<()> {
            let mut pos = (index as usize * (b.header.max_entries as usize)) << 6;
            for bits in b.values.iter() {
                for bi in 0..64u64 {
                    if pos >= self.nr_entries {
                        break;
                    }

                    self.bits.lock().unwrap().set(pos, bits & (1 << bi) != 0);
                    pos += 1;
                }
            }
            Ok(())
        }
    }

    //-------------------
    // Mapping visitor

    struct Inner<'a> {
        visitor: &'a mut dyn MetadataVisitor,
        dirty_bits: FixedBitSet,
        valid_mappings: FixedBitSet,
    }

    pub struct MappingEmitter<'a> {
        inner: Mutex<Inner<'a>>,
    }

    impl<'a> MappingEmitter<'a> {
        pub fn new(
            nr_entries: usize,
            dirty_bits: FixedBitSet,
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

    impl<'a> ArrayVisitor<Mapping> for MappingEmitter<'a> {
        fn visit(&self, index: u64, b: ArrayBlock<Mapping>) -> array::Result<()> {
            let cbegin = index as u32 * b.header.max_entries;
            let cend = cbegin + b.header.nr_entries;
            for (map, cblock) in b.values.iter().zip(cbegin..cend) {
                if !map.is_valid() {
                    continue;
                }

                let mut inner = self.inner.lock().unwrap();
                let dirty = inner.dirty_bits.contains(cblock as usize);
                let m = xml::Map {
                    cblock,
                    oblock: map.oblock,
                    dirty,
                };

                inner.valid_mappings.set(cblock as usize, true);
                inner
                    .visitor
                    .mapping(&m)
                    .map_err(|e| array::value_err(format!("{}", e)))?;
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

impl<'a> ArrayVisitor<Hint> for HintEmitter<'a> {
    fn visit(&self, index: u64, b: ArrayBlock<Hint>) -> array::Result<()> {
        let cbegin = index as u32 * b.header.max_entries;
        let cend = cbegin + b.header.nr_entries;
        for (hint, cblock) in b.values.iter().zip(cbegin..cend) {
            if !self.valid_mappings.contains(cblock as usize) {
                continue;
            }

            let h = xml::Hint {
                cblock,
                data: hint.hint.to_vec(),
            };

            self.emitter
                .lock()
                .unwrap()
                .hint(&h)
                .map_err(|e| array::value_err(format!("{}", e)))?;
        }

        Ok(())
    }
}

//------------------------------------------

pub struct CacheDumpOptions<'a> {
    pub dev: &'a Path,
    pub async_io: bool,
    pub repair: bool,
}

struct Context {
    engine: Arc<dyn IoEngine + Send + Sync>,
}

fn mk_context(opts: &CacheDumpOptions) -> anyhow::Result<Context> {
    let engine: Arc<dyn IoEngine + Send + Sync>;

    if opts.async_io {
        engine = Arc::new(AsyncIoEngine::new(opts.dev, MAX_CONCURRENT_IO, false)?);
    } else {
        let nr_threads = std::cmp::max(8, num_cpus::get() * 2);
        engine = Arc::new(SyncIoEngine::new(opts.dev, nr_threads, false)?);
    }

    Ok(Context { engine })
}

fn dump_metadata(ctx: &Context, sb: &Superblock, _repair: bool) -> anyhow::Result<()> {
    let engine = &ctx.engine;

    let mut out = xml::XmlWriter::new(std::io::stdout());
    let xml_sb = xml::Superblock {
        uuid: "".to_string(),
        block_size: sb.data_block_size,
        nr_cache_blocks: sb.cache_blocks,
        policy: std::str::from_utf8(&sb.policy_name)?.to_string(),
        hint_width: sb.policy_hint_size,
    };
    out.superblock_b(&xml_sb)?;

    out.mappings_b()?;
    let valid_mappings = match sb.version {
        1 => {
            let w = ArrayWalker::new(engine.clone(), false);
            let mut emitter = format1::MappingEmitter::new(sb.cache_blocks as usize, &mut out);
            w.walk(&mut emitter, sb.mapping_root)?;
            emitter.get_valid()
        }
        2 => {
            // We need to walk the dirty bitset first.
            let w = ArrayWalker::new(engine.clone(), false);
            let mut v = format2::DirtyVisitor::new(sb.cache_blocks as usize);

            if let Some(root) = sb.dirty_root {
                w.walk(&mut v, root)?;
            } else {
                // FIXME: is there a way this can legally happen?  eg,
                // a crash of a freshly created cache?
                return Err(anyhow!("format 2 selected, but no dirty bitset present"));
            }
            let dirty_bits = v.get_bits();

            let w = ArrayWalker::new(engine.clone(), false);
            let mut emitter =
                format2::MappingEmitter::new(sb.cache_blocks as usize, dirty_bits, &mut out);
            w.walk(&mut emitter, sb.mapping_root)?;
            emitter.get_valid()
        }
        v => {
            return Err(anyhow!("unsupported metadata version: {}", v));
        }
    };
    out.mappings_e()?;

    out.hints_b()?;
    {
        let w = ArrayWalker::new(engine.clone(), false);
        let mut emitter = HintEmitter::new(&mut out, valid_mappings);
        w.walk(&mut emitter, sb.hint_root)?;
    }
    out.hints_e()?;

    out.superblock_e()?;

    Ok(())
}

pub fn dump(opts: CacheDumpOptions) -> anyhow::Result<()> {
    let ctx = mk_context(&opts)?;
    let engine = &ctx.engine;
    let sb = read_superblock(engine.as_ref(), SUPERBLOCK_LOCATION)?;

    dump_metadata(&ctx, &sb, opts.repair)
}

//------------------------------------------
