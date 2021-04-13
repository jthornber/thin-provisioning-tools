use anyhow::anyhow;
use fixedbitset::FixedBitSet;
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::cache::hint::*;
use crate::cache::mapping::*;
use crate::cache::superblock::*;
use crate::io_engine::{AsyncIoEngine, IoEngine, SyncIoEngine};
use crate::pdata::array;
use crate::pdata::array_walker::*;
use crate::pdata::bitset_walker::*;

//------------------------------------------

const MAX_CONCURRENT_IO: u32 = 1024;

//------------------------------------------

mod format1 {
    use super::*;

    pub struct MappingChecker {
        nr_origin_blocks: u64,
        seen_oblocks: Mutex<BTreeSet<u64>>,
    }

    impl MappingChecker {
        pub fn new(nr_origin_blocks: Option<u64>) -> MappingChecker {
            MappingChecker {
                nr_origin_blocks: if let Some(n) = nr_origin_blocks {n} else {MAX_ORIGIN_BLOCKS},
                seen_oblocks: Mutex::new(BTreeSet::new()),
            }
        }

        fn check_flags(&self, m: &Mapping) -> array::Result<()> {
            if (m.flags & !(MappingFlags::Valid as u32 | MappingFlags::Dirty as u32)) != 0 {
                return Err(array::value_err(format!("unknown flags in mapping: {}", m.flags)));
            }
            if !m.is_valid() && m.is_dirty() {
                return Err(array::value_err(format!("dirty bit found on an unmapped block")));
            }
            Ok(())
        }

        fn check_oblock(&self, m: &Mapping) -> array::Result<()> {
            if !m.is_valid() {
                if m.oblock > 0 {
                    return Err(array::value_err(format!("invalid block is mapped")));
                }
                return Ok(());
            }
            if m.oblock >= self.nr_origin_blocks {
                return Err(array::value_err(format!("mapping beyond end of the origin device")));
            }

            let mut seen_oblocks = self.seen_oblocks.lock().unwrap();
            if seen_oblocks.contains(&m.oblock) {
                return Err(array::value_err(format!("origin block already mapped")));
            }
            seen_oblocks.insert(m.oblock);

            Ok(())
        }
    }

    impl ArrayVisitor<Mapping> for MappingChecker {
        fn visit(&self, _index: u64, m: Mapping) -> array::Result<()> {
            self.check_flags(&m)?;
            self.check_oblock(&m)?;

            Ok(())
        }
    }
}

mod format2 {
    use super::*;

    pub struct MappingChecker {
        nr_origin_blocks: u64,
        inner: Mutex<Inner>,
    }

    struct Inner {
        seen_oblocks: BTreeSet<u64>,
        dirty_bits: FixedBitSet,
    }

    impl MappingChecker {
        pub fn new(nr_origin_blocks: Option<u64>, dirty_bits: FixedBitSet) -> MappingChecker {
            MappingChecker {
                nr_origin_blocks: if let Some(n) = nr_origin_blocks {n} else {MAX_ORIGIN_BLOCKS},
                inner: Mutex::new(Inner {
                    seen_oblocks: BTreeSet::new(),
                    dirty_bits,
                }),
            }
        }

        fn check_flags(&self, m: &Mapping, dirty_bit: bool) -> array::Result<()> {
            if (m.flags & !(MappingFlags::Valid as u32)) != 0 {
                return Err(array::value_err(format!("unknown flags in mapping: {}", m.flags)));
            }
            if !m.is_valid() && dirty_bit {
                return Err(array::value_err(format!("dirty bit found on an unmapped block")));
            }
            Ok(())
        }

        fn check_oblock(&self, m: &Mapping, seen_oblocks: &mut BTreeSet<u64>) -> array::Result<()> {
            if !m.is_valid() {
                if m.oblock > 0 {
                    return Err(array::value_err(format!("invalid mapped block")));
                }
                return Ok(());
            }
            if m.oblock >= self.nr_origin_blocks {
                return Err(array::value_err(format!("mapping beyond end of the origin device")));
            }
            if seen_oblocks.contains(&m.oblock) {
                return Err(array::value_err(format!("origin block already mapped")));
            }
            seen_oblocks.insert(m.oblock);

            Ok(())
        }
    }

    impl ArrayVisitor<Mapping> for MappingChecker {
        fn visit(&self, index: u64, m: Mapping) -> array::Result<()> {
            let mut inner = self.inner.lock().unwrap();
            self.check_flags(&m, inner.dirty_bits.contains(index as usize))?;
            self.check_oblock(&m, &mut inner.seen_oblocks)?;

            Ok(())
        }
    }
}

//------------------------------------------

struct HintChecker;

impl HintChecker {
    fn new() -> HintChecker {
        HintChecker
    }
}

impl ArrayVisitor<Hint> for HintChecker {
    fn visit(&self, _index: u64, _hint: Hint) -> array::Result<()> {
        // TODO: check hints
        Ok(())
    }
}

//------------------------------------------

// TODO: ignore_non_fatal, clear_needs_check, auto_repair
pub struct CacheCheckOptions<'a> {
    pub dev: &'a Path,
    pub async_io: bool,
    pub sb_only: bool,
    pub skip_mappings: bool,
    pub skip_hints: bool,
    pub skip_discards: bool,
}

// TODO: thread pool, report
struct Context {
    engine: Arc<dyn IoEngine + Send + Sync>,
}

fn mk_context(opts: &CacheCheckOptions) -> anyhow::Result<Context> {
    let engine: Arc<dyn IoEngine + Send + Sync>;

    if opts.async_io {
        engine = Arc::new(AsyncIoEngine::new(opts.dev, MAX_CONCURRENT_IO, false)?);
    } else {
        let nr_threads = std::cmp::max(8, num_cpus::get() * 2);
        engine = Arc::new(SyncIoEngine::new(opts.dev, nr_threads, false)?);
    }

    Ok(Context { engine })
}

fn check_superblock(sb: &Superblock) -> anyhow::Result<()> {
    if sb.version >= 2 && sb.dirty_root == None {
        return Err(anyhow!("dirty bitset not found"));
    }
    Ok(())
}

pub fn check(opts: CacheCheckOptions) -> anyhow::Result<()> {
    let ctx = mk_context(&opts)?;

    let engine = &ctx.engine;

    let sb = read_superblock(engine.as_ref(), SUPERBLOCK_LOCATION)?;
    check_superblock(&sb)?;

    if opts.sb_only {
        return Ok(());
    }

    let nr_origin_blocks;
    if sb.flags.clean_shutdown {
        let origin_sectors = sb.discard_block_size * sb.discard_nr_blocks;
        nr_origin_blocks = Some(origin_sectors / sb.data_block_size as u64);
    } else {
        nr_origin_blocks = None;
    }

    // TODO: factor out into check_mappings()
    if !opts.skip_mappings {
        let w = ArrayWalker::new(engine.clone(), false);
        match sb.version {
            1 => {
                let mut c = format1::MappingChecker::new(nr_origin_blocks);
                w.walk(&mut c, sb.mapping_root)?;
            }
            2 => {
                // FIXME: possibly size truncation on 32-bit targets
                let mut dirty_bits = FixedBitSet::with_capacity(sb.cache_blocks as usize);
                // TODO: allow ignore_none_fatal
                read_bitset(engine.clone(), sb.dirty_root.unwrap(), false, &mut dirty_bits)?;
                let mut c = format2::MappingChecker::new(nr_origin_blocks, dirty_bits);
                w.walk(&mut c, sb.mapping_root)?;
            }
            v => {
                return Err(anyhow!("unsupported metadata version {}", v));
            }
        }
    }

    if !opts.skip_hints && sb.hint_root != 0 && sb.policy_hint_size != 0 {
        if sb.policy_hint_size != 4 {
            return Err(anyhow!("cache_check only supports policy hint size of 4"));
        }
        let w = ArrayWalker::new(engine.clone(), false);
        let mut c = HintChecker::new();
        w.walk(&mut c, sb.hint_root)?;
    }

    // The discard bitset might not be available if the cache has never been suspended,
    // e.g., a crash of freshly created cache.
    if !opts.skip_discards && sb.discard_root != 0 {
        let mut discard_bits = FixedBitSet::with_capacity(sb.discard_nr_blocks as usize);
        read_bitset(engine.clone(), sb.discard_root, false, &mut discard_bits)?;
    }

    Ok(())
}

//------------------------------------------
