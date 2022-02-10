use anyhow::anyhow;
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::cache::hint::*;
use crate::cache::mapping::*;
use crate::cache::superblock::*;
use crate::commands::utils::*;
use crate::io_engine::{AsyncIoEngine, IoEngine, SyncIoEngine};
use crate::pdata::array::{self, ArrayBlock, ArrayError};
use crate::pdata::array_walker::*;
use crate::pdata::bitset::*;
use crate::pdata::space_map::*;
use crate::pdata::space_map_checker::*;
use crate::pdata::space_map_common::*;
use crate::pdata::unpack::unpack;
use crate::report::*;

//------------------------------------------

const MAX_CONCURRENT_IO: u32 = 1024;

//------------------------------------------

fn inc_superblock(sm: &ASpaceMap) -> anyhow::Result<()> {
    let mut sm = sm.lock().unwrap();
    sm.inc(SUPERBLOCK_LOCATION, 1)?;
    Ok(())
}

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
                nr_origin_blocks: if let Some(n) = nr_origin_blocks {
                    n
                } else {
                    MAX_ORIGIN_BLOCKS
                },
                seen_oblocks: Mutex::new(BTreeSet::new()),
            }
        }

        fn check_flags(&self, m: &Mapping) -> array::Result<()> {
            if (m.flags & !(MappingFlags::Valid as u32 | MappingFlags::Dirty as u32)) != 0 {
                return Err(array::value_err(format!(
                    "unknown flags in mapping: {}",
                    m.flags
                )));
            }
            if !m.is_valid() && m.is_dirty() {
                return Err(array::value_err(
                    "dirty bit found on an unmapped block".to_string(),
                ));
            }
            Ok(())
        }

        fn check_oblock(&self, m: &Mapping) -> array::Result<()> {
            if !m.is_valid() {
                if m.oblock > 0 {
                    return Err(array::value_err("invalid block is mapped".to_string()));
                }
                return Ok(());
            }
            if m.oblock >= self.nr_origin_blocks {
                return Err(array::value_err(
                    "mapping beyond end of the origin device".to_string(),
                ));
            }
            let mut seen_oblocks = self.seen_oblocks.lock().unwrap();
            if seen_oblocks.contains(&m.oblock) {
                return Err(array::value_err("origin block already mapped".to_string()));
            }
            seen_oblocks.insert(m.oblock);
            Ok(())
        }
    }

    impl ArrayVisitor<Mapping> for MappingChecker {
        fn visit(&self, _index: u64, b: ArrayBlock<Mapping>) -> array::Result<()> {
            let mut errs: Vec<ArrayError> = Vec::new();

            for m in b.values.iter() {
                if let Err(e) = self.check_flags(m) {
                    errs.push(e);
                }
                if let Err(e) = self.check_oblock(m) {
                    errs.push(e);
                }
            }

            // FIXME: duplicate to BTreeWalker::build_aggregrate()
            match errs.len() {
                0 => Ok(()),
                1 => Err(errs[0].clone()),
                _ => Err(array::aggregate_error(errs)),
            }
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
        dirty_bits: CheckedBitSet,
    }

    impl MappingChecker {
        pub fn new(nr_origin_blocks: Option<u64>, dirty_bits: CheckedBitSet) -> MappingChecker {
            MappingChecker {
                nr_origin_blocks: if let Some(n) = nr_origin_blocks {
                    n
                } else {
                    MAX_ORIGIN_BLOCKS
                },
                inner: Mutex::new(Inner {
                    seen_oblocks: BTreeSet::new(),
                    dirty_bits,
                }),
            }
        }

        fn check_flags(&self, m: &Mapping, dirty_bit: Option<bool>) -> array::Result<()> {
            if (m.flags & !(MappingFlags::Valid as u32)) != 0 {
                return Err(array::value_err(format!(
                    "unknown flags in mapping: {}",
                    m.flags
                )));
            }
            if !m.is_valid() && dirty_bit.is_some() && dirty_bit.unwrap() {
                return Err(array::value_err(
                    "dirty bit found on an unmapped block".to_string(),
                ));
            }
            Ok(())
        }

        fn check_oblock(&self, m: &Mapping, seen_oblocks: &mut BTreeSet<u64>) -> array::Result<()> {
            if !m.is_valid() {
                if m.oblock > 0 {
                    return Err(array::value_err("invalid mapped block".to_string()));
                }
                return Ok(());
            }
            if m.oblock >= self.nr_origin_blocks {
                return Err(array::value_err(
                    "mapping beyond end of the origin device".to_string(),
                ));
            }
            if seen_oblocks.contains(&m.oblock) {
                return Err(array::value_err("origin block already mapped".to_string()));
            }
            seen_oblocks.insert(m.oblock);

            Ok(())
        }
    }

    impl ArrayVisitor<Mapping> for MappingChecker {
        fn visit(&self, index: u64, b: ArrayBlock<Mapping>) -> array::Result<()> {
            let mut inner = self.inner.lock().unwrap();
            let mut errs: Vec<ArrayError> = Vec::new();

            let cbegin = index as u32 * b.header.max_entries;
            let cend = cbegin + b.header.nr_entries;
            for (m, cblock) in b.values.iter().zip(cbegin..cend) {
                if let Err(e) = self.check_flags(m, inner.dirty_bits.contains(cblock as usize)) {
                    errs.push(e);
                }
                if let Err(e) = self.check_oblock(m, &mut inner.seen_oblocks) {
                    errs.push(e);
                }
            }

            // FIXME: duplicate to BTreeWalker::build_aggregrate()
            match errs.len() {
                0 => Ok(()),
                1 => Err(errs[0].clone()),
                _ => Err(array::aggregate_error(errs)),
            }
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
    fn visit(&self, _index: u64, _b: ArrayBlock<Hint>) -> array::Result<()> {
        // TODO: check hints
        Ok(())
    }
}

//------------------------------------------

// TODO: clear_needs_check, auto_repair
pub struct CacheCheckOptions<'a> {
    pub dev: &'a Path,
    pub async_io: bool,
    pub sb_only: bool,
    pub skip_mappings: bool,
    pub skip_hints: bool,
    pub skip_discards: bool,
    pub ignore_non_fatal: bool,
    pub auto_repair: bool,
    pub report: Arc<Report>,
}

// TODO: thread pool
struct Context {
    report: Arc<Report>,
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

    Ok(Context {
        report: opts.report.clone(),
        engine,
    })
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
    let metadata_sm = core_sm(engine.get_nr_blocks(), u8::MAX as u32);
    inc_superblock(&metadata_sm)?;

    let sb = match read_superblock(engine.as_ref(), SUPERBLOCK_LOCATION) {
        Ok(sb) => sb,
        Err(e) => {
            check_not_xml(opts.dev, &opts.report);
            return Err(e);
        }
    };

    check_superblock(&sb)?;

    if opts.sb_only {
        return Ok(());
    }

    // The discard bitset is optional and could be updated during device suspension.
    // A restored metadata therefore comes with a zero-sized discard bitset,
    // and also zeroed discard_block_size and discard_nr_blocks.
    let nr_origin_blocks;
    if sb.flags.clean_shutdown && sb.discard_block_size > 0 && sb.discard_nr_blocks > 0 {
        let origin_sectors = sb.discard_block_size * sb.discard_nr_blocks;
        nr_origin_blocks = Some(origin_sectors / sb.data_block_size as u64);
    } else {
        nr_origin_blocks = None;
    }

    // TODO: factor out into check_mappings()
    if !opts.skip_mappings {
        let w =
            ArrayWalker::new_with_sm(engine.clone(), metadata_sm.clone(), opts.ignore_non_fatal)?;
        match sb.version {
            1 => {
                let mut c = format1::MappingChecker::new(nr_origin_blocks);
                if let Err(e) = w.walk(&mut c, sb.mapping_root) {
                    ctx.report.fatal(&format!("{}", e));
                }
            }
            2 => {
                let (dirty_bits, err) = read_bitset_with_sm(
                    engine.clone(),
                    sb.dirty_root.unwrap(),
                    sb.cache_blocks as usize,
                    metadata_sm.clone(),
                    opts.ignore_non_fatal,
                )?;
                if err.is_some() {
                    ctx.report.fatal(&format!("{}", err.unwrap()));
                }
                let mut c = format2::MappingChecker::new(nr_origin_blocks, dirty_bits);
                if let Err(e) = w.walk(&mut c, sb.mapping_root) {
                    ctx.report.fatal(&format!("{}", e));
                }
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
        let w =
            ArrayWalker::new_with_sm(engine.clone(), metadata_sm.clone(), opts.ignore_non_fatal)?;
        let mut c = HintChecker::new();
        if let Err(e) = w.walk(&mut c, sb.hint_root) {
            ctx.report.fatal(&format!("{}", e));
        }
    }

    // The discard bitset might not be available if the cache has never been suspended,
    // e.g., a crash of freshly created cache.
    if !opts.skip_discards && sb.discard_root != 0 {
        let (_discard_bits, err) = read_bitset_with_sm(
            engine.clone(),
            sb.discard_root,
            sb.discard_nr_blocks as usize,
            metadata_sm.clone(),
            opts.ignore_non_fatal,
        )?;
        if err.is_some() {
            ctx.report.fatal(&format!("{}", err.unwrap()));
        }
    }

    let root = unpack::<SMRoot>(&sb.metadata_sm_root[0..])?;
    let metadata_leaks = check_metadata_space_map(
        engine.clone(),
        ctx.report.clone(),
        root,
        metadata_sm.clone(),
        opts.ignore_non_fatal,
    )?;

    if opts.auto_repair && !metadata_leaks.is_empty() {
        ctx.report.info("Repairing metadata leaks.");
        repair_space_map(ctx.engine.clone(), metadata_leaks, metadata_sm.clone())?;
    }

    Ok(())
}

//------------------------------------------
