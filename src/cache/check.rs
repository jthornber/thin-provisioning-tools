use anyhow::anyhow;
use std::collections::*;
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::cache::hint::*;
use crate::cache::mapping::*;
use crate::cache::superblock::*;
use crate::io_engine::{AsyncIoEngine, IoEngine, SyncIoEngine};
use crate::pdata::array_walker::*;

//------------------------------------------

const MAX_CONCURRENT_IO: u32 = 1024;

//------------------------------------------

struct CheckMappingVisitor {
    metadata_version: u32,
    allowed_flags: u32,
    nr_origin_blocks: u64,
    seen_oblocks: Arc<Mutex<BTreeSet<u64>>>,
    //dirty_iterator: Option<BitsetIterator>,
}

impl CheckMappingVisitor {
    fn new(metadata_version: u32, nr_origin_blocks: Option<u64>, dirty_root: Option<u64>) -> CheckMappingVisitor {
        let mut flags: u32 = MappingFlags::Valid as u32;
        //let dirty_iterator;
        if metadata_version == 1 {
            flags |= MappingFlags::Dirty as u32;
            //dirty_iterator = None;
        } else {
            let _b = dirty_root.expect("dirty bitset unavailable");
            //dirty_iterator = Some(BitsetIterator::new(b));
        }

        CheckMappingVisitor {
            metadata_version,
            allowed_flags: flags,
            nr_origin_blocks: if let Some(n) = nr_origin_blocks {n} else {MAX_ORIGIN_BLOCKS},
            seen_oblocks: Arc::new(Mutex::new(BTreeSet::new())),
            //dirty_iterator,
        }
    }

    // TODO: move to ctor of Mapping?
    fn check_flags(&self, m: &Mapping) -> anyhow::Result<()> {
        if (m.flags & !self.allowed_flags) != 0 {
            return Err(anyhow!("unknown flags in mapping"));
        }

        if !m.is_valid() {
            if self.metadata_version == 1 {
                if m.is_dirty() {
                    return Err(anyhow!("dirty bit found on an unmapped block"));
                }
            }/*else if dirty_iterator.expect("dirty bitset unavailable").next() {
                return Err(anyhow!("dirty bit found on an unmapped block"));
            }*/
        }

        Ok(())
    }

    fn check_oblock(&self, m: &Mapping) -> anyhow::Result<()> {
        if !m.is_valid() {
            if m.oblock > 0 {
                return Err(anyhow!("invalid mapped block"));
            }
            return Ok(());
        }

        if m.oblock >= self.nr_origin_blocks {
            return Err(anyhow!("mapping beyond end of the origin device"));
        }

        let mut seen_oblocks = self.seen_oblocks.lock().unwrap();
        if seen_oblocks.contains(&m.oblock) {
            return Err(anyhow!("origin block already mapped"));
        }
        seen_oblocks.insert(m.oblock);

        Ok(())
    }
}

impl ArrayVisitor<Mapping> for CheckMappingVisitor {
    fn visit(&self, _index: u64, m: Mapping) -> anyhow::Result<()> {
        self.check_flags(&m)?;
        self.check_oblock(&m)?;

        Ok(())
    }
}

//------------------------------------------

struct CheckHintVisitor;

impl CheckHintVisitor {
    fn new() -> CheckHintVisitor {
        CheckHintVisitor
    }
}

impl ArrayVisitor<Hint> for CheckHintVisitor {
    fn visit(&self, _index: u64, _hint: Hint) -> anyhow::Result<()> {
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
        let mut c = CheckMappingVisitor::new(sb.version, nr_origin_blocks, sb.dirty_root);
        w.walk(&mut c, sb.mapping_root)?;

        if sb.version >= 2 {
            // TODO: check dirty bitset
        }
    }

    if !opts.skip_hints && sb.hint_root != 0 && sb.policy_hint_size != 0 {
        if sb.policy_hint_size != 4 {
            return Err(anyhow!("cache_check only supports policy hint size of 4"));
        }
        let w = ArrayWalker::new(engine.clone(), false);
        let mut c = CheckHintVisitor::new();
        w.walk(&mut c, sb.hint_root)?;
    }

    if !opts.skip_discards {
        // TODO: check discard bitset
    }

    Ok(())
}

//------------------------------------------
