use std::marker::PhantomData;
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::io_engine::{AsyncIoEngine, IoEngine, SyncIoEngine};
use crate::cache::hint::Hint;
use crate::cache::mapping::Mapping;
use crate::cache::superblock::*;
use crate::cache::xml::{self, MetadataVisitor};
use crate::pdata::array_walker::*;

//------------------------------------------

const MAX_CONCURRENT_IO: u32 = 1024;

//------------------------------------------

// TODO: pull out MetadataVisitor from the xml crate?
struct MappingEmitter {
    emitter: Arc<Mutex<dyn MetadataVisitor>>,
}

impl MappingEmitter {
    pub fn new(emitter: Arc<Mutex<dyn MetadataVisitor>>) -> MappingEmitter {
        MappingEmitter {
            emitter,
        }
    }
}

impl ArrayBlockVisitor<Mapping> for MappingEmitter {
    fn visit(&self, index: u64, m: Mapping) -> anyhow::Result<()> {
        if m.oblock == 0 {
            return Ok(());
        }

        // TODO: eliminate xml::Map?
        let m = xml::Map {
            cblock: index as u32,
            oblock: m.oblock,
            dirty: m.is_dirty(),
        };

        let mut emitter = self.emitter.lock().unwrap();
        emitter.mapping(&m)?;

        Ok(())
    }
}

//-----------------------------------------

struct HintEmitter<Width> {
    emitter: Arc<Mutex<dyn MetadataVisitor>>,
    _not_used: PhantomData<Width>,
}

impl<Width> HintEmitter<Width> {
    pub fn new(emitter: Arc<Mutex<dyn MetadataVisitor>>) -> HintEmitter<Width> {
        HintEmitter {
            emitter,
            _not_used: PhantomData,
        }
    }
}

impl<Width: typenum::Unsigned> ArrayBlockVisitor<Hint<Width>> for HintEmitter<Width> {
    fn visit(&self, index: u64, hint: Hint<Width>) -> anyhow::Result<()> {
        // TODO: skip invalid blocks

        let h = xml::Hint {
            cblock: index as u32,
            data: hint.hint.to_vec(),
        };

        let mut emitter = self.emitter.lock().unwrap();
        emitter.hint(&h)?;

        Ok(())
    }
}

//------------------------------------------

pub struct CacheDumpOptions<'a> {
    pub dev: &'a Path,
    pub async_io: bool,
    pub repair: bool,
}

// TODO: add report
struct Context {
    engine: Arc<dyn IoEngine + Send + Sync>,
}

fn mk_context(opts: &CacheDumpOptions) -> anyhow::Result<Context> {
    let engine: Arc<dyn IoEngine + Send + Sync>;

    if opts.async_io {
        engine = Arc::new(AsyncIoEngine::new(
            opts.dev,
            MAX_CONCURRENT_IO,
            false,
        )?);
    } else {
        let nr_threads = std::cmp::max(8, num_cpus::get() * 2);
        engine = Arc::new(SyncIoEngine::new(opts.dev, nr_threads, false)?);
    }

    Ok(Context {
        engine,
    })
}

fn dump_metadata(ctx: &Context, sb: &Superblock, _repair: bool) -> anyhow::Result<()>{
    let engine = &ctx.engine;

    let out = Arc::new(Mutex::new(xml::XmlWriter::new(std::io::stdout())));
    let xml_sb = xml::Superblock {
        uuid: "".to_string(),
        block_size: sb.data_block_size,
        nr_cache_blocks: sb.cache_blocks,
        policy: std::str::from_utf8(&sb.policy_name[..])?.to_string(),
        hint_width: sb.policy_hint_size,
    };
    out.lock().unwrap().superblock_b(&xml_sb)?;

    out.lock().unwrap().mappings_b()?;
    let w = ArrayWalker::new(engine.clone(), false);
    let emitter = Box::new(MappingEmitter::new(out.clone()));
    w.walk(emitter, sb.mapping_root)?;
    out.lock().unwrap().mappings_e()?;

    out.lock().unwrap().hints_b()?;
    type Width = typenum::U4; // FIXME: align with sb.policy_hint_size
    let emitter = Box::new(HintEmitter::<Width>::new(out.clone()));
    w.walk(emitter, sb.hint_root)?;
    out.lock().unwrap().hints_e()?;

    // FIXME: walk discards
    //out.lock().unwrap().discards_b()?;
    //out.lock().unwrap().discards_e()?;

    out.lock().unwrap().superblock_e()?;

    Ok(())
}

pub fn dump(opts: CacheDumpOptions) -> anyhow::Result<()> {
    let ctx = mk_context(&opts)?;
    let engine = &ctx.engine;
    let sb = read_superblock(engine.as_ref(), SUPERBLOCK_LOCATION)?;

    dump_metadata(&ctx, &sb, opts.repair)
}

//------------------------------------------
