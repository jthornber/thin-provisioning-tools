use std::fs::File;
use std::io::BufWriter;
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::era::ir::{self, MetadataVisitor};
use crate::era::superblock::*;
use crate::era::writeset::Writeset;
use crate::era::xml;
use crate::io_engine::{AsyncIoEngine, IoEngine, SyncIoEngine};
use crate::pdata::array::{self, ArrayBlock};
use crate::pdata::array_walker::*;
use crate::pdata::bitset::read_bitset_no_err;
use crate::pdata::btree_walker::btree_to_map;

//------------------------------------------

const MAX_CONCURRENT_IO: u32 = 1024;

//-----------------------------------------

struct EraEmitter<'a> {
    emitter: Mutex<&'a mut dyn MetadataVisitor>,
}

impl<'a> EraEmitter<'a> {
    pub fn new(emitter: &'a mut dyn MetadataVisitor) -> EraEmitter {
        EraEmitter {
            emitter: Mutex::new(emitter),
        }
    }
}

impl<'a> ArrayVisitor<u32> for EraEmitter<'a> {
    fn visit(&self, index: u64, b: ArrayBlock<u32>) -> array::Result<()> {
        let begin = index as u32 * b.header.max_entries;
        let end = begin + b.header.nr_entries;
        for (v, block) in b.values.iter().zip(begin..end) {
            let era = ir::Era { block, era: *v };

            self.emitter
                .lock()
                .unwrap()
                .era(&era)
                .map_err(|e| array::value_err(format!("{}", e)))?;
        }

        Ok(())
    }
}

//------------------------------------------

pub struct EraDumpOptions<'a> {
    pub input: &'a Path,
    pub output: Option<&'a Path>,
    pub async_io: bool,
    pub logical: bool,
    pub repair: bool,
}

struct Context {
    engine: Arc<dyn IoEngine + Send + Sync>,
}

fn mk_context(opts: &EraDumpOptions) -> anyhow::Result<Context> {
    let engine: Arc<dyn IoEngine + Send + Sync>;

    if opts.async_io {
        engine = Arc::new(AsyncIoEngine::new(opts.input, MAX_CONCURRENT_IO, false)?);
    } else {
        let nr_threads = std::cmp::max(8, num_cpus::get() * 2);
        engine = Arc::new(SyncIoEngine::new(opts.input, nr_threads, false)?);
    }

    Ok(Context { engine })
}

// notify the visitor about the marked blocks only
fn dump_writeset(
    engine: Arc<dyn IoEngine + Send + Sync>,
    out: &mut dyn MetadataVisitor,
    era: u32,
    ws: &Writeset,
    repair: bool,
) -> anyhow::Result<()> {
    // TODO: deal with broken writeset
    let bits = read_bitset_no_err(engine.clone(), ws.root, ws.nr_bits as usize, repair)?;

    out.writeset_b(&ir::Writeset {
        era,
        nr_bits: ws.nr_bits,
    })?;

    // [begin, end) denotes the range of set bits.
    let mut begin: u32 = 0;
    let mut end: u32 = 0;
    for (index, entry) in bits.as_slice().iter().enumerate() {
        let mut n = *entry;

        if n == u32::MAX {
            end = std::cmp::min(end + 32, ws.nr_bits);
            continue;
        }

        while n > 0 {
            let zeros = n.trailing_zeros();
            if zeros > 0 {
                if end > begin {
                    let m = ir::MarkedBlocks {
                        begin,
                        len: end - begin,
                    };
                    out.writeset_blocks(&m)?;
                }
                n >>= zeros;
                end += zeros;
                begin = end;
            }

            let ones = n.trailing_ones();
            n >>= ones;
            end = std::cmp::min(end + ones, ws.nr_bits);
        }

        // emit the range if it ends before the entry boundary
        let endpos = ((index as u32) << 5) + 32;
        if end < endpos {
            if end > begin {
                let m = ir::MarkedBlocks {
                    begin,
                    len: end - begin,
                };
                out.writeset_blocks(&m)?;
            }
            begin = endpos;
            end = begin;
        }
    }

    if end > begin {
        let m = ir::MarkedBlocks {
            begin,
            len: end - begin,
        };
        out.writeset_blocks(&m)?;
    }

    out.writeset_e()?;

    Ok(())
}

pub fn dump_metadata(
    engine: Arc<dyn IoEngine + Send + Sync>,
    out: &mut dyn MetadataVisitor,
    sb: &Superblock,
    repair: bool,
) -> anyhow::Result<()> {
    let xml_sb = ir::Superblock {
        uuid: "".to_string(),
        block_size: sb.data_block_size,
        nr_blocks: sb.nr_blocks,
        current_era: sb.current_era,
    };
    out.superblock_b(&xml_sb)?;

    let mut path = vec![0];
    let writesets =
        btree_to_map::<Writeset>(&mut path, engine.clone(), repair, sb.writeset_tree_root)?;
    for (era, ws) in writesets.iter() {
        dump_writeset(engine.clone(), out, *era as u32, ws, repair)?;
    }

    out.era_b()?;
    let w = ArrayWalker::new(engine.clone(), repair);
    let mut emitter = EraEmitter::new(out);
    w.walk(&mut emitter, sb.era_array_root)?;
    out.era_e()?;

    out.superblock_e()?;
    out.eof()?;

    Ok(())
}

pub fn dump_metadata_logical(
    _engine: Arc<dyn IoEngine + Send + Sync>,
    _out: &mut dyn MetadataVisitor,
    _sb: &Superblock,
    _repair: bool,
) -> anyhow::Result<()> {
    // TODO
    Ok(())
}

pub fn dump(opts: EraDumpOptions) -> anyhow::Result<()> {
    let ctx = mk_context(&opts)?;
    let sb = read_superblock(ctx.engine.as_ref(), SUPERBLOCK_LOCATION)?;

    let writer: Box<dyn Write>;
    if opts.output.is_some() {
        writer = Box::new(BufWriter::new(File::create(opts.output.unwrap())?));
    } else {
        writer = Box::new(BufWriter::new(std::io::stdout()));
    }
    let mut out = xml::XmlWriter::new(writer, false);

    if opts.logical {
        dump_metadata_logical(ctx.engine, &mut out, &sb, opts.repair)
    } else {
        dump_metadata(ctx.engine, &mut out, &sb, opts.repair)
    }
}

//------------------------------------------
