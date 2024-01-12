use anyhow::{anyhow, Context, Result};
use fixedbitset::FixedBitSet;
use std::convert::TryFrom;
use std::fs::File;
use std::io::BufWriter;
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::commands::engine::*;
use crate::dump_utils::*;
use crate::era::ir::{self, MetadataVisitor};
use crate::era::superblock::*;
use crate::era::writeset::Writeset;
use crate::era::xml;
use crate::io_engine::*;
use crate::pdata::array::{self, ArrayBlock};
use crate::pdata::array_walker::*;
use crate::pdata::bitset::read_bitset;
use crate::pdata::btree_walker::btree_to_map;

//------------------------------------------

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

trait Archive {
    fn set(&mut self, key: u32, value: u32) -> Result<()>;
    fn get(&self, key: u32) -> Option<u32>;
}

// In-core archive of writeset eras.
// The actual era for a given block is `digested_era + deltas[b]` if `deltas[b]` is non-zero.
struct EraArchive<T> {
    digested_era: u32, // maximum possible era in the era array
    deltas: Vec<T>,
}

fn new_era_archive(nr_blocks: u32, archived_begin: u32, nr_writesets: u32) -> Box<dyn Archive> {
    match nr_writesets + 1 {
        0..=255 => Box::new(EraArchive {
            digested_era: archived_begin.wrapping_sub(1),
            deltas: vec![0u8; nr_blocks as usize],
        }),
        256..=65535 => Box::new(EraArchive {
            digested_era: archived_begin.wrapping_sub(1),
            deltas: vec![0u16; nr_blocks as usize],
        }),
        _ => Box::new(EraArchive {
            digested_era: archived_begin.wrapping_sub(1),
            deltas: vec![0u32; nr_blocks as usize],
        }),
    }
}

impl<T: std::convert::TryFrom<u32>> Archive for EraArchive<T>
where
    T: Copy + Into<u32> + TryFrom<u32>,
    <T as TryFrom<u32>>::Error: std::fmt::Debug,
{
    fn set(&mut self, block: u32, delta: u32) -> Result<()> {
        self.deltas[block as usize] = T::try_from(delta).unwrap();
        Ok(())
    }

    fn get(&self, block: u32) -> Option<u32> {
        if let Some(&delta) = self.deltas.get(block as usize) {
            let d: u32 = delta.into();
            if d == 0 {
                None
            } else {
                Some(self.digested_era.wrapping_add(d))
            }
        } else {
            None
        }
    }
}

//------------------------------------------

struct Inner<'a> {
    emitter: &'a mut dyn MetadataVisitor,
    era_archive: &'a dyn Archive,
}

struct LogicalEraEmitter<'a> {
    inner: Mutex<Inner<'a>>,
}

impl<'a> LogicalEraEmitter<'a> {
    pub fn new(
        emitter: &'a mut dyn MetadataVisitor,
        era_archive: &'a dyn Archive,
    ) -> LogicalEraEmitter<'a> {
        LogicalEraEmitter {
            inner: Mutex::new(Inner {
                emitter,
                era_archive,
            }),
        }
    }
}

impl<'a> ArrayVisitor<u32> for LogicalEraEmitter<'a> {
    fn visit(&self, index: u64, b: ArrayBlock<u32>) -> array::Result<()> {
        let mut inner = self.inner.lock().unwrap();

        let begin = index as u32 * b.header.max_entries;
        let end = begin + b.header.nr_entries;
        for (v, block) in b.values.iter().zip(begin..end) {
            let era = if let Some(archived) = inner.era_archive.get(block) {
                ir::Era {
                    block,
                    era: archived,
                }
            } else {
                ir::Era { block, era: *v }
            };

            inner
                .emitter
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
    pub engine_opts: EngineOptions,
    pub logical: bool,
    pub repair: bool,
}

struct EraDumpContext {
    engine: Arc<dyn IoEngine + Send + Sync>,
}

fn mk_context(opts: &EraDumpOptions) -> anyhow::Result<EraDumpContext> {
    let engine = EngineBuilder::new(opts.input, &opts.engine_opts)
        .exclusive(!opts.engine_opts.use_metadata_snap)
        .build()?;
    Ok(EraDumpContext { engine })
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
    let bits = read_bitset(engine.clone(), ws.root, ws.nr_bits as usize, repair)?;

    out.writeset_b(&ir::Writeset {
        era,
        nr_bits: ws.nr_bits,
    })
    .context(OutputError)?;

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
                    out.writeset_blocks(&m).context(OutputError)?;
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
                out.writeset_blocks(&m).context(OutputError)?;
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
        out.writeset_blocks(&m).context(OutputError)?;
    }

    out.writeset_e().context(OutputError)?;

    Ok(())
}

fn dump_eras(
    engine: Arc<dyn IoEngine + Send + Sync>,
    out: &mut dyn MetadataVisitor,
    root: u64,
    ignore_non_fatal: bool,
) -> Result<()> {
    let ablocks = collect_array_blocks_with_path(engine.clone(), ignore_non_fatal, root)?;
    let emitter = EraEmitter::new(out);
    walk_array_blocks(engine, ablocks, &emitter)
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
    out.superblock_b(&xml_sb).context(OutputError)?;

    let writesets = get_writesets_ordered(engine.clone(), sb, repair)?;
    for (era, ws) in writesets.iter() {
        dump_writeset(engine.clone(), out, *era, ws, repair)?;
    }

    out.era_b().context(OutputError)?;
    dump_eras(engine, out, sb.era_array_root, repair)?;
    out.era_e().context(OutputError)?;

    out.superblock_e().context(OutputError)?;
    out.eof().context(OutputError)?;

    Ok(())
}

//-----------------------------------------

fn get_writesets_ordered(
    engine: Arc<dyn IoEngine + Send + Sync>,
    sb: &Superblock,
    repair: bool,
) -> Result<Vec<(u32, Writeset)>> {
    let mut path = vec![0];
    let mut writesets =
        btree_to_map::<Writeset>(&mut path, engine.clone(), repair, sb.writeset_tree_root)?;

    if sb.current_writeset.root != 0 {
        if writesets.contains_key(&(sb.current_era as u64)) {
            return Err(anyhow!(
                "Duplicated era found in current_writeset and the writeset tree"
            ));
        }
        writesets.insert(sb.current_era as u64, sb.current_writeset);
    }

    if writesets.is_empty() {
        return Ok(Vec::new());
    }

    let mut v = Vec::<(u32, Writeset)>::new();
    let era_begin = sb.current_era.wrapping_sub((writesets.len() - 1) as u32);
    for era in era_begin..=sb.current_era {
        if let Some(ws) = writesets.get(&(era as u64)) {
            v.push((era, *ws));
        } else {
            return Err(anyhow!("Writeset of era {} is not present", era));
        }
    }

    Ok(v)
}

fn collate_writeset(index: u32, bitset: &FixedBitSet, archive: &mut dyn Archive) -> Result<()> {
    let era_delta = index + 1;

    for (i, entry) in bitset.as_slice().iter().enumerate() {
        let mut bi = (i << 5) as u32;
        let mut n = *entry;
        while n > 0 {
            if n & 0x1 > 0 {
                archive.set(bi, era_delta)?;
            }
            n >>= 1;
            bi += 1;
        }
    }

    Ok(())
}

fn collate_writesets(
    engine: Arc<dyn IoEngine + Send + Sync>,
    sb: &Superblock,
    repair: bool,
) -> Result<Box<dyn Archive>> {
    let writesets = get_writesets_ordered(engine.clone(), sb, repair)?;

    let archived_begin = writesets.first().map_or(0u32, |(era, _ws)| *era);
    let mut archive = new_era_archive(sb.nr_blocks, archived_begin, writesets.len() as u32);

    for (index, (_era, ws)) in writesets.iter().enumerate() {
        let bitset = read_bitset(engine.clone(), ws.root, ws.nr_bits as usize, repair)?;
        collate_writeset(index as u32, &bitset, archive.as_mut())?;
    }

    Ok(archive)
}

fn dump_eras_logical(
    engine: Arc<dyn IoEngine + Send + Sync>,
    out: &mut dyn MetadataVisitor,
    root: u64,
    era_archive: &dyn Archive,
    ignore_non_fatal: bool,
) -> Result<()> {
    let ablocks = collect_array_blocks_with_path(engine.clone(), ignore_non_fatal, root)?;
    let emitter = LogicalEraEmitter::new(out, era_archive);
    walk_array_blocks(engine, ablocks, &emitter)
}

pub fn dump_metadata_logical(
    engine: Arc<dyn IoEngine + Send + Sync>,
    out: &mut dyn MetadataVisitor,
    sb: &Superblock,
    repair: bool,
) -> anyhow::Result<()> {
    let era_archive = collate_writesets(engine.clone(), sb, repair)?;

    let xml_sb = ir::Superblock {
        uuid: "".to_string(),
        block_size: sb.data_block_size,
        nr_blocks: sb.nr_blocks,
        current_era: sb.current_era,
    };
    out.superblock_b(&xml_sb).context(OutputError)?;

    out.era_b().context(OutputError)?;
    dump_eras_logical(engine, out, sb.era_array_root, era_archive.as_ref(), repair)?;
    out.era_e().context(OutputError)?;

    out.superblock_e().context(OutputError)?;
    out.eof().context(OutputError)?;

    Ok(())
}

//-----------------------------------------

pub fn dump(opts: EraDumpOptions) -> anyhow::Result<()> {
    let ctx = mk_context(&opts)?;
    let sb = read_superblock(ctx.engine.as_ref(), SUPERBLOCK_LOCATION)?;

    let writer: Box<dyn Write> = if opts.output.is_some() {
        let f = File::create(opts.output.unwrap()).context(OutputError)?;
        Box::new(BufWriter::new(f))
    } else {
        Box::new(BufWriter::new(std::io::stdout()))
    };
    let mut out = xml::XmlWriter::new(writer, false);

    let writesets = get_writesets_ordered(ctx.engine.clone(), &sb, opts.repair)?;
    if opts.logical && !writesets.is_empty() {
        dump_metadata_logical(ctx.engine, &mut out, &sb, opts.repair)
    } else {
        dump_metadata(ctx.engine, &mut out, &sb, opts.repair)
    }
}

//------------------------------------------
