use anyhow::Result;
use quick_xml::events::{BytesEnd, BytesStart, Event};
use quick_xml::Writer;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::commands::engine::*;
use crate::era::superblock::*;
use crate::era::writeset::*;
use crate::io_engine::*;
use crate::math::div_up;
use crate::pdata::array::{self, value_err, ArrayBlock};
use crate::pdata::array_walker::*;
use crate::pdata::btree_walker::*;
use crate::xml::mk_attr;

//------------------------------------------

struct BitsetCollator {
    composed_bits: Mutex<Vec<u64>>,
}

impl BitsetCollator {
    fn new(bitset: Vec<u64>) -> BitsetCollator {
        BitsetCollator {
            composed_bits: Mutex::new(bitset),
        }
    }

    fn complete(self) -> Vec<u64> {
        self.composed_bits.into_inner().unwrap()
    }
}

impl ArrayVisitor<u64> for BitsetCollator {
    fn visit(&self, index: u64, b: ArrayBlock<u64>) -> array::Result<()> {
        let mut bitset = self.composed_bits.lock().unwrap();
        let idx = index as usize * b.header.max_entries as usize; // index of u64 in bitset array
        for (entry, dest) in b.values.iter().zip(bitset.iter_mut().skip(idx)) {
            *dest |= entry;
        }
        Ok(())
    }
}

//------------------------------------------

struct EraArrayCollator {
    composed_bits: Mutex<Vec<u64>>,
    threshold: u32,
}

impl EraArrayCollator {
    fn new(bitset: Vec<u64>, threshold: u32) -> EraArrayCollator {
        EraArrayCollator {
            composed_bits: Mutex::new(bitset),
            threshold,
        }
    }

    fn complete(self) -> Vec<u64> {
        self.composed_bits.into_inner().unwrap()
    }
}

impl ArrayVisitor<u32> for EraArrayCollator {
    fn visit(&self, index: u64, b: ArrayBlock<u32>) -> array::Result<()> {
        let blk_begin = index as usize * b.header.max_entries as usize; // range of data blocks
        let blk_end = blk_begin + b.header.max_entries as usize;

        let mut bitset = self.composed_bits.lock().unwrap();
        let mut bitset_iter = bitset.iter_mut();
        let mut idx = blk_begin >> 6; // index of u64 in bitset array
        let mut dest = bitset_iter
            .nth(idx)
            .ok_or_else(|| value_err("array index out of bounds".to_string()))?;
        let mut buf = *dest;
        for (era, blk) in b.values.iter().zip(blk_begin..blk_end) {
            if *era < self.threshold {
                continue;
            }
            let steps = (blk >> 6) - idx;
            if steps > 0 {
                *dest = buf;
                idx += steps;
                dest = bitset_iter
                    .nth(steps - 1)
                    .ok_or_else(|| value_err("array index out of bounds".to_string()))?;
                buf = *dest;
            }
            buf |= 1 << (blk & 0x3F);
        }
        *dest = buf;

        Ok(())
    }
}

//------------------------------------------

fn collate_writeset(
    engine: Arc<dyn IoEngine + Send + Sync>,
    writeset_root: u64,
    marked_bits: Vec<u64>,
) -> Result<Vec<u64>> {
    let w = ArrayWalker::new(engine, false);
    let c = BitsetCollator::new(marked_bits);
    w.walk(&c, writeset_root)?;
    Ok(c.complete())
}

fn collate_era_array(
    engine: Arc<dyn IoEngine + Send + Sync>,
    era_array_root: u64,
    marked_bits: Vec<u64>,
    threshold: u32,
) -> Result<Vec<u64>> {
    let w = ArrayWalker::new(engine, false);
    let c = EraArrayCollator::new(marked_bits, threshold);
    w.walk(&c, era_array_root)?;
    Ok(c.complete())
}

fn mark_blocks_since(
    engine: Arc<dyn IoEngine + Send + Sync>,
    sb: &Superblock,
    threshold: u32,
) -> Result<Vec<u64>> {
    let mut marked_bits = vec![0; div_up(sb.nr_blocks as usize, 64)];

    let mut path = vec![0];
    let wsets = btree_to_map::<Writeset>(&mut path, engine.clone(), false, sb.writeset_tree_root)?;
    for (era, ws) in wsets.iter() {
        if (*era as u32) < threshold {
            continue;
        }
        marked_bits = collate_writeset(engine.clone(), ws.root, marked_bits)?;
    }

    if let Some(archived_begin) = wsets.keys().next() {
        if *archived_begin as u32 > threshold {
            marked_bits =
                collate_era_array(engine.clone(), sb.era_array_root, marked_bits, threshold)?;
        }
    }

    Ok(marked_bits)
}

fn emit_start<W: Write>(w: &mut Writer<W>) -> Result<()> {
    let elem = BytesStart::new("blocks");
    w.write_event(Event::Start(elem))?;
    Ok(())
}

fn emit_end<W: Write>(w: &mut Writer<W>) -> Result<()> {
    let elem = BytesEnd::new("blocks");
    w.write_event(Event::End(elem))?;
    Ok(())
}

fn emit_range<W: Write>(w: &mut Writer<W>, begin: u32, end: u32) -> Result<()> {
    if end > begin + 1 {
        let mut elem = BytesStart::new("range");
        elem.push_attribute(mk_attr(b"begin", begin));
        elem.push_attribute(mk_attr(b"end", end));
        w.write_event(Event::Empty(elem))?;
    } else if end > begin {
        let mut elem = BytesStart::new("block");
        elem.push_attribute(mk_attr(b"block", begin));
        w.write_event(Event::Empty(elem))?;
    }

    Ok(())
}

fn emit_blocks<W: Write>(marked_bits: &[u64], nr_blocks: u32, w: &mut Writer<W>) -> Result<()> {
    let mut begin: u32 = 0;
    let mut end: u32 = 0;

    emit_start(w)?;

    for (index, entry) in marked_bits.iter().enumerate() {
        let mut n = *entry;

        if n == u64::max_value() {
            end = std::cmp::min(end + 64, nr_blocks);
            continue;
        }

        while n > 0 {
            let zeros = n.trailing_zeros();
            if zeros > 0 {
                if end > begin {
                    emit_range(w, begin, end)?;
                }
                n >>= zeros;
                end += zeros;
                begin = end;
            }

            let ones = n.trailing_ones();
            n >>= ones;
            end = std::cmp::min(end + ones, nr_blocks);
        }

        let endpos = (index << 6) as u32 + 64;
        if end < endpos {
            if end > begin {
                emit_range(w, begin, end)?;
            }
            begin = endpos;
            end = begin;
        }
    }

    if end > begin {
        emit_range(w, begin, end)?;
    }

    emit_end(w)?;

    Ok(())
}

//------------------------------------------

pub struct EraInvalidateOptions<'a> {
    pub input: &'a Path,
    pub output: Option<&'a Path>,
    pub engine_opts: EngineOptions,
    pub threshold: u32,
}

struct Context {
    engine: Arc<dyn IoEngine + Send + Sync>,
}

fn mk_context(opts: &EraInvalidateOptions) -> anyhow::Result<Context> {
    let engine = EngineBuilder::new(opts.input, &opts.engine_opts)
        .exclusive(!opts.engine_opts.use_metadata_snap)
        .build()?;
    Ok(Context { engine })
}

pub fn invalidate(opts: &EraInvalidateOptions) -> Result<()> {
    let ctx = mk_context(opts)?;

    let sb = if opts.engine_opts.use_metadata_snap {
        read_superblock_snap(ctx.engine.as_ref())?
    } else {
        read_superblock(ctx.engine.as_ref(), SUPERBLOCK_LOCATION)?
    };

    let w: Box<dyn Write> = if opts.output.is_some() {
        Box::new(BufWriter::new(File::create(opts.output.unwrap())?))
    } else {
        Box::new(BufWriter::new(std::io::stdout()))
    };
    let mut writer = Writer::new_with_indent(w, 0x20, 2);

    let marked_bits = mark_blocks_since(ctx.engine, &sb, opts.threshold)?;
    emit_blocks(&marked_bits, sb.nr_blocks, &mut writer)
}

//------------------------------------------
