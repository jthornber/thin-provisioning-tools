use anyhow::{anyhow, Result};
use std::fs::File;
use std::io::BufWriter;
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::checksum;
use crate::io_engine::{AsyncIoEngine, Block, IoEngine, SyncIoEngine};
use crate::pdata::btree::{self, *};
use crate::pdata::btree_walker::*;
use crate::pdata::space_map_common::*;
use crate::pdata::unpack::*;
use crate::report::*;
use crate::thin::block_time::*;
use crate::thin::ir::{self, MetadataVisitor};
use crate::thin::metadata::*;
use crate::thin::metadata_repair::*;
use crate::thin::superblock::*;
use crate::thin::xml;

//------------------------------------------

struct RunBuilder {
    run: Option<ir::Map>,
}

impl RunBuilder {
    fn new() -> RunBuilder {
        RunBuilder { run: None }
    }

    fn next(&mut self, thin_block: u64, data_block: u64, time: u32) -> Option<ir::Map> {
        use ir::Map;

        match self.run {
            None => {
                self.run = Some(ir::Map {
                    thin_begin: thin_block,
                    data_begin: data_block,
                    time,
                    len: 1,
                });
                None
            }
            Some(ir::Map {
                thin_begin,
                data_begin,
                time: mtime,
                len,
            }) => {
                if thin_block == (thin_begin + len)
                    && data_block == (data_begin + len)
                    && mtime == time
                {
                    self.run.as_mut().unwrap().len += 1;
                    None
                } else {
                    self.run.replace(Map {
                        thin_begin: thin_block,
                        data_begin: data_block,
                        time,
                        len: 1,
                    })
                }
            }
        }
    }

    fn complete(&mut self) -> Option<ir::Map> {
        self.run.take()
    }
}

//------------------------------------------

struct MVInner<'a> {
    md_out: &'a mut dyn MetadataVisitor,
    builder: RunBuilder,
}

struct MappingVisitor<'a> {
    inner: Mutex<MVInner<'a>>,
}

//------------------------------------------

impl<'a> MappingVisitor<'a> {
    fn new(md_out: &'a mut dyn MetadataVisitor) -> MappingVisitor<'a> {
        MappingVisitor {
            inner: Mutex::new(MVInner {
                md_out,
                builder: RunBuilder::new(),
            }),
        }
    }
}

impl<'a> NodeVisitor<BlockTime> for MappingVisitor<'a> {
    fn visit(
        &self,
        _path: &[u64],
        _kr: &KeyRange,
        _h: &NodeHeader,
        keys: &[u64],
        values: &[BlockTime],
    ) -> btree::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        for (k, v) in keys.iter().zip(values.iter()) {
            if let Some(run) = inner.builder.next(*k, v.block, v.time) {
                inner
                    .md_out
                    .map(&run)
                    .map_err(|e| btree::value_err(format!("{}", e)))?;
            }
        }

        Ok(())
    }

    fn visit_again(&self, _path: &[u64], b: u64) -> btree::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner
            .md_out
            .ref_shared(&format!("{}", b))
            .map_err(|e| btree::value_err(format!("{}", e)))?;
        Ok(())
    }

    fn end_walk(&self) -> btree::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        if let Some(run) = inner.builder.complete() {
            inner
                .md_out
                .map(&run)
                .map_err(|e| btree::value_err(format!("{}", e)))?;
        }
        Ok(())
    }
}

//------------------------------------------

const MAX_CONCURRENT_IO: u32 = 1024;

pub struct ThinDumpOptions<'a> {
    pub input: &'a Path,
    pub output: Option<&'a Path>,
    pub async_io: bool,
    pub report: Arc<Report>,
    pub repair: bool,
    pub use_metadata_snap: bool,
    pub overrides: SuperblockOverrides,
}

struct Context {
    report: Arc<Report>,
    engine: Arc<dyn IoEngine + Send + Sync>,
}

fn mk_context(opts: &ThinDumpOptions) -> Result<Context> {
    let engine: Arc<dyn IoEngine + Send + Sync> = if opts.async_io {
        Arc::new(AsyncIoEngine::new(opts.input, MAX_CONCURRENT_IO, false)?)
    } else {
        let nr_threads = std::cmp::max(8, num_cpus::get() * 2);
        Arc::new(SyncIoEngine::new(opts.input, nr_threads, false)?)
    };

    Ok(Context {
        report: opts.report.clone(),
        engine,
    })
}

//------------------------------------------

fn emit_leaf(v: &mut MappingVisitor, b: &Block) -> Result<()> {
    use Node::*;
    let path = Vec::new();
    let kr = KeyRange::new();

    let bt = checksum::metadata_block_type(b.get_data());
    if bt != checksum::BT::NODE {
        return Err(anyhow!(format!(
            "checksum failed for node {}, {:?}",
            b.loc, bt
        )));
    }

    let node = unpack_node::<BlockTime>(&path, b.get_data(), true, true)?;

    match node {
        Internal { .. } => {
            return Err(anyhow!("not a leaf"));
        }
        Leaf {
            header,
            keys,
            values,
        } => {
            if let Err(_e) = v.visit(&path, &kr, &header, &keys, &values) {
                return Err(anyhow!("couldn't emit leaf node"));
            }
        }
    }

    Ok(())
}

fn read_for<T>(engine: Arc<dyn IoEngine>, blocks: &[u64], mut t: T) -> Result<()>
where
    T: FnMut(Block) -> Result<()>,
{
    for cs in blocks.chunks(engine.get_batch_size()) {
        for b in engine
            .read_many(cs)
            .map_err(|_e| anyhow!("read_many failed"))?
        {
            t(b.map_err(|_e| anyhow!("read of individual block failed"))?)?;
        }
    }

    Ok(())
}

fn emit_leaves(engine: Arc<dyn IoEngine>, out: &mut dyn MetadataVisitor, ls: &[u64]) -> Result<()> {
    let mut v = MappingVisitor::new(out);
    let proc = |b| {
        emit_leaf(&mut v, &b)?;
        Ok(())
    };

    read_for(engine, ls, proc)?;
    v.end_walk().map_err(|_| anyhow!("failed to emit leaves"))
}

fn emit_entries(
    engine: Arc<dyn IoEngine>,
    out: &mut dyn MetadataVisitor,
    entries: &[Entry],
) -> Result<()> {
    let mut leaves = Vec::new();

    for e in entries {
        match e {
            Entry::Leaf(b) => {
                leaves.push(*b);
            }
            Entry::Ref(id) => {
                if !leaves.is_empty() {
                    emit_leaves(engine.clone(), out, &leaves[0..])?;
                    leaves.clear();
                }
                let str = format!("{}", id);
                out.ref_shared(&str)?;
            }
        }
    }

    if !leaves.is_empty() {
        emit_leaves(engine, out, &leaves[0..])?;
    }

    Ok(())
}

pub fn dump_metadata(
    engine: Arc<dyn IoEngine>,
    out: &mut dyn MetadataVisitor,
    sb: &Superblock,
    md: &Metadata,
) -> Result<()> {
    let data_root = unpack::<SMRoot>(&sb.data_sm_root[0..])?;
    let out_sb = ir::Superblock {
        uuid: "".to_string(),
        time: sb.time,
        transaction: sb.transaction_id,
        flags: if sb.flags.needs_check { Some(1) } else { None },
        version: Some(2),
        data_block_size: sb.data_block_size,
        nr_data_blocks: data_root.nr_blocks,
        metadata_snap: None,
    };
    out.superblock_b(&out_sb)?;

    for d in &md.defs {
        out.def_shared_b(&format!("{}", d.def_id))?;
        emit_entries(engine.clone(), out, &d.map.entries)?;
        out.def_shared_e()?;
    }

    for dev in &md.devs {
        let device = ir::Device {
            dev_id: dev.thin_id,
            mapped_blocks: dev.detail.mapped_blocks,
            transaction: dev.detail.transaction_id,
            creation_time: dev.detail.creation_time,
            snap_time: dev.detail.snapshotted_time,
        };
        out.device_b(&device)?;
        emit_entries(engine.clone(), out, &dev.map.entries)?;
        out.device_e()?;
    }
    out.superblock_e()?;
    out.eof()?;

    Ok(())
}

//------------------------------------------

pub fn dump(opts: ThinDumpOptions) -> Result<()> {
    let ctx = mk_context(&opts)?;
    let sb = if opts.repair {
        read_or_rebuild_superblock(
            ctx.engine.clone(),
            ctx.report.clone(),
            SUPERBLOCK_LOCATION,
            &opts.overrides,
        )?
    } else if opts.use_metadata_snap {
        read_superblock_snap(ctx.engine.as_ref())?
    } else {
        read_superblock(ctx.engine.as_ref(), SUPERBLOCK_LOCATION)
            .and_then(|sb| sb.overrides(&opts.overrides))?
    };
    let md = build_metadata(ctx.engine.clone(), &sb)?;
    let md = optimise_metadata(md)?;

    let writer: Box<dyn Write> = if opts.output.is_some() {
        Box::new(BufWriter::new(File::create(opts.output.unwrap())?))
    } else {
        Box::new(BufWriter::new(std::io::stdout()))
    };
    let mut out = xml::XmlWriter::new(writer);

    dump_metadata(ctx.engine, &mut out, &sb, &md)
}

//------------------------------------------
