use anyhow::{anyhow, Result};
use std::io::BufWriter;
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::commands::engine::*;
use crate::io_engine::*;
use crate::pdata::btree::{self, KeyRange, NodeHeader};
use crate::pdata::btree_walker::{btree_to_map, BTreeWalker, NodeVisitor};
use crate::pdata::space_map::common::SMRoot;
use crate::pdata::unpack::unpack;
use crate::report::Report;
use crate::thin::block_time::BlockTime;
use crate::thin::delta_visitor::*;
use crate::thin::ir;
use crate::thin::metadata_repair::is_superblock_consistent;
use crate::thin::superblock::*;

//------------------------------------------

struct RunBuilder {
    run: Option<DataMapping>,
}

impl RunBuilder {
    fn new() -> RunBuilder {
        RunBuilder { run: None }
    }

    fn next(&mut self, thin_block: u64, data_block: u64) -> Option<DataMapping> {
        if let Some(ref mut r) = self.run {
            if r.thin_begin + r.len == thin_block {
                r.len += 1;
                None
            } else {
                self.run.replace(DataMapping {
                    thin_begin: thin_block,
                    data_begin: data_block,
                    len: 1,
                })
            }
        } else {
            self.run.replace(DataMapping {
                thin_begin: thin_block,
                data_begin: data_block,
                len: 1,
            })
        }
    }

    fn complete(&mut self) -> Option<DataMapping> {
        self.run.take()
    }
}

struct RecorderInner {
    mappings: Vec<DataMapping>,
    builder: RunBuilder,
}

struct MappingRecorder {
    inner: Mutex<RecorderInner>,
}

impl MappingRecorder {
    fn new() -> MappingRecorder {
        MappingRecorder {
            inner: Mutex::new(RecorderInner {
                mappings: Vec::new(),
                builder: RunBuilder::new(),
            }),
        }
    }

    fn complete(self) -> Vec<DataMapping> {
        let mut inner = self.inner.lock().unwrap();
        if let Some(m) = inner.builder.complete() {
            inner.mappings.push(m);
        }
        let mut mappings = Vec::new();
        std::mem::swap(&mut mappings, &mut inner.mappings);
        mappings
    }
}

impl NodeVisitor<BlockTime> for MappingRecorder {
    fn visit(
        &self,
        _path: &[u64],
        _kr: &KeyRange,
        _h: &NodeHeader,
        keys: &[u64],
        values: &[BlockTime],
    ) -> btree::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        for (k, v) in keys.iter().zip(values) {
            if let Some(m) = inner.builder.next(*k, v.block) {
                inner.mappings.push(m);
            }
        }
        Ok(())
    }

    fn visit_again(&self, _path: &[u64], _b: u64) -> btree::Result<()> {
        Ok(())
    }

    fn end_walk(&self) -> btree::Result<()> {
        Ok(())
    }
}

// The `time` field is not extracted for CoW indication. The mapped data block
// does the job. i.e., the data block must be different if the mapping had been
// CoW'ed.
pub fn get_mappings(
    engine: Arc<dyn IoEngine + Send + Sync>,
    root: u64,
) -> Result<Vec<DataMapping>> {
    let mr = MappingRecorder::new();
    let w = Arc::new(BTreeWalker::new(engine.clone(), false));
    let mut path = Vec::new();
    w.walk(&mut path, &mr, root)?;
    Ok(mr.complete())
}

//------------------------------------------

struct MappingStream<'a> {
    iter: &'a mut dyn Iterator<Item = &'a DataMapping>,
    current: Option<DataMapping>,
}

impl<'a> MappingStream<'a> {
    fn new(iter: &'a mut dyn Iterator<Item = &'a DataMapping>) -> MappingStream<'a> {
        let current = iter.next().cloned();
        MappingStream { iter, current }
    }

    fn more_mappings(&self) -> bool {
        self.current.is_some()
    }

    fn get_mapping(&self) -> Option<DataMapping> {
        self.current.clone()
    }

    fn consume(&mut self, len: u64) -> Result<()> {
        if let Some(ref mut current) = self.current {
            if len > current.len {
                return Err(anyhow!("delta too long"));
            }

            if len == current.len {
                self.current = self.iter.next().cloned();
            } else {
                current.thin_begin += len;
                current.data_begin += len;
                current.len -= len;
            }
            Ok(())
        } else {
            Err(anyhow!("end of stream already reached"))
        }
    }
}

fn dump_delta_mappings(
    left: &[DataMapping],
    right: &[DataMapping],
    visitor: &mut dyn DeltaVisitor,
) -> Result<()> {
    let mut left_iter = left.iter();
    let mut right_iter = right.iter();
    let mut ls = MappingStream::new(&mut left_iter);
    let mut rs = MappingStream::new(&mut right_iter);

    while ls.more_mappings() && rs.more_mappings() {
        let lm = ls.get_mapping().unwrap();
        let rm = rs.get_mapping().unwrap();

        if lm.thin_begin < rm.thin_begin {
            let len = std::cmp::min(lm.len, rm.thin_begin - lm.thin_begin);
            let delta = Delta::LeftOnly(DataMapping {
                thin_begin: lm.thin_begin,
                data_begin: lm.data_begin,
                len,
            });
            visitor.delta(&delta)?;
            ls.consume(len)?;
        } else if rm.thin_begin < lm.thin_begin {
            let len = std::cmp::min(rm.len, lm.thin_begin - rm.thin_begin);
            let delta = Delta::RightOnly(DataMapping {
                thin_begin: rm.thin_begin,
                data_begin: rm.data_begin,
                len,
            });
            visitor.delta(&delta)?;
            rs.consume(len)?;
        } else if lm.data_begin == rm.data_begin {
            let len = std::cmp::min(lm.len, rm.len);
            let delta = Delta::Same(DataMapping {
                thin_begin: lm.thin_begin,
                data_begin: lm.data_begin,
                len,
            });
            visitor.delta(&delta)?;
            ls.consume(len)?;
            rs.consume(len)?;
        } else {
            let len = std::cmp::min(lm.len, rm.len);
            let delta = Delta::Differ(DiffMapping {
                thin_begin: lm.thin_begin,
                left_data_begin: lm.data_begin,
                right_data_begin: rm.data_begin,
                len,
            });
            visitor.delta(&delta)?;
            ls.consume(len)?;
            rs.consume(len)?;
        }
    }

    while ls.more_mappings() {
        let lm = ls.get_mapping().unwrap();
        let len = lm.len;
        visitor.delta(&Delta::LeftOnly(lm))?;
        ls.consume(len)?;
    }

    while rs.more_mappings() {
        let rm = rs.get_mapping().unwrap();
        let len = rm.len;
        visitor.delta(&Delta::RightOnly(rm))?;
        rs.consume(len)?;
    }

    Ok(())
}

fn dump_diff(
    engine: Arc<dyn IoEngine + Send + Sync>,
    visitor: &mut dyn DeltaVisitor,
    sb: &Superblock,
    snap1: Snap,
    snap2: Snap,
) -> Result<()> {
    let mut path = Vec::new();
    let roots = btree_to_map::<u64>(&mut path, engine.clone(), false, sb.mapping_root)?;

    let root1 = match snap1 {
        Snap::DeviceId(dev_id) => *roots
            .get(&dev_id)
            .ok_or_else(|| anyhow!("Unable to find mapping tree for snap1 ({})", dev_id))?,
        Snap::RootBlock(b) => b,
    };
    let mappings1 = get_mappings(engine.clone(), root1)?;

    let root2 = match snap2 {
        Snap::DeviceId(dev_id) => *roots
            .get(&dev_id)
            .ok_or_else(|| anyhow!("Unable to find mapping tree for snap2 ({})", dev_id))?,
        Snap::RootBlock(b) => b,
    };
    let mappings2 = get_mappings(engine.clone(), root2)?;

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

    visitor.superblock_b(&out_sb)?;
    visitor.diff_b(snap1, snap2)?;
    dump_delta_mappings(&mappings1, &mappings2, visitor)?;
    visitor.diff_e()?;
    visitor.superblock_e()?;

    Ok(())
}

//------------------------------------------

pub struct ThinDeltaOptions<'a> {
    pub input: &'a Path,
    pub engine_opts: EngineOptions,
    pub report: Arc<Report>,
    pub snap1: Snap,
    pub snap2: Snap,
    pub verbose: bool,
}

struct Context {
    engine: Arc<dyn IoEngine + Send + Sync>,
    _report: Arc<Report>, // TODO: report the scanning progress
}

fn mk_context(opts: &ThinDeltaOptions) -> Result<Context> {
    let engine = EngineBuilder::new(opts.input, &opts.engine_opts)
        .exclusive(!opts.engine_opts.use_metadata_snap)
        .build()?;

    Ok(Context {
        engine,
        _report: opts.report.clone(),
    })
}

pub fn delta(opts: ThinDeltaOptions) -> Result<()> {
    let ctx = mk_context(&opts)?;

    let sb = if opts.engine_opts.use_metadata_snap {
        read_superblock_snap(ctx.engine.as_ref())?
    } else {
        read_superblock(ctx.engine.as_ref(), SUPERBLOCK_LOCATION)?
    };

    // ensure the metadata is consistent
    is_superblock_consistent(sb.clone(), ctx.engine.clone(), false)?;

    let w = BufWriter::new(std::io::stdout());
    let mut writer: Box<dyn DeltaVisitor> = if opts.verbose {
        Box::new(VerboseXmlWriter::new(w))
    } else {
        Box::new(SimpleXmlWriter::new(w))
    };

    dump_diff(ctx.engine, writer.as_mut(), &sb, opts.snap1, opts.snap2)
}

//------------------------------------------
