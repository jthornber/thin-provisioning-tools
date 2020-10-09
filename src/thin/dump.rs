use anyhow::Result;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::io_engine::{AsyncIoEngine, IoEngine, SyncIoEngine};
use crate::pdata::btree::{self, *};
use crate::pdata::space_map::*;
use crate::pdata::unpack::*;
use crate::report::*;
use crate::thin::block_time::*;
use crate::thin::device_detail::*;
use crate::thin::superblock::*;
use crate::thin::xml::{self, MetadataVisitor};

//------------------------------------------

struct RunBuilder {
    run: Option<xml::Map>,
}

impl RunBuilder {
    fn new() -> RunBuilder {
        RunBuilder { run: None }
    }

    fn next(&mut self, thin_block: u64, data_block: u64, time: u32) -> Option<xml::Map> {
        use xml::Map;

        match self.run {
            None => {
                self.run = Some(xml::Map {
                    thin_begin: thin_block,
                    data_begin: data_block,
                    time: time,
                    len: 1,
                });
                None
            }
            Some(xml::Map {
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
                        time: time,
                        len: 1,
                    })
                }
            }
        }
    }

    fn complete(&mut self) -> Option<xml::Map> {
        self.run.take()
    }
}

//------------------------------------------

struct MVInner<'a> {
    md_out: &'a mut dyn xml::MetadataVisitor,
    builder: RunBuilder,
}

struct MappingVisitor<'a> {
    inner: Mutex<MVInner<'a>>,
}

//------------------------------------------

impl<'a> MappingVisitor<'a> {
    fn new(md_out: &'a mut dyn xml::MetadataVisitor) -> MappingVisitor<'a> {
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
        _path: &Vec<u64>,
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

    fn visit_again(&self, _path: &Vec<u64>, b: u64) -> btree::Result<()> {
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
    pub dev: &'a Path,
    pub async_io: bool,
    pub report: Arc<Report>,
}

struct Context {
    report: Arc<Report>,
    engine: Arc<dyn IoEngine + Send + Sync>,
}

fn mk_context(opts: &ThinDumpOptions) -> Result<Context> {
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

//------------------------------------------

struct NoopVisitor {}

impl<V: Unpack> btree::NodeVisitor<V> for NoopVisitor {
    fn visit(
        &self,
        _path: &Vec<u64>,
        _kr: &btree::KeyRange,
        _h: &btree::NodeHeader,
        _k: &[u64],
        _values: &[V],
    ) -> btree::Result<()> {
        Ok(())
    }

    fn visit_again(&self, _path: &Vec<u64>, _b: u64) -> btree::Result<()> {
        Ok(())
    }

    fn end_walk(&self) -> btree::Result<()> {
        Ok(())
    }
}

fn find_shared_nodes(
    ctx: &Context,
    nr_metadata_blocks: u64,
    roots: &BTreeMap<u64, u64>,
) -> Result<(BTreeSet<u64>, Arc<Mutex<dyn SpaceMap + Send + Sync>>)> {
    // By default the walker uses a restricted space map that can only count to 1.  So
    // we explicitly create a full sm.
    let sm = core_sm(nr_metadata_blocks, roots.len() as u32);
    let w = BTreeWalker::new_with_sm(ctx.engine.clone(), sm.clone(), false)?;

    let mut path = Vec::new();
    path.push(0);

    for (thin_id, root) in roots {
        ctx.report.info(&format!("scanning {}", thin_id));
        let v = NoopVisitor {};
        w.walk::<NoopVisitor, BlockTime>(&mut path, &v, *root)?;
    }

    let mut shared = BTreeSet::new();
    {
        let sm = sm.lock().unwrap();
        for i in 0..sm.get_nr_blocks().unwrap() {
            if sm.get(i).expect("couldn't get count from space map.") > 1 {
                shared.insert(i);
            }
        }
    }

    return Ok((shared, sm));
}

//------------------------------------------

fn dump_node(
    ctx: &Context,
    out: &mut dyn xml::MetadataVisitor,
    root: u64,
    sm: &Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    force: bool, // sets the ref count for the root to zero to force output.
) -> Result<()> {
    let w = BTreeWalker::new_with_sm(ctx.engine.clone(), sm.clone(), false)?;
    let mut path = Vec::new();
    path.push(0);

    let v = MappingVisitor::new(out);

    // Temporarily set the ref count for the root to zero.
    let mut old_count = 0;
    if force {
        let mut sm = sm.lock().unwrap();
        old_count = sm.get(root).unwrap();
        sm.set(root, 0)?;
    }

    w.walk::<MappingVisitor, BlockTime>(&mut path, &v, root)?;

    // Reset the ref count for root.
    if force {
        let mut sm = sm.lock().unwrap();
        sm.set(root, old_count)?;
    }

    Ok(())
}

//------------------------------------------

pub fn dump(opts: ThinDumpOptions) -> Result<()> {
    let ctx = mk_context(&opts)?;

    let report = &ctx.report;
    let engine = &ctx.engine;

    // superblock
    report.set_title("Reading superblock");
    let sb = read_superblock(engine.as_ref(), SUPERBLOCK_LOCATION)?;
    let metadata_root = unpack::<SMRoot>(&sb.metadata_sm_root[0..])?;
    let data_root = unpack::<SMRoot>(&sb.data_sm_root[0..])?;
    let mut path = Vec::new();
    path.push(0);

    report.set_title("Reading device details");
    let devs = btree_to_map::<DeviceDetail>(&mut path, engine.clone(), true, sb.details_root)?;

    report.set_title("Reading mappings roots");
    let roots = btree_to_map::<u64>(&mut path, engine.clone(), true, sb.mapping_root)?;

    report.set_title("Finding shared mappings");
    let (shared, sm) = find_shared_nodes(&ctx, metadata_root.nr_blocks, &roots)?;
    report.info(&format!("{} shared nodes found", shared.len()));

    let mut out = xml::XmlWriter::new(std::io::stdout());
    let xml_sb = xml::Superblock {
        uuid: "".to_string(),
        time: sb.time as u64,
        transaction: sb.transaction_id,
        flags: None,
        version: Some(2),
        data_block_size: sb.data_block_size,
        nr_data_blocks: data_root.nr_blocks,
        metadata_snap: None,
    };
    out.superblock_b(&xml_sb)?;

    report.set_title("Dumping shared regions");
    for b in shared {
        out.def_shared_b(&format!("{}", b))?;
        dump_node(&ctx, &mut out, b, &sm, true)?;
        out.def_shared_e()?;
    }

    report.set_title("Dumping mappings");
    for (thin_id, detail) in devs {
        let d = xml::Device {
            dev_id: thin_id as u32,
            mapped_blocks: detail.mapped_blocks,
            transaction: detail.transaction_id,
            creation_time: detail.creation_time as u64,
            snap_time: detail.snapshotted_time as u64,
        };
        out.device_b(&d)?;
        let root = roots.get(&thin_id).unwrap();
        dump_node(&ctx, &mut out, *root, &sm, false)?;
        out.device_e()?;
    }
    out.superblock_e()?;

    Ok(())
}

//------------------------------------------
