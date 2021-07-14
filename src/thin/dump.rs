use anyhow::{anyhow, Result};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::BufWriter;
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::checksum;
use crate::io_engine::{AsyncIoEngine, Block, IoEngine, SyncIoEngine};
use crate::pdata::btree::{self, *};
use crate::pdata::btree_leaf_walker::*;
use crate::pdata::btree_walker::*;
use crate::pdata::space_map::*;
use crate::pdata::space_map_common::*;
use crate::pdata::unpack::*;
use crate::report::*;
use crate::thin::block_time::*;
use crate::thin::device_detail::*;
use crate::thin::ir::{self, MetadataVisitor};
use crate::thin::runs::*;
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
}

struct Context {
    report: Arc<Report>,
    engine: Arc<dyn IoEngine + Send + Sync>,
}

fn mk_context(opts: &ThinDumpOptions) -> Result<Context> {
    let engine: Arc<dyn IoEngine + Send + Sync>;

    if opts.async_io {
        engine = Arc::new(AsyncIoEngine::new(opts.input, MAX_CONCURRENT_IO, false)?);
    } else {
        let nr_threads = std::cmp::max(8, num_cpus::get() * 2);
        engine = Arc::new(SyncIoEngine::new(opts.input, nr_threads, false)?);
    }

    Ok(Context {
        report: opts.report.clone(),
        engine,
    })
}

//------------------------------------------

type DefId = u64;
type ThinId = u32;

#[derive(Clone)]
enum Entry {
    Leaf(u64),
    Ref(DefId),
}

#[derive(Clone)]
struct Mapping {
    kr: KeyRange,
    entries: Vec<Entry>,
}

#[derive(Clone)]
struct Device {
    thin_id: ThinId,
    detail: DeviceDetail,
    map: Mapping,
}

#[derive(Clone)]
struct Def {
    def_id: DefId,
    map: Mapping,
}

#[derive(Clone)]
struct Metadata {
    defs: Vec<Def>,
    devs: Vec<Device>,
}

//------------------------------------------

struct CollectLeaves {
    leaves: Vec<Entry>,
}

impl CollectLeaves {
    fn new() -> CollectLeaves {
        CollectLeaves { leaves: Vec::new() }
    }
}

impl LeafVisitor<BlockTime> for CollectLeaves {
    fn visit(&mut self, _kr: &KeyRange, b: u64) -> btree::Result<()> {
        self.leaves.push(Entry::Leaf(b));
        Ok(())
    }

    fn visit_again(&mut self, b: u64) -> btree::Result<()> {
        self.leaves.push(Entry::Ref(b));
        Ok(())
    }

    fn end_walk(&mut self) -> btree::Result<()> {
        Ok(())
    }
}

fn collect_leaves(
    engine: Arc<dyn IoEngine + Send + Sync>,
    roots: &BTreeSet<u64>,
) -> Result<BTreeMap<u64, Vec<Entry>>> {
    let mut map: BTreeMap<u64, Vec<Entry>> = BTreeMap::new();
    let mut sm = RestrictedSpaceMap::new(engine.get_nr_blocks());

    for r in roots {
        let mut w = LeafWalker::new(engine.clone(), &mut sm, false);
        let mut v = CollectLeaves::new();
        let mut path = vec![0];
        w.walk::<CollectLeaves, BlockTime>(&mut path, &mut v, *r)?;

        map.insert(*r, v.leaves);
    }

    Ok(map)
}

//------------------------------------------

fn build_metadata(ctx: &Context, sb: &Superblock) -> Result<Metadata> {
    let report = &ctx.report;
    let engine = &ctx.engine;
    let mut path = vec![0];

    report.set_title("Reading device details");
    let details = btree_to_map::<DeviceDetail>(&mut path, engine.clone(), true, sb.details_root)?;

    report.set_title("Reading mappings roots");
    let roots;
    {
        let sm = Arc::new(Mutex::new(RestrictedSpaceMap::new(engine.get_nr_blocks())));
        roots =
            btree_to_map_with_path::<u64>(&mut path, engine.clone(), sm, true, sb.mapping_root)?;
    }

    report.set_title(&format!("Collecting leaves for {} roots", roots.len()));
    let mapping_roots = roots.values().map(|(_, root)| *root).collect();
    let entry_map = collect_leaves(engine.clone(), &mapping_roots)?;

    let defs = Vec::new();
    let mut devs = Vec::new();
    for (thin_id, (_path, root)) in roots {
        let id = thin_id as u64;
        let detail = details.get(&id).expect("couldn't find device details");
        let es = entry_map.get(&root).unwrap();
        let kr = KeyRange::new(); // FIXME: finish
        devs.push(Device {
            thin_id: thin_id as u32,
            detail: *detail,
            map: Mapping {
                kr,
                entries: es.to_vec(),
            },
        });
    }

    Ok(Metadata { defs, devs })
}

//------------------------------------------

fn gather_entries(g: &mut Gatherer, es: &[Entry]) {
    g.new_seq();
    for e in es {
        match e {
            Entry::Leaf(b) => {
                g.next(*b);
            }
            Entry::Ref(_id) => {
                g.new_seq();
            }
        }
    }
}

fn build_runs(devs: &[Device]) -> BTreeMap<u64, Vec<u64>> {
    let mut g = Gatherer::new();

    for d in devs {
        gather_entries(&mut g, &d.map.entries);
    }

    // The runs become defs that just contain leaves.
    let mut runs = BTreeMap::new();
    for run in g.gather() {
        runs.insert(run[0], run);
    }

    runs
}

fn entries_to_runs(runs: &BTreeMap<u64, Vec<u64>>, es: &[Entry]) -> Vec<Entry> {
    use Entry::*;

    let mut result = Vec::new();
    let mut entry_index = 0;
    while entry_index < es.len() {
        match es[entry_index] {
            Ref(id) => {
                result.push(Ref(id));
                entry_index += 1;
            }
            Leaf(b) => {
                if let Some(run) = runs.get(&b) {
                    result.push(Ref(b));
                    entry_index += run.len();
                } else {
                    result.push(Leaf(b));
                    entry_index += 1;
                }
            }
        }
    }

    result
}

fn build_defs(runs: BTreeMap<u64, Vec<u64>>) -> Vec<Def> {
    let mut defs = Vec::new();
    for (head, run) in runs.iter() {
        let kr = KeyRange::new();
        let entries: Vec<Entry> = run.iter().map(|b| Entry::Leaf(*b)).collect();
        defs.push(Def {
            def_id: *head,
            map: Mapping { kr, entries },
        });
    }

    defs
}

// FIXME: do we really need to track kr?
// FIXME: I think this may be better done as part of restore.
fn optimise_metadata(md: Metadata) -> Result<Metadata> {
    let runs = build_runs(&md.devs);
    eprintln!("{} runs", runs.len());

    // Expand old devs to use the new atomic runs
    let mut devs = Vec::new();
    for d in &md.devs {
        let kr = KeyRange::new();
        let entries = entries_to_runs(&runs, &d.map.entries);
        devs.push(Device {
            thin_id: d.thin_id,
            detail: d.detail,
            map: Mapping { kr, entries },
        });
    }

    let defs = build_defs(runs);

    Ok(Metadata { defs, devs })
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

    let node = unpack_node::<BlockTime>(&path, &b.get_data(), true, true)?;

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

fn emit_leaves(ctx: &Context, out: &mut dyn MetadataVisitor, ls: &[u64]) -> Result<()> {
    let mut v = MappingVisitor::new(out);
    let proc = |b| {
        emit_leaf(&mut v, &b)?;
        Ok(())
    };

    read_for(ctx.engine.clone(), ls, proc)?;
    v.end_walk().map_err(|_| anyhow!("failed to emit leaves"))
}

fn emit_entries<W: Write>(
    ctx: &Context,
    out: &mut xml::XmlWriter<W>,
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
                    emit_leaves(&ctx, out, &leaves[0..])?;
                    leaves.clear();
                }
                let str = format!("{}", id);
                out.ref_shared(&str)?;
            }
        }
    }

    if !leaves.is_empty() {
        emit_leaves(&ctx, out, &leaves[0..])?;
    }

    Ok(())
}

fn dump_metadata(ctx: &Context, w: &mut dyn Write, sb: &Superblock, md: &Metadata) -> Result<()> {
    let data_root = unpack::<SMRoot>(&sb.data_sm_root[0..])?;
    let mut out = xml::XmlWriter::new(w);
    let xml_sb = ir::Superblock {
        uuid: "".to_string(),
        time: sb.time,
        transaction: sb.transaction_id,
        flags: None,
        version: Some(2),
        data_block_size: sb.data_block_size,
        nr_data_blocks: data_root.nr_blocks,
        metadata_snap: None,
    };
    out.superblock_b(&xml_sb)?;

    ctx.report.set_title("Dumping shared regions");
    for d in &md.defs {
        out.def_shared_b(&format!("{}", d.def_id))?;
        emit_entries(ctx, &mut out, &d.map.entries)?;
        out.def_shared_e()?;
    }

    ctx.report.set_title("Dumping devices");
    for dev in &md.devs {
        let device = ir::Device {
            dev_id: dev.thin_id,
            mapped_blocks: dev.detail.mapped_blocks,
            transaction: dev.detail.transaction_id,
            creation_time: dev.detail.creation_time,
            snap_time: dev.detail.snapshotted_time,
        };
        out.device_b(&device)?;
        emit_entries(ctx, &mut out, &dev.map.entries)?;
        out.device_e()?;
    }
    out.superblock_e()?;
    out.eof()?;

    Ok(())
}

//------------------------------------------

pub fn dump(opts: ThinDumpOptions) -> Result<()> {
    let ctx = mk_context(&opts)?;
    let sb = read_superblock(ctx.engine.as_ref(), SUPERBLOCK_LOCATION)?;
    let md = build_metadata(&ctx, &sb)?;

    ctx.report
        .set_title("Optimising metadata to improve leaf packing");
    let md = optimise_metadata(md)?;

    let mut writer: Box<dyn Write>;
    if opts.output.is_some() {
        writer = Box::new(BufWriter::new(File::create(opts.output.unwrap())?));
    } else {
        writer = Box::new(BufWriter::new(std::io::stdout()));
    }
    dump_metadata(&ctx, &mut writer, &sb, &md)
}

//------------------------------------------
