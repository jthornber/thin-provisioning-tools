use anyhow::{anyhow, Result};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::{Arc, Mutex};
use threadpool::ThreadPool;

use crate::checksum;
use crate::commands::engine::*;
use crate::io_engine::*;
use crate::pdata::btree::{self, *};
use crate::pdata::btree_walker::*;
use crate::pdata::space_map::checker::*;
use crate::pdata::space_map::common::*;
use crate::pdata::space_map::*;
use crate::pdata::unpack::*;
use crate::report::*;
use crate::thin::block_time::*;
use crate::thin::device_detail::*;
use crate::thin::superblock::*;

//------------------------------------------

struct BottomLevelVisitor {
    data_sm: ASpaceMap,
}

//------------------------------------------

impl NodeVisitor<BlockTime> for BottomLevelVisitor {
    fn visit(
        &self,
        _path: &[u64],
        _kr: &KeyRange,
        _h: &NodeHeader,
        _k: &[u64],
        values: &[BlockTime],
    ) -> btree::Result<()> {
        // FIXME: do other checks

        if values.is_empty() {
            return Ok(());
        }

        let mut data_sm = self.data_sm.lock().unwrap();

        let mut start = values[0].block;
        let mut len = 1;

        for b in values.iter().skip(1) {
            let block = b.block;
            if block == start + len {
                len += 1;
            } else {
                data_sm.inc(start, len).unwrap();
                start = block;
                len = 1;
            }
        }

        data_sm.inc(start, len).unwrap();
        Ok(())
    }

    fn visit_again(&self, _path: &[u64], _b: u64) -> btree::Result<()> {
        Ok(())
    }

    fn end_walk(&self) -> btree::Result<()> {
        Ok(())
    }
}

//------------------------------------------

fn inc_superblock(sm: &ASpaceMap) -> Result<()> {
    let mut sm = sm.lock().unwrap();
    sm.inc(SUPERBLOCK_LOCATION, 1)?;
    Ok(())
}

//------------------------------------------

pub struct ThinCheckOptions<'a> {
    pub input: &'a Path,
    pub engine_opts: EngineOptions,
    pub sb_only: bool,
    pub skip_mappings: bool,
    pub ignore_non_fatal: bool,
    pub auto_repair: bool,
    pub clear_needs_check: bool,
    pub override_mapping_root: Option<u64>,
    pub report: Arc<Report>,
}

struct Context {
    report: Arc<Report>,
    engine: Arc<dyn IoEngine + Send + Sync>,
    pool: ThreadPool,
}

//----------------------------------------

// BTree nodes can get scattered across the metadata device.  Which can
// result in a lot of seeks on spindle devices if we walk the trees in
// depth first order.  To get around this we walk the upper levels of
// the btrees to build a list of the leaf nodes.  Then process the leaf
// nodes in location order.

// We know the metadata area is limited to 16G, so u32 is large enough
// hold block numbers.

#[allow(dead_code)]
enum NodeInfo {
    Internal {
        keys: KeyRange,

        // Errors found in _this_ node only; children may have errors
        error: Option<anyhow::Error>,
        children_are_leaves: bool,
        children: Vec<u32>,
    },
    Leaf {
        keys: KeyRange,
    },
}

/// block_nr -> node info
type NodeMap = BTreeMap<u32, NodeInfo>;

// FIXME: add context to errors

fn verify_checksum(b: &Block) -> Result<()> {
    let bt = checksum::metadata_block_type(b.get_data());
    if bt != checksum::BT::NODE {
        return Err(anyhow!("corrupt block: checksum failed"));
    }
    Ok(())
}

fn is_seen(
    loc: u32,
    metadata_sm: &Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    nodes: &mut NodeMap,
) -> bool {
    let mut sm = metadata_sm.lock().unwrap();
    sm.inc(loc as u64, 1).expect("space map inc failed");
    nodes.contains_key(&loc)
}

#[allow(clippy::too_many_arguments)]
fn read_node_(
    ctx: &Context,
    metadata_sm: &Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    b: &Block,
    depth: usize,
    kr: &KeyRange,
    ignore_non_fatal: bool,
    is_root: bool,
    nodes: &mut NodeMap,
) -> Result<NodeInfo> {
    verify_checksum(b)?;

    // FIXME: use proper path, actually can we recreate the path from the node info?
    let path = Vec::new();
    let node = unpack_node::<u64>(&path, b.get_data(), ignore_non_fatal, is_root)?;

    use btree::Node::*;
    if let Internal { keys, values, .. } = node {
        let children = values.iter().map(|v| *v as u32).collect::<Vec<u32>>();
        let child_keys = split_key_ranges(&path, kr, &keys)?;

        // filter out previously visited nodes
        let mut new_values = Vec::with_capacity(values.len());
        let mut new_keys = Vec::with_capacity(child_keys.len());
        for i in 0..values.len() {
            if !is_seen(values[i] as u32, metadata_sm, nodes) {
                new_values.push(values[i]);
                new_keys.push(child_keys[i].clone());
            }
        }
        let values = new_values;
        let child_keys = new_keys;

        if depth == 0 {
            let mut sm = metadata_sm.lock().unwrap();
            for (loc, kr) in values.iter().zip(child_keys) {
                sm.inc(*loc, 1).expect("space map inc failed");
                nodes.insert(*loc as u32, NodeInfo::Leaf { keys: kr.clone() });
            }
        } else {
            // we could error each child rather than the current node
            let bs = ctx.engine.read_many(&values)?;

            for (i, (b, kr)) in bs.iter().zip(child_keys).enumerate() {
                if let Ok(b) = b {
                    read_node(
                        ctx,
                        metadata_sm,
                        b,
                        depth - 1,
                        &kr,
                        ignore_non_fatal,
                        false,
                        nodes,
                    );
                } else {
                    nodes.insert(
                        values[i] as u32,
                        NodeInfo::Internal {
                            keys: kr.clone(),
                            error: Some(anyhow!("io error")),
                            children_are_leaves: depth == 0,
                            children: Vec::new(),
                        },
                    );
                }
            }
        }

        Ok(NodeInfo::Internal {
            keys: kr.clone(),
            error: None,
            children_are_leaves: depth == 0,
            children,
        })
    } else {
        Err(anyhow!("btree nodes are not all at the same depth."))
    }
}

/// Reads a btree node and all internal btree nodes below it into the
/// nodes parameter.  No errors are returned, instead the optional
/// error field of the nodes will be filled in.
#[allow(clippy::too_many_arguments)]
fn read_node(
    ctx: &Context,
    metadata_sm: &Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    b: &Block,
    depth: usize,
    keys: &KeyRange,
    ignore_non_fatal: bool,
    is_root: bool,
    nodes: &mut NodeMap,
) {
    let block_nr = b.loc as u32;
    match read_node_(
        ctx,
        metadata_sm,
        b,
        depth,
        keys,
        ignore_non_fatal,
        is_root,
        nodes,
    ) {
        Err(e) => {
            nodes.insert(
                block_nr,
                NodeInfo::Internal {
                    keys: keys.clone(),
                    error: Some(e),
                    children_are_leaves: depth == 0,
                    children: Vec::new(),
                },
            );
        }
        Ok(n) => {
            nodes.insert(block_nr, n);
        }
    }
}

/// Gets the depth of a bottom level mapping tree.  0 means the root is a leaf node.
// FIXME: what if there's an error on the path to the leftmost leaf?
fn get_depth(ctx: &Context, path: &mut Vec<u64>, root: u64, is_root: bool) -> Result<usize> {
    use Node::*;

    let b = ctx.engine.read(root).map_err(|_| io_err(path))?;
    verify_checksum(&b)?;

    let node = unpack_node::<BlockTime>(path, b.get_data(), true, is_root)?;

    match node {
        Internal { values, .. } => {
            let n = get_depth(ctx, path, values[0], false)?;
            Ok(n + 1)
        }
        Leaf { .. } => Ok(0),
    }
}

fn read_internal_nodes(
    ctx: &Context,
    metadata_sm: &Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    root: u32,
    ignore_non_fatal: bool,
    nodes: &mut NodeMap,
) {
    let keys = KeyRange::new();
    if is_seen(root, metadata_sm, nodes) {
        return;
    }

    let mut path = Vec::new();
    // FIXME: make get-depth more resilient
    let depth = get_depth(ctx, &mut path, root as u64, true).expect("get_depth failed");

    if depth == 0 {
        nodes.insert(
            root as u32,
            NodeInfo::Leaf {
                keys: KeyRange::new(),
            },
        );
        return;
    }

    if let Ok(b) = ctx.engine.read(root as u64) {
        read_node(
            ctx,
            metadata_sm,
            &b,
            depth - 1,
            &keys,
            ignore_non_fatal,
            true,
            nodes,
        );
    } else {
        // FIXME: factor out common code
        nodes.insert(
            root,
            NodeInfo::Internal {
                keys: keys.clone(),
                error: Some(anyhow!("io error")),
                children_are_leaves: depth == 0,
                children: Vec::new(),
            },
        );
    }
}

// Check the mappings filling in the data_sm as we go.
fn check_mapping_bottom_level(
    ctx: &Context,
    metadata_sm: &Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    data_sm: &Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    roots: &BTreeMap<u64, (Vec<u64>, u64)>,
    ignore_non_fatal: bool,
) -> Result<()> {
    ctx.report.set_sub_title("mapping tree");

    let mut nodes = BTreeMap::new();
    let mut tree_roots = BTreeSet::new();

    for (_path, root) in roots.values() {
        tree_roots.insert(*root);
        read_internal_nodes(ctx, metadata_sm, *root as u32, ignore_non_fatal, &mut nodes);
    }

    // Build a vec of the leaf locations.  These will be in disk location
    // order.
    // FIXME: use with_capacity
    let mut leaves = Vec::new();
    for (loc, info) in nodes {
        if let NodeInfo::Leaf { keys: _ } = info {
            leaves.push(loc as u64);
        }
    }

    // Process chunks of leaves at once so the io engine can aggregate reads.
    let mut chunk_start = 0;
    let leaves = Arc::new(leaves);
    let tree_roots = Arc::new(tree_roots);
    while chunk_start < leaves.len() {
        let len = std::cmp::min(16 * 1024, leaves.len() - chunk_start);
        let engine = ctx.engine.clone();
        let data_sm = data_sm.clone();
        let leaves = leaves.clone();
        let tree_roots = tree_roots.clone();

        ctx.pool.execute(move || {
            let c = &leaves[chunk_start..(chunk_start + len)];
            let blocks = engine.read_many(c).expect("lazy");
            for (loc, b) in c.iter().zip(blocks) {
                let b = b.expect("lazy");
                verify_checksum(&b).expect("lazy programmer");
                let is_root = tree_roots.contains(loc);

                let path = Vec::new();
                let node = unpack_node::<BlockTime>(&path, b.get_data(), ignore_non_fatal, is_root)
                    .expect("lazy");
                match node {
                    Node::Leaf { values, .. } => {
                        // FIXME: check keys are within range
                        let mut data_sm = data_sm.lock().unwrap();
                        for v in values {
                            data_sm.inc(v.block, 1).expect("data_sm.inc() failed");
                        }
                    }
                    _ => {
                        panic!("node changed it's type under me");
                    }
                }
            }
        });
        chunk_start += len;
    }
    ctx.pool.join();

    /*
    let w = Arc::new(BTreeWalker::new_with_sm(
        ctx.engine.clone(),
        metadata_sm.clone(),
        ignore_non_fatal,
    )?);

    // We want to print out errors as we progress, so we aggregate for each thin and print
    // at that point.
    let mut failed = false;

    if roots.len() > 64 {
        let errs = Arc::new(Mutex::new(Vec::new()));
        for (path, root) in roots.values() {
            let data_sm = data_sm.clone();
            let root = *root;
            let v = BottomLevelVisitor { data_sm };
            let w = w.clone();
            let mut path = path.clone();
            let errs = errs.clone();

            ctx.pool.execute(move || {
                if let Err(e) = w.walk(&mut path, &v, root) {
                    let mut errs = errs.lock().unwrap();
                    errs.push(e);
                }
            });
        }
        ctx.pool.join();
        let errs = Arc::try_unwrap(errs).unwrap().into_inner().unwrap();
        if !errs.is_empty() {
            ctx.report.fatal(&format!("{}", aggregate_error(errs)));
            failed = true;
        }
    } else {
        for (path, root) in roots.values() {
            let w = w.clone();
            let data_sm = data_sm.clone();
            let root = *root;
            let v = Arc::new(BottomLevelVisitor { data_sm });
            let mut path = path.clone();

            if let Err(e) = walk_threaded(&mut path, w, &ctx.pool, v, root) {
                failed = true;
                ctx.report.fatal(&format!("{}", e));
            }
        }
    }

    if failed {
        Err(anyhow!("Check of mappings failed"))
    } else {
        Ok(())
    }
    */
    Ok(())
}

fn read_sb(opts: &ThinCheckOptions, engine: Arc<dyn IoEngine + Sync + Send>) -> Result<Superblock> {
    // superblock
    let sb = if opts.engine_opts.use_metadata_snap {
        read_superblock_snap(engine.as_ref())?
    } else {
        read_superblock(engine.as_ref(), SUPERBLOCK_LOCATION)?
    };
    Ok(sb)
}

fn mk_context_(engine: Arc<dyn IoEngine + Send + Sync>, report: Arc<Report>) -> Result<Context> {
    // let nr_threads = engine.suggest_nr_threads();
    let nr_threads = 16;
    let pool = ThreadPool::new(nr_threads);

    Ok(Context {
        report,
        engine,
        pool,
    })
}

fn mk_context(opts: &ThinCheckOptions) -> Result<Context> {
    let engine = EngineBuilder::new(opts.input, &opts.engine_opts)
        .write(opts.auto_repair || opts.clear_needs_check)
        .build()?;
    mk_context_(engine, opts.report.clone())
}

fn print_info(sb: &Superblock, report: Arc<Report>) -> Result<()> {
    let root = unpack::<SMRoot>(&sb.metadata_sm_root[0..])?;
    report.to_stdout(&format!("TRANSACTION_ID={}", sb.transaction_id));
    report.to_stdout(&format!(
        "METADATA_FREE_BLOCKS={}",
        root.nr_blocks - root.nr_allocated
    ));
    Ok(())
}

pub fn check(opts: ThinCheckOptions) -> Result<()> {
    let ctx = mk_context(&opts)?;

    // FIXME: temporarily get these out
    let report = &ctx.report;
    let engine = &ctx.engine;

    let mut sb = read_sb(&opts, engine.clone())?;
    let _ = print_info(&sb, report.clone());

    report.set_title("Checking thin metadata");

    sb.mapping_root = opts.override_mapping_root.unwrap_or(sb.mapping_root);

    if opts.sb_only {
        if opts.clear_needs_check {
            let cleared = clear_needs_check_flag(ctx.engine.clone())?;
            if cleared {
                ctx.report.info("Cleared needs_check flag");
            }
        }
        return Ok(());
    }

    let metadata_root = unpack::<SMRoot>(&sb.metadata_sm_root[0..])?;
    let mut path = vec![0];

    // Device details.   We read this once to get the number of thin devices, and hence the
    // maximum metadata ref count.  Then create metadata space map, and reread to increment
    // the ref counts for that metadata.
    let devs = btree_to_map::<DeviceDetail>(
        &mut path,
        engine.clone(),
        opts.ignore_non_fatal,
        sb.details_root,
    )?;
    let nr_devs = devs.len();
    let metadata_sm = core_sm(engine.get_nr_blocks(), nr_devs as u32);
    inc_superblock(&metadata_sm)?;

    report.set_sub_title("device details tree");
    let _devs = btree_to_map_with_sm::<DeviceDetail>(
        &mut path,
        engine.clone(),
        metadata_sm.clone(),
        opts.ignore_non_fatal,
        sb.details_root,
    )?;

    let mon_sm = metadata_sm.clone();
    let monitor = ProgressMonitor::new(report.clone(), metadata_root.nr_allocated, move || {
        mon_sm.lock().unwrap().get_nr_allocated().unwrap()
    });

    // mapping top level
    report.set_sub_title("mapping tree");
    let roots = btree_to_map_with_path::<u64>(
        &mut path,
        engine.clone(),
        metadata_sm.clone(),
        opts.ignore_non_fatal,
        sb.mapping_root,
    )?;

    if opts.skip_mappings {
        let cleared = clear_needs_check_flag(ctx.engine.clone())?;
        if cleared {
            ctx.report.info("Cleared needs_check flag");
        }
        return Ok(());
    }

    // mapping bottom level
    let root = unpack::<SMRoot>(&sb.data_sm_root[0..])?;
    let data_sm = core_sm(root.nr_blocks, nr_devs as u32);
    check_mapping_bottom_level(&ctx, &metadata_sm, &data_sm, &roots, opts.ignore_non_fatal)?;

    // trees in metadata snap
    if sb.metadata_snap > 0 {
        {
            let mut metadata_sm = metadata_sm.lock().unwrap();
            metadata_sm.inc(sb.metadata_snap, 1)?;
        }
        let sb_snap = read_superblock(engine.as_ref(), sb.metadata_snap)?;

        // device details
        btree_to_map_with_sm::<DeviceDetail>(
            &mut path,
            engine.clone(),
            metadata_sm.clone(),
            opts.ignore_non_fatal,
            sb_snap.details_root,
        )?;

        // mapping top level
        let roots_snap = btree_to_map_with_path::<u64>(
            &mut path,
            engine.clone(),
            metadata_sm.clone(),
            opts.ignore_non_fatal,
            sb_snap.mapping_root,
        )?;

        // mapping bottom level
        check_mapping_bottom_level(
            &ctx,
            &metadata_sm,
            &data_sm,
            &roots_snap,
            opts.ignore_non_fatal,
        )?;
    }

    //-----------------------------------------

    report.set_sub_title("data space map");
    let root = unpack::<SMRoot>(&sb.data_sm_root[0..])?;
    let data_leaks = check_disk_space_map(
        engine.clone(),
        report.clone(),
        root,
        data_sm.clone(),
        metadata_sm.clone(),
        opts.ignore_non_fatal,
    )?;

    //-----------------------------------------

    report.set_sub_title("metadata space map");
    let root = unpack::<SMRoot>(&sb.metadata_sm_root[0..])?;

    // Now the counts should be correct and we can check it.
    let metadata_leaks = check_metadata_space_map(
        engine.clone(),
        report.clone(),
        root,
        metadata_sm.clone(),
        opts.ignore_non_fatal,
    )?;

    //-----------------------------------------

    if (opts.auto_repair || opts.clear_needs_check)
        && (opts.engine_opts.use_metadata_snap || opts.override_mapping_root.is_some())
    {
        return Err(anyhow!("cannot perform repair outside the actual metadata"));
    }

    if !data_leaks.is_empty() {
        if opts.auto_repair {
            ctx.report.info("Repairing data leaks.");
            repair_space_map(ctx.engine.clone(), data_leaks, data_sm.clone())?;
        } else if !opts.ignore_non_fatal {
            return Err(anyhow!("data space map contains leaks"));
        }
    }

    if !metadata_leaks.is_empty() {
        if opts.auto_repair {
            ctx.report.info("Repairing metadata leaks.");
            repair_space_map(ctx.engine.clone(), metadata_leaks, metadata_sm.clone())?;
        } else if !opts.ignore_non_fatal {
            return Err(anyhow!("metadata space map contains leaks"));
        }
    }

    if opts.auto_repair || opts.clear_needs_check {
        let cleared = clear_needs_check_flag(ctx.engine.clone())?;
        if cleared {
            ctx.report.info("Cleared needs_check flag");
        }
    }

    monitor.stop();

    Ok(())
}

pub fn clear_needs_check_flag(engine: Arc<dyn IoEngine + Send + Sync>) -> Result<bool> {
    let mut sb = read_superblock(engine.as_ref(), SUPERBLOCK_LOCATION)?;
    if !sb.flags.needs_check {
        return Ok(false);
    }
    sb.flags.needs_check = false;
    write_superblock(engine.as_ref(), SUPERBLOCK_LOCATION, &sb).map(|_| true)
}

//------------------------------------------

// Some callers wish to know which blocks are allocated.
pub struct CheckMaps {
    pub metadata_sm: Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    pub data_sm: Arc<Mutex<dyn SpaceMap + Send + Sync>>,
}

pub fn check_with_maps(
    engine: Arc<dyn IoEngine + Send + Sync>,
    report: Arc<Report>,
) -> Result<CheckMaps> {
    let ctx = mk_context_(engine.clone(), report.clone())?;
    report.set_title("Checking thin metadata");

    // superblock
    let sb = read_superblock(engine.as_ref(), SUPERBLOCK_LOCATION)?;

    let metadata_root = unpack::<SMRoot>(&sb.metadata_sm_root[0..])?;
    let mut path = vec![0];

    // Device details.   We read this once to get the number of thin devices, and hence the
    // maximum metadata ref count.  Then create metadata space map, and reread to increment
    // the ref counts for that metadata.
    let devs = btree_to_map::<DeviceDetail>(&mut path, engine.clone(), false, sb.details_root)?;
    let nr_devs = devs.len();
    let metadata_sm = core_sm(engine.get_nr_blocks(), nr_devs as u32);
    inc_superblock(&metadata_sm)?;

    report.set_sub_title("device details tree");
    let _devs = btree_to_map_with_sm::<DeviceDetail>(
        &mut path,
        engine.clone(),
        metadata_sm.clone(),
        false,
        sb.details_root,
    )?;

    let mon_sm = metadata_sm.clone();
    let monitor = ProgressMonitor::new(report.clone(), metadata_root.nr_allocated, move || {
        mon_sm.lock().unwrap().get_nr_allocated().unwrap()
    });

    // mapping top level
    report.set_sub_title("mapping tree");
    let roots = btree_to_map_with_path::<u64>(
        &mut path,
        engine.clone(),
        metadata_sm.clone(),
        false,
        sb.mapping_root,
    )?;

    // mapping bottom level
    let root = unpack::<SMRoot>(&sb.data_sm_root[0..])?;
    let data_sm = core_sm(root.nr_blocks, nr_devs as u32);
    check_mapping_bottom_level(&ctx, &metadata_sm, &data_sm, &roots, false)?;

    //-----------------------------------------

    report.set_sub_title("data space map");
    let root = unpack::<SMRoot>(&sb.data_sm_root[0..])?;
    let _data_leaks = check_disk_space_map(
        engine.clone(),
        report.clone(),
        root,
        data_sm.clone(),
        metadata_sm.clone(),
        false,
    )?;

    //-----------------------------------------

    report.set_sub_title("metadata space map");
    let root = unpack::<SMRoot>(&sb.metadata_sm_root[0..])?;

    // Now the counts should be correct and we can check it.
    let _metadata_leaks =
        check_metadata_space_map(engine.clone(), report, root, metadata_sm.clone(), false)?;

    //-----------------------------------------

    monitor.stop();

    Ok(CheckMaps {
        metadata_sm: metadata_sm.clone(),
        data_sm: data_sm.clone(),
    })
}

//------------------------------------------
