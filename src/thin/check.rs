use anyhow::{anyhow, Result};
use fixedbitset::FixedBitSet;
use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;
use std::sync::mpsc::{self, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread;

use crate::commands::engine::*;
use crate::hashvec::HashVec;
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
use crate::thin::metadata_repair::is_superblock_consistent;
use crate::thin::superblock::*;

//------------------------------------------

// minimum number of entries of a node with 64-bit mapped type
const MIN_ENTRIES: u8 = 84;

fn inc_superblock(sm: &ASpaceMap, metadata_snap: u64) -> Result<()> {
    let mut sm = sm.lock().unwrap();
    sm.inc(SUPERBLOCK_LOCATION, 1)?;
    if metadata_snap > 0 {
        sm.inc(metadata_snap, 1)?;
    }
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
    pub override_details_root: Option<u64>,
    pub report: Arc<Report>,
}

struct Context {
    report: Arc<Report>,
    engine: Arc<dyn IoEngine + Send + Sync>,
}

//----------------------------------------

// BTree nodes can get scattered across the metadata device.  Which can
// result in a lot of seeks on spindle devices if we walk the trees in
// depth first order.  To get around this we walk the upper levels of
// the btrees to build a list of the leaf nodes.  Then process the leaf
// nodes in location order.

// We know the metadata area is limited to 16G, so u32 is large enough
// hold block numbers.

#[derive(Debug, Clone)]
struct InternalNodeInfo {
    keys: Vec<u64>,
    children: Vec<u32>,
}

#[derive(Debug, Clone, Default)]
struct NodeSummary {
    key_low: u64,     // min mapped block
    key_high: u64,    // max mapped block, inclusive
    nr_mappings: u64, // number of valid mappings in this subtree
    nr_entries: u8,   // number of entries in this node, up to 252 given it is the mapping tree
    nr_errors: u8,    // number of errors found in this subtree, up to 255
}

impl NodeSummary {
    fn from_leaf(keys: &[u64]) -> Self {
        let nr_entries = keys.len();
        let key_low = if nr_entries > 0 { keys[0] } else { 0 };
        let key_high = if nr_entries > 0 {
            keys[nr_entries - 1]
        } else {
            0
        };

        NodeSummary {
            key_low,
            key_high,
            nr_mappings: nr_entries as u64,
            nr_entries: nr_entries as u8,
            nr_errors: 0,
        }
    }

    fn error() -> Self {
        Self {
            key_low: 0,
            key_high: 0,
            nr_mappings: 0,
            nr_entries: 0,
            nr_errors: 1,
        }
    }

    fn append(&mut self, child: &NodeSummary) -> anyhow::Result<()> {
        if self.nr_mappings == 0 {
            self.key_low = child.key_low;
            self.key_high = child.key_high;
        } else if child.nr_mappings > 0 {
            if child.key_low <= self.key_high {
                return Err(anyhow!("overlapped keys"));
            }
            self.key_high = child.key_high;
        }
        self.nr_mappings += child.nr_mappings;
        self.nr_entries += 1;
        self.nr_errors = self.nr_errors.saturating_add(child.nr_errors);

        Ok(())
    }
}

#[derive(PartialEq)]
enum NodeType {
    None,
    Internal,
    Leaf,
    Error,
}

#[derive(Debug)]
struct NodeMap {
    node_type: FixedBitSet,
    leaf_nodes: FixedBitSet, // FIXME: remove this one
    nr_leaves: u32,
    internal_info: HashVec<InternalNodeInfo>,

    // Stores errors of the node itself; errors in children are not included
    node_errors: HashVec<NodeError>,
}

impl NodeMap {
    fn new(nr_blocks: u32) -> NodeMap {
        NodeMap {
            node_type: FixedBitSet::with_capacity((nr_blocks as usize) * 2),
            leaf_nodes: FixedBitSet::with_capacity(nr_blocks as usize),
            nr_leaves: 0,
            internal_info: HashVec::new(),
            node_errors: HashVec::new(),
        }
    }

    fn get_type(&self, blocknr: u32) -> NodeType {
        // FIXME: query two bits at once
        let lsb = self.node_type.contains(blocknr as usize * 2);
        let msb = self.node_type.contains(blocknr as usize * 2 + 1);
        if !lsb && msb {
            NodeType::Error
        } else if lsb && !msb {
            NodeType::Leaf
        } else if lsb && msb {
            NodeType::Internal
        } else {
            NodeType::None
        }
    }

    fn set_type_(&mut self, blocknr: u32, t: NodeType) {
        match t {
            NodeType::Leaf => {
                self.node_type.insert(blocknr as usize * 2);
                self.leaf_nodes.insert(blocknr as usize);
                self.nr_leaves += 1;
            }
            NodeType::Internal => {
                // FIXME: update two bits at once
                self.node_type.insert(blocknr as usize * 2);
                self.node_type.insert(blocknr as usize * 2 + 1);
                if self.leaf_nodes.contains(blocknr as usize) {
                    self.leaf_nodes.toggle(blocknr as usize);
                    self.nr_leaves -= 1;
                }
            }
            NodeType::Error => {
                // FIXME: update two bits at once
                self.node_type.insert(blocknr as usize * 2 + 1);
                if self.leaf_nodes.contains(blocknr as usize) {
                    self.node_type.toggle(blocknr as usize * 2);
                    self.leaf_nodes.toggle(blocknr as usize);
                    self.nr_leaves -= 1;
                }
            }
            _ => {}
        }
    }

    fn insert_internal_node(&mut self, blocknr: u32, info: InternalNodeInfo) -> Result<()> {
        // Only accepts converting a potential unread leaf
        let node_type = self.get_type(blocknr);
        if node_type != NodeType::None && node_type != NodeType::Leaf {
            return Err(anyhow!("type changed"));
        }
        self.internal_info.insert(blocknr, info);
        self.set_type_(blocknr, NodeType::Internal);
        Ok(())
    }

    fn insert_leaf(&mut self, blocknr: u32) -> Result<()> {
        // Only accepts an unread block
        if self.get_type(blocknr) != NodeType::None {
            return Err(anyhow!("type changed"));
        }
        self.set_type_(blocknr, NodeType::Leaf);
        Ok(())
    }

    fn insert_error(&mut self, blocknr: u32, e: NodeError) -> Result<()> {
        // Only accepts converting a potential unread leaf
        let node_type = self.get_type(blocknr);
        if node_type != NodeType::None && node_type != NodeType::Leaf {
            return Err(anyhow!("type changed"));
        }
        self.node_errors.insert(blocknr, e);
        self.set_type_(blocknr, NodeType::Error);
        Ok(())
    }

    // Returns total number of nodes found
    fn len(&self) -> u32 {
        self.internal_info.len() as u32 + self.nr_leaves
    }
}

fn is_seen(loc: u32, metadata_sm: &Arc<Mutex<dyn SpaceMap + Send + Sync>>) -> Result<bool> {
    let mut sm = metadata_sm.lock().unwrap();
    sm.inc(loc as u64, 1)?;
    Ok(sm.get(loc as u64).unwrap_or(0) > 1)
}

// FIXME: split up this function
fn read_node_(
    ctx: &Context,
    metadata_sm: &Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    b: &Block,
    depth: usize,
    ignore_non_fatal: bool,
    nodes: &mut NodeMap,
) {
    // allow underfull nodes in the first pass
    let node = match check_and_unpack_node::<u64>(b, ignore_non_fatal, true) {
        Ok(n) => n,
        Err(e) => {
            // theoretically never fail
            let _ = nodes.insert_error(b.loc as u32, e);
            return;
        }
    };

    use btree::Node::*;
    if let Internal { keys, values, .. } = node {
        let children = values.iter().map(|v| *v as u32).collect::<Vec<u32>>(); // FIXME: slow

        // insert the node info in pre-order fashion to better detect loops in the path
        let info = InternalNodeInfo { keys, children };

        let _ = nodes.insert_internal_node(b.loc as u32, info);

        // filter out previously visited nodes
        let mut new_values = Vec::with_capacity(values.len());
        for v in values {
            if let Ok(seen) = is_seen(v as u32, metadata_sm) {
                // Add the unread leaf if now it looks like an internal.
                // Add the visited internal if now it looks like a leaf.
                let node_type = nodes.get_type(v as u32);
                let maybe_internal = depth > 0 && node_type == NodeType::Leaf;
                let maybe_leaf = depth == 0 && node_type == NodeType::None;
                if !seen || maybe_internal || maybe_leaf {
                    new_values.push(v);
                }
            }
        }
        let values = new_values;

        if depth == 0 {
            for loc in values {
                let _ = nodes.insert_leaf(loc as u32);
            }
        } else {
            // we could error each child rather than the current node
            match ctx.engine.read_many(&values) {
                Ok(bs) => {
                    for (i, b) in bs.iter().enumerate() {
                        if let Ok(b) = b {
                            read_node(ctx, metadata_sm, b, depth - 1, ignore_non_fatal, nodes);
                        } else {
                            // theoretically never fail
                            let _ = nodes.insert_error(values[i] as u32, NodeError::IoError);
                        }
                    }
                }
                Err(_) => {
                    // error every child node
                    for loc in values {
                        // theoretically never fail
                        let _ = nodes.insert_error(loc as u32, NodeError::IoError);
                    }
                }
            };
        }
    }
}

/// Reads a btree node and all internal btree nodes below it into the
/// nodes parameter.  No errors are returned, instead the optional
/// error field of the nodes will be filled in.
fn read_node(
    ctx: &Context,
    metadata_sm: &Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    b: &Block,
    depth: usize,
    ignore_non_fatal: bool,
    nodes: &mut NodeMap,
) {
    read_node_(ctx, metadata_sm, b, depth, ignore_non_fatal, nodes);
}

/// Gets the depth of a bottom level mapping tree.  0 means the root is a leaf node.
// FIXME: what if there's an error on the path to the leftmost leaf?
fn get_depth(ctx: &Context, path: &mut Vec<u64>, root: u64, is_root: bool) -> Result<usize> {
    use Node::*;

    let b = ctx.engine.read(root).map_err(|_| io_err(path))?;
    let node =
        check_and_unpack_node::<BlockTime>(&b, true, is_root).map_err(|e| node_err(path, e))?;

    match node {
        Internal { values, .. } => {
            // recurse down to the first good leaf
            let mut last_err = None;
            for child in values {
                if path.contains(&child) {
                    continue; // skip loops
                }

                path.push(child);
                match get_depth(ctx, path, child, false) {
                    Ok(n) => return Ok(n + 1),
                    Err(e) => {
                        last_err = Some(e);
                    }
                }
                path.pop();
            }
            Err(last_err.unwrap_or_else(|| node_err(path, NodeError::NumEntriesTooSmall).into()))
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
    match is_seen(root, metadata_sm) {
        Ok(true) | Err(_) => return,
        _ => {}
    }

    // FIXME: make get-depth more resilient
    let mut path = Vec::new();
    let depth = if let Ok(d) = get_depth(ctx, &mut path, root as u64, true) {
        d
    } else {
        return;
    };

    if depth == 0 {
        // The root will be skipped if it is a confirmed internal
        let _ = nodes.insert_leaf(root);
        return;
    }

    if let Ok(b) = ctx.engine.read(root as u64) {
        read_node(ctx, metadata_sm, &b, depth - 1, ignore_non_fatal, nodes);
    } else {
        // FIXME: factor out common code
        let _ = nodes.insert_error(root, NodeError::IoError);
    }
}

// Summarize a subtree rooted at the specified block.
// Only a good internal node will have a summary stored.
// TODO: check the tree is balanced by comparing the height of visited nodes
fn summarize_tree(
    path: &mut Vec<u64>,
    kr: &KeyRange,
    root: u32,
    is_root: bool,
    nodes: &NodeMap,
    summaries: &mut HashVec<NodeSummary>,
    ignore_non_fatal: bool,
) -> NodeSummary {
    if let Some(sum) = summaries.get(root) {
        // Check underfull
        if !ignore_non_fatal && !is_root && sum.nr_entries < MIN_ENTRIES {
            return NodeSummary::error();
        }

        // Check the key range against the parent keys.
        if sum.nr_mappings > 0 {
            if let Some(n) = kr.start {
                // The parent key could be less than or equal to,
                // but not greater than the child's first key
                if n > sum.key_low {
                    return NodeSummary::error();
                }
            }
            if let Some(n) = kr.end {
                // note that KeyRange is a right-opened interval
                if n < sum.key_high {
                    return NodeSummary::error();
                }
            }
        }

        return sum.clone();
    }

    match nodes.get_type(root) {
        NodeType::Internal => {
            if let Some(info) = nodes.internal_info.get(root) {
                // Check underfull
                if !ignore_non_fatal && !is_root && info.keys.len() < MIN_ENTRIES as usize {
                    return NodeSummary::error();
                }

                // Split up the key range for the children.
                // Return immediately if the keys don't match.
                let child_keys = match split_key_ranges(path, kr, &info.keys) {
                    Ok(keys) => keys,
                    Err(_) => return NodeSummary::error(),
                };

                // Gather information from the children
                let mut sum = NodeSummary::default();
                for (i, b) in info.children.iter().enumerate() {
                    path.push(*b as u64);
                    let child_sums = summarize_tree(
                        path,
                        &child_keys[i],
                        *b,
                        false,
                        nodes,
                        summaries,
                        ignore_non_fatal,
                    );
                    let _ = sum.append(&child_sums);
                    path.pop();
                }

                summaries.insert(root, sum.clone());

                sum
            } else {
                // This is unexpected. However, we would like to skip that kind of error.
                NodeSummary::error()
            }
        }

        // This is unexpected since a leaf should have been summarized
        _ => NodeSummary::error(),
    }
}

// Check the mappings filling in the data_sm as we go.
fn check_mappings_bottom_level_(
    ctx: &Context,
    metadata_sm: &Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    data_sm: &Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    roots: &[u64],
    ignore_non_fatal: bool,
) -> Result<HashVec<NodeSummary>> {
    let report = &ctx.report;

    let start = std::time::Instant::now();
    let nodes = collect_nodes_in_use(ctx, metadata_sm, roots, ignore_non_fatal);
    let duration = start.elapsed();
    report.debug(&format!("reading internal nodes: {:?}", duration));

    let start = std::time::Instant::now();
    let (nodes, mut summaries) = read_leaf_nodes(ctx, nodes, data_sm, ignore_non_fatal)?;
    let duration = start.elapsed();
    report.debug(&format!("reading leaf nodes: {:?}", duration));

    let start = std::time::Instant::now();
    count_mapped_blocks(roots, &nodes, &mut summaries, ignore_non_fatal);
    let duration = start.elapsed();
    report.debug(&format!("counting mapped blocks: {:?}", duration));

    report.info(&format!("nr internal nodes: {}", nodes.internal_info.len()));
    report.info(&format!("nr leaves: {}", nodes.nr_leaves));

    if !nodes.node_errors.is_empty() {
        let mut nr_io_errors = 0;
        let mut nr_checksum_errors = 0;
        for e in nodes.node_errors.values() {
            match e {
                NodeError::IoError => nr_io_errors += 1,
                NodeError::ChecksumError => nr_checksum_errors += 1,
                _ => {}
            }
        }
        report.fatal(&format!(
            "{} nodes in data mapping tree contain errors",
            nodes.node_errors.len()
        ));
        report.fatal(&format!(
            "{} io errors, {} checksum errors",
            nr_io_errors, nr_checksum_errors
        ));
    }

    Ok(summaries)
}

fn collect_nodes_in_use(
    ctx: &Context,
    metadata_sm: &Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    roots: &[u64],
    ignore_non_fatal: bool,
) -> NodeMap {
    let mut nodes = NodeMap::new(ctx.engine.get_nr_blocks() as u32);

    for root in roots {
        read_internal_nodes(ctx, metadata_sm, *root as u32, ignore_non_fatal, &mut nodes);
    }

    nodes
}

fn unpacker(
    blocks_rx: &Arc<Mutex<mpsc::Receiver<Vec<Block>>>>,
    nodes_tx: SyncSender<Vec<Node<BlockTime>>>,
    node_map: Arc<Mutex<NodeMap>>,
    ignore_non_fatal: bool,
) {
    loop {
        let blocks = {
            let blocks_rx = blocks_rx.lock().unwrap();
            if let Ok(blocks) = blocks_rx.recv() {
                blocks
            } else {
                break;
            }
        };

        let mut nodes = Vec::with_capacity(blocks.len());
        let mut errs = Vec::new();

        for b in blocks {
            // Allow under full nodes in this phase.  The under full
            // property will be check later based on the path context.
            match check_and_unpack_node::<BlockTime>(&b, ignore_non_fatal, true) {
                Ok(n) => {
                    nodes.push(n);
                }
                Err(e) => {
                    errs.push((b.loc, e));
                }
            }
        }

        if !errs.is_empty() {
            let mut node_map = node_map.lock().unwrap();
            for (b, e) in errs {
                // theoretically never fail
                let _ = node_map.insert_error(b as u32, e);
            }
        }

        if nodes_tx.send(nodes).is_err() {
            break;
        }
    }
}

fn summariser(
    nodes_rx: mpsc::Receiver<Vec<Node<BlockTime>>>,
    data_sm: &Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    summaries: &Arc<Mutex<HashVec<NodeSummary>>>,
) {
    let mut summaries = summaries.lock().unwrap();

    loop {
        let nodes = {
            if let Ok(nodes) = nodes_rx.recv() {
                nodes
            } else {
                break;
            }
        };

        for n in nodes {
            if let Node::Leaf {
                keys,
                values,
                header,
            } = n
            {
                let mut data_sm = data_sm.lock().unwrap();
                for v in values {
                    // Ignore errors on increment
                    let _ = data_sm.inc(v.block, 1);
                }

                let sum = NodeSummary::from_leaf(&keys);
                summaries.insert(header.block as u32, sum);
            } else {
                // Do not report error here. The error will be captured
                // in the second phase.
            }
        }
    }
}

fn read_leaf_nodes(
    ctx: &Context,
    nodes: NodeMap,
    data_sm: &Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    ignore_non_fatal: bool,
) -> Result<(NodeMap, HashVec<NodeSummary>)> {
    const QUEUE_DEPTH: usize = 4;
    const NR_UNPACKERS: usize = 4;

    // Single IO thread reads vecs of blocks
    // Many unpackers take the block vecs and turn them into btree nodes
    // Single 'summariser' thread processes the nodes

    // Build a vec of the leaf locations.  These will be in disk location
    // order.
    let mut leaves = Vec::with_capacity(nodes.nr_leaves as usize);
    for loc in nodes.leaf_nodes.ones() {
        leaves.push(loc as u64);
    }

    let (blocks_tx, blocks_rx) = mpsc::sync_channel::<Vec<Block>>(QUEUE_DEPTH);
    let blocks_rx = Arc::new(Mutex::new(blocks_rx));

    let (nodes_tx, nodes_rx) = mpsc::sync_channel::<Vec<Node<BlockTime>>>(QUEUE_DEPTH);

    // Process chunks of leaves at once so the io engine can aggregate reads.
    let summaries = Arc::new(Mutex::new(HashVec::with_capacity(nodes.len())));
    let nodes = Arc::new(Mutex::new(nodes));

    // Kick off the unpackers
    let mut unpackers = Vec::with_capacity(NR_UNPACKERS);
    for _i in 0..NR_UNPACKERS {
        let blocks_rx = blocks_rx.clone();
        let nodes_tx = nodes_tx.clone();
        let node_map = nodes.clone();
        unpackers.push(thread::spawn(move || {
            unpacker(&blocks_rx, nodes_tx, node_map, ignore_non_fatal)
        }));
    }
    drop(blocks_rx);
    drop(nodes_tx);

    // Kick off the summariser
    let summariser_tid = {
        let data_sm = data_sm.clone();
        let summaries = summaries.clone();
        thread::spawn(move || {
            summariser(nodes_rx, &data_sm, &summaries);
        })
    };

    // IO is done in the main thread
    let engine = ctx.engine.clone();
    for c in leaves.chunks(1024) {
        let mut bs = Vec::with_capacity(c.len());

        // TODO: Retry blocks ignored by vectored io
        if let Ok(blocks) = engine.read_many(c) {
            for b in blocks {
                if b.is_err() {
                    continue;
                }

                let b = b.unwrap();
                bs.push(b);
            }

            blocks_tx
                .send(bs)
                .expect("couldn't send blocks to unpacker");
        } else {
            let mut nodes = nodes.lock().unwrap();
            for b in c {
                let _ = nodes.insert_error(*b as u32, NodeError::IoError);
            }
        }
    }

    drop(blocks_tx);

    // Wait for child threads
    for tid in unpackers {
        tid.join().expect("couldn't join unpacker");
    }
    summariser_tid.join().expect("couldn't join summariser");

    // extract the results
    let nodes = Arc::try_unwrap(nodes).unwrap().into_inner().unwrap();
    let summaries = Arc::try_unwrap(summaries).unwrap().into_inner().unwrap();

    Ok((nodes, summaries))
}

// Revisit the trees to count the number of mappings for each device.
// Also, check the key ranges and the underfull property for each node.
//
// A key range is verified by comparing it against the actual keys gathered
// from its children. That's why the checking is performed after reading the
// leaves.
//
// Likewise, the underfull property is path-dependent, thus it's better to
// check this property after reading all the nodes. For example, given an
// underfull node that is erroneously being used as a root and non-root in
// different trees, only the second case should be treated as an error.
fn count_mapped_blocks(
    roots: &[u64],
    nodes: &NodeMap,
    summaries: &mut HashVec<NodeSummary>,
    ignore_non_fatal: bool,
) {
    for root in roots.iter() {
        let mut path = vec![0, *root]; // the path is just for error reporting
        let kr = KeyRange::new();
        summarize_tree(
            &mut path,
            &kr,
            *root as u32,
            true,
            nodes,
            summaries,
            ignore_non_fatal,
        );
    }
}

//------------------------------------------

fn check_mapped_blocks(
    ctx: &Context,
    devs: &mut dyn Iterator<Item = (&u64, &u64, &DeviceDetail)>,
    summaries: &HashVec<NodeSummary>,
) -> Result<()> {
    let report = &ctx.report;

    let start = std::time::Instant::now();
    let mut failed = false;
    for (thin_id, root, details) in devs {
        if let Some(sum) = summaries.get(*root as u32) {
            if sum.nr_errors > 0 {
                failed = true;
                let missed = details.mapped_blocks.saturating_sub(sum.nr_mappings);
                let mut errors = sum.nr_errors.to_string();
                if sum.nr_errors == 255 {
                    errors.push('+');
                }
                report.fatal(&format!(
                    "Thin device {} has {} errors and is missing {} mappings, while expected {}",
                    thin_id, errors, missed, details.mapped_blocks
                ));
            } else if sum.nr_mappings != details.mapped_blocks {
                failed = true;
                report.fatal(&format!(
                    "Thin device {} has unexpected number of mappings, expected {}, actual {}",
                    thin_id, details.mapped_blocks, sum.nr_mappings
                ));
            }
        } else {
            failed = true;
            report.fatal(&format!(
                "Thin device {} is missing root with {} mappings",
                thin_id, details.mapped_blocks
            ));
        }
    }
    let duration = start.elapsed();
    report.debug(&format!("checking mapped blocks: {:?}", duration));

    if failed {
        Err(anyhow!("Check of mappings failed"))
    } else {
        Ok(())
    }
}

fn mk_context_(engine: Arc<dyn IoEngine + Send + Sync>, report: Arc<Report>) -> Result<Context> {
    Ok(Context { report, engine })
}

fn mk_context(opts: &ThinCheckOptions) -> Result<Context> {
    let engine = EngineBuilder::new(opts.input, &opts.engine_opts)
        .write(opts.auto_repair || opts.clear_needs_check)
        .exclusive(!opts.engine_opts.use_metadata_snap)
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

#[derive(thiserror::Error, Debug)]
struct MetadataError {
    context: String,
    err: anyhow::Error,
}

impl fmt::Display for MetadataError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.context, self.err)
    }
}

fn metadata_err(context: &str, err: anyhow::Error) -> MetadataError {
    MetadataError {
        context: context.to_string(),
        err,
    }
}

// We read the top-level tree once to get the number of thin devices, and hence the
// maximum metadata ref count.  Then create metadata space map.
fn create_metadata_sm(
    engine: &Arc<dyn IoEngine + Send + Sync>,
    sb: &Superblock,
    sb_snap: &Option<Result<Superblock>>,
    ignore_non_fatal: bool,
) -> Result<ASpaceMap> {
    let mut path = vec![0];

    // Use a temporary space map to reach out non-shared leaves so we could get
    // the maximum reference count of a bottom-level leaf it could be.
    let metadata_sm = Arc::new(Mutex::new(RestrictedSpaceMap::new(engine.get_nr_blocks())));

    let roots = btree_to_map_with_sm::<u64>(
        &mut path,
        engine.clone(),
        metadata_sm.clone(),
        ignore_non_fatal,
        sb.mapping_root,
    )
    .map_err(|e| metadata_err("mapping top-level", e.into()))?;

    let mut nr_devs = roots.len();

    if let Some(Ok(sb_snap)) = sb_snap {
        let roots_snap = btree_to_map_with_sm::<u64>(
            &mut path,
            engine.clone(),
            metadata_sm,
            ignore_non_fatal,
            sb_snap.mapping_root,
        )
        .map_err(|e| metadata_err("mapping top-level", e.into()))?;
        nr_devs += roots_snap.len();
    }

    let metadata_sm = core_sm(engine.get_nr_blocks(), nr_devs as u32);

    Ok(metadata_sm)
}

fn get_devices_(
    engine: &Arc<dyn IoEngine + Send + Sync>,
    sb: &Superblock,
    metadata_sm: &ASpaceMap,
    ignore_non_fatal: bool,
) -> Result<(BTreeMap<u64, DeviceDetail>, BTreeMap<u64, u64>)> {
    let mut path = vec![0];

    // Reread device details to increment the ref counts for that metadata.
    let devs = btree_to_map_with_sm::<DeviceDetail>(
        &mut path,
        engine.clone(),
        metadata_sm.clone(),
        ignore_non_fatal,
        sb.details_root,
    )
    .map_err(|e| metadata_err("device details tree", e.into()))?;

    // Mapping top level
    let roots = btree_to_map_with_sm::<u64>(
        &mut path,
        engine.clone(),
        metadata_sm.clone(),
        ignore_non_fatal,
        sb.mapping_root,
    )
    .map_err(|e| metadata_err("mapping top-level", e.into()))?;

    Ok((devs, roots))
}

fn get_thins_from_superblock(
    engine: &Arc<dyn IoEngine + Send + Sync>,
    sb: &Superblock,
    metadata_sm: &ASpaceMap,
    all_roots: &mut Vec<u64>,
    ignore_non_fatal: bool,
) -> Result<BTreeMap<u64, (u64, DeviceDetail)>> {
    let (devs, roots) = get_devices_(engine, sb, metadata_sm, ignore_non_fatal)?;

    // It's highly possible that the pool is damaged by multiple activation
    // if the two trees are inconsistent. In this situation, there's no need to
    // do further checking, and users should perform the repair process.
    // Here we don't use is_superblock_consistent() to avoid extra reads.
    if !roots.keys().eq(devs.keys()) {
        return Err(anyhow!(
            "Inconsistency between the details tree and the mapping tree"
        ));
    }

    for root in roots.values() {
        all_roots.push(*root);
    }

    let thins = roots
        .into_iter()
        .zip(devs.into_values())
        .map(|((id, root), details)| (id, (root, details)))
        .collect();

    Ok(thins)
}

fn get_thins_from_metadata_snap(
    engine: &Arc<dyn IoEngine + Send + Sync>,
    sb_snap: &Superblock,
    metadata_sm: &ASpaceMap,
    thins: &BTreeMap<u64, (u64, DeviceDetail)>,
    all_roots: &mut Vec<u64>,
    ignore_non_fatal: bool,
) -> Result<BTreeMap<u64, (u64, DeviceDetail)>> {
    // Check consistency between device details and the top-level tree
    is_superblock_consistent(sb_snap.clone(), engine.clone(), ignore_non_fatal)?;

    // Extract roots and details stored in non-shared leaves
    let (devs_snap, roots_snap) = get_devices_(engine, sb_snap, metadata_sm, ignore_non_fatal)?;

    // Select the thins that are exclusive to metadata snapshot for further checking,
    // i.e., those whose tuples (dev_id, root) are not present in top-level tree of
    // superblock. Here are some notes:
    //
    // * We must use the full tuple (dev_id, root) to identify devices in the context
    //   of metadata snapshot for these reasons:
    //   - The data mappings might have been changed after taking a metadata snasphot.
    //   - Thin id reuse: a long-lived metadata snapshot might contains deleted thins
    //     whose dev_id are now used by other new thins.
    // * The roots and details extracted by btree_to_map_with_sm() are those stored in
    //   non-shared leaves. They might not represent the same subset of thins due to
    //   their different value sizes in btree, i.e., shadowing a top-level leaves clones
    //   more entries than a details tree leave.
    let roots_excl: BTreeMap<u64, u64> = roots_snap
        .iter()
        .filter(|(dev_id, root)| thins.get(*dev_id).filter(|(r, _)| *root == r).is_none())
        .map(|(dev_id, root)| (*dev_id, *root))
        .collect();
    let devs_excl: BTreeMap<u64, DeviceDetail> = devs_snap
        .into_iter()
        .filter(|(dev_id, _details)| roots_excl.contains_key(dev_id))
        .collect();

    if !devs_excl.keys().eq(roots_excl.keys()) {
        return Err(anyhow!("unexpected thin ids"));
    }

    // Append *all* the roots to the list for further checking.
    // Roots duplicate to those in superblock are also added for ref counting purpose.
    roots_snap.values().for_each(|root| {
        all_roots.push(*root);
    });

    let thins_snap: BTreeMap<u64, (u64, DeviceDetail)> = roots_excl
        .into_iter()
        .zip(devs_excl.values())
        .map(|((id, root), details)| (id, (root, *details)))
        .collect();

    Ok(thins_snap)
}

fn check_mappings_bottom_level(
    ctx: &Context,
    sb: &Superblock,
    metadata_sm: &Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    data_sm: &Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    roots: &[u64],
    ignore_non_fatal: bool,
) -> Result<HashVec<NodeSummary>> {
    let report = &ctx.report;

    let metadata_root = unpack::<SMRoot>(&sb.metadata_sm_root[0..])?;
    let data_root = unpack::<SMRoot>(&sb.data_sm_root[0..])?;

    let mon_meta_sm = metadata_sm.clone();
    let mon_data_sm = data_sm.clone();
    let monitor = ProgressMonitor::new(
        report.clone(),
        metadata_root.nr_allocated + (data_root.nr_allocated / 8),
        move || {
            mon_meta_sm.lock().unwrap().get_nr_allocated().unwrap()
                + (mon_data_sm.lock().unwrap().get_nr_allocated().unwrap() / 8)
        },
    );

    let summaries =
        check_mappings_bottom_level_(ctx, metadata_sm, data_sm, roots, ignore_non_fatal);

    monitor.stop();

    summaries
}

fn create_data_sm(sb: &Superblock, nr_devs: u32) -> Result<ASpaceMap> {
    let data_root = unpack::<SMRoot>(&sb.data_sm_root[0..])?;

    let data_sm = if nr_devs <= 1 {
        Arc::new(Mutex::new(RestrictedSpaceMap::new(data_root.nr_blocks)))
    } else {
        core_sm(data_root.nr_blocks, nr_devs)
    };

    Ok(data_sm)
}

pub fn check(opts: ThinCheckOptions) -> Result<()> {
    if (opts.auto_repair || opts.clear_needs_check)
        && (opts.engine_opts.use_metadata_snap
            || opts.override_mapping_root.is_some()
            || opts.override_details_root.is_some())
    {
        return Err(anyhow!("cannot perform repair outside the actual metadata"));
    }

    let ctx = mk_context(&opts)?;

    // FIXME: temporarily get these out
    let report = &ctx.report;
    let engine = &ctx.engine;

    let mut sb = read_superblock(engine.as_ref(), SUPERBLOCK_LOCATION)?;

    // Read the superblock in metadata snapshot (allow errors)
    let sb_snap = if sb.metadata_snap > 0 {
        Some(read_superblock(engine.as_ref(), sb.metadata_snap))
    } else {
        None
    };

    if opts.engine_opts.use_metadata_snap {
        if sb_snap.is_none() {
            return Err(anyhow!("no current metadata snap"));
        }

        if let Some(Err(e)) = sb_snap {
            return Err(metadata_err("metadata snap", e).into());
        }
    }

    let _ = print_info(&sb, report.clone());

    if opts.sb_only {
        if opts.clear_needs_check {
            let cleared = clear_needs_check_flag(engine.clone())?;
            if cleared {
                report.warning("Cleared needs_check flag");
            }
        }
        return Ok(());
    }

    //------------------------------------

    report.set_title("Checking thin metadata");

    let metadata_sm = if opts.engine_opts.use_metadata_snap {
        Arc::new(Mutex::new(RestrictedSpaceMap::new(engine.get_nr_blocks())))
    } else {
        create_metadata_sm(engine, &sb, &sb_snap, opts.ignore_non_fatal)?
    };

    inc_superblock(&metadata_sm, sb.metadata_snap)?;

    //------------------------------------
    // Check device details and the top-level tree

    report.set_sub_title("device details tree");

    sb.mapping_root = opts.override_mapping_root.unwrap_or(sb.mapping_root);
    sb.details_root = opts.override_details_root.unwrap_or(sb.details_root);

    // List of all the bottom-level roots referenced by the top-level tree,
    // including those reside in the metadata snapshot.
    let mut all_roots = Vec::<u64>::new();

    // Collect thin devices in-use
    let thins = if !opts.engine_opts.use_metadata_snap {
        get_thins_from_superblock(
            engine,
            &sb,
            &metadata_sm,
            &mut all_roots,
            opts.ignore_non_fatal,
        )?
    } else {
        BTreeMap::new()
    };

    // Collect thin devices reside in the metadata snapshot only
    // (allow errors if option -m is not applied)
    let thins_snap = match sb_snap {
        Some(Ok(ref sbs)) => get_thins_from_metadata_snap(
            engine,
            sbs,
            &metadata_sm,
            &thins,
            &mut all_roots,
            opts.ignore_non_fatal,
        ),
        Some(Err(e)) => Err(e),
        None => Ok(BTreeMap::new()),
    };

    if opts.skip_mappings {
        let cleared = clear_needs_check_flag(engine.clone())?;
        if cleared {
            report.warning("Cleared needs_check flag");
        }
        return Ok(());
    }

    //----------------------------------------
    // Check data mappings

    report.set_sub_title("mapping tree");

    report.info(&format!("number of devices to check: {}", all_roots.len()));

    let data_sm = if opts.engine_opts.use_metadata_snap {
        create_data_sm(&sb, 1)?
    } else {
        create_data_sm(&sb, all_roots.len() as u32)?
    };

    let summaries = check_mappings_bottom_level(
        &ctx,
        &sb,
        &metadata_sm,
        &data_sm,
        &all_roots,
        opts.ignore_non_fatal,
    )?;

    // Check the number of mapped blocks
    let mut iter = thins
        .iter()
        .map(|(id, (root, details))| (id, root, details));
    check_mapped_blocks(&ctx, &mut iter, &summaries)?;

    match thins_snap {
        Err(e) => {
            return Err(metadata_err("metadata snap", e).into());
        }
        Ok(thins_snap) => {
            let mut iter = thins_snap
                .iter()
                .map(|(id, (root, details))| (id, root, details));
            check_mapped_blocks(&ctx, &mut iter, &summaries)?;
        }
    }

    if opts.engine_opts.use_metadata_snap {
        return Ok(());
    }

    //-----------------------------------------
    // Check the data space map

    report.set_sub_title("data space map");
    let start = std::time::Instant::now();
    let root = unpack::<SMRoot>(&sb.data_sm_root[0..])?;
    let data_leaks = check_disk_space_map(
        engine.clone(),
        report.clone(),
        root,
        data_sm.clone(),
        metadata_sm.clone(),
        opts.ignore_non_fatal,
    )
    .map_err(|e| metadata_err("data space map", e))?;
    let duration = start.elapsed();
    report.debug(&format!("checking data space map: {:?}", duration));

    //-----------------------------------------
    // Check the metadata space map

    report.set_sub_title("metadata space map");
    let start = std::time::Instant::now();
    let root = unpack::<SMRoot>(&sb.metadata_sm_root[0..])?;

    // Now the counts should be correct and we can check it.
    let metadata_leaks = check_metadata_space_map(
        engine.clone(),
        report.clone(),
        root,
        metadata_sm.clone(),
        opts.ignore_non_fatal,
    )
    .map_err(|e| metadata_err("metadata space map", e))?;
    let duration = start.elapsed();
    report.debug(&format!("checking metadata space map: {:?}", duration));

    //-----------------------------------------
    // Fix minor issues found in the metadata

    if !data_leaks.is_empty() {
        if opts.auto_repair || opts.clear_needs_check {
            report.warning("Repairing data leaks.");
            repair_space_map(engine.clone(), data_leaks, data_sm.clone())?;
        } else if !opts.ignore_non_fatal {
            return Err(anyhow!("data space map contains leaks"));
        }
    }

    if !metadata_leaks.is_empty() {
        if opts.auto_repair || opts.clear_needs_check {
            report.warning("Repairing metadata leaks.");
            repair_space_map(engine.clone(), metadata_leaks, metadata_sm.clone())?;
        } else if !opts.ignore_non_fatal {
            return Err(anyhow!("metadata space map contains leaks"));
        }
    }

    if opts.auto_repair || opts.clear_needs_check {
        let cleared = clear_needs_check_flag(engine.clone())?;
        if cleared {
            report.warning("Cleared needs_check flag");
        }
    }

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

    let sb = read_superblock(engine.as_ref(), SUPERBLOCK_LOCATION)?;
    let metadata_sm = create_metadata_sm(&engine, &sb, &None, false)?;
    inc_superblock(&metadata_sm, sb.metadata_snap)?;

    //-----------------------------------------

    report.set_sub_title("device details tree");

    let mut all_roots = Vec::<u64>::new();
    let thins = get_thins_from_superblock(&engine, &sb, &metadata_sm, &mut all_roots, false)?;

    //-----------------------------------------

    report.set_sub_title("mapping tree");

    let data_sm = create_data_sm(&sb, all_roots.len() as u32)?;
    let summaries = check_mappings_bottom_level_(&ctx, &metadata_sm, &data_sm, &all_roots, false)?;

    // Check the number of mapped blocks
    let mut iter = thins
        .iter()
        .map(|(id, (root, details))| (id, root, details));
    check_mapped_blocks(&ctx, &mut iter, &summaries)?;

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

    Ok(CheckMaps {
        metadata_sm: metadata_sm.clone(),
        data_sm: data_sm.clone(),
    })
}

//------------------------------------------
