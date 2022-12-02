use anyhow::{anyhow, Result};
use fixedbitset::FixedBitSet;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::mpsc::{self, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread;
use threadpool::ThreadPool;

use crate::checksum;
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
    nr_entries: u8,   // number of entires in this node
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
        }
    }

    fn append(&mut self, other: &NodeSummary) -> anyhow::Result<()> {
        if other.nr_mappings > 0 {
            if self.nr_mappings == 0 {
                *self = other.clone();
            } else {
                if other.key_low <= self.key_high {
                    return Err(anyhow!("overlapped keys"));
                }
                self.key_high = other.key_high;
                self.nr_mappings += other.nr_mappings;
            }
        }

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
    node_errors: HashVec<BTreeError>, // TODO: use NodeError instead
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
        // A potential leaf could be labeled as an actual internal node
        let node_type = self.get_type(blocknr);
        if node_type != NodeType::None && node_type != NodeType::Leaf {
            return Err(anyhow!("type changed"));
        }
        self.internal_info.insert(blocknr, info);
        self.set_type_(blocknr, NodeType::Internal);
        Ok(())
    }

    fn insert_leaf(&mut self, blocknr: u32) -> Result<()> {
        // Only an unread block could be labeled as a potential leaf
        if self.get_type(blocknr) != NodeType::None {
            return Err(anyhow!("type changed"));
        }
        self.set_type_(blocknr, NodeType::Leaf);
        Ok(())
    }

    fn insert_error(&mut self, blocknr: u32, e: BTreeError) -> Result<()> {
        // A potential leaf could be labeled as an broken internal
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

// FIXME: add context to errors

fn verify_checksum(b: &Block) -> btree::Result<()> {
    let bt = checksum::metadata_block_type(b.get_data());
    if bt != checksum::BT::NODE {
        return Err(BTreeError::NodeError(String::from(
            "corrupt block: checksum failed",
        )));
    }
    Ok(())
}

fn is_seen(loc: u32, metadata_sm: &Arc<Mutex<dyn SpaceMap + Send + Sync>>) -> Result<bool> {
    let mut sm = metadata_sm.lock().unwrap();
    sm.inc(loc as u64, 1)?;
    Ok(sm.get(loc as u64).unwrap_or(0) > 1)
}

// Returns error of the node itself (checksum or unpack error),
// but not the errors from its children.
// FIXME: split up this function
fn read_node_(
    ctx: &Context,
    metadata_sm: &Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    b: &Block,
    depth: usize,
    ignore_non_fatal: bool,
    nodes: &mut NodeMap,
) -> btree::Result<()> {
    verify_checksum(b)?;

    // allow underfull nodes in the first pass
    // FIXME: use proper path, actually can we recreate the path from the node info?
    let path = Vec::new();
    let node = unpack_node::<u64>(&path, b.get_data(), ignore_non_fatal, true)?;

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
                            let _ = nodes.insert_error(values[i] as u32, BTreeError::IoError);
                        }
                    }
                }
                Err(_) => {
                    // error every child node
                    for loc in values {
                        // theoretically never fail
                        let _ = nodes.insert_error(loc as u32, BTreeError::IoError);
                    }
                }
            };
        }
    }

    Ok(())
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
    let block_nr = b.loc as u32;
    if let Err(e) = read_node_(ctx, metadata_sm, b, depth, ignore_non_fatal, nodes) {
        // theoretically never fail
        let _ = nodes.insert_error(block_nr, e);
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
    match is_seen(root, metadata_sm) {
        Ok(true) | Err(_) => return,
        _ => {}
    }

    // FIXME: make get-depth more resilient
    let mut path = Vec::new();
    let depth;
    if let Ok(d) = get_depth(ctx, &mut path, root as u64, true) {
        depth = d;
    } else {
        return;
    }

    if depth == 0 {
        // The root will be skipped if it is a confirmed internal
        let _ = nodes.insert_leaf(root as u32);
        return;
    }

    if let Ok(b) = ctx.engine.read(root as u64) {
        read_node(ctx, metadata_sm, &b, depth - 1, ignore_non_fatal, nodes);
    } else {
        // FIXME: factor out common code
        let _ = nodes.insert_error(root, BTreeError::IoError);
    }
}

// Summarize a subtree rooted at the speicifc block.
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
            return NodeSummary::default();
        }

        // Check the key range against the parent keys.
        if sum.nr_mappings > 0 {
            if let Some(n) = kr.start {
                // The parent key could be less than or equal to,
                // but not greater than the child's first key
                if n > sum.key_low {
                    return NodeSummary::default();
                }
            }
            if let Some(n) = kr.end {
                // note that KeyRange is a right-opened interval
                if n < sum.key_high {
                    return NodeSummary::default();
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
                    return NodeSummary::default();
                }

                // Gather information from the children
                // FIXME: handle key range mismatch on internal nodes
                let child_keys = split_key_ranges(path, kr, &info.keys).unwrap();

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
                NodeSummary::default()
            }
        }
        _ => NodeSummary::default(),
    }
}

// Check the mappings filling in the data_sm as we go.
fn check_mapping_bottom_level(
    ctx: &Context,
    metadata_sm: &Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    data_sm: &Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    roots: &[u64],
    ignore_non_fatal: bool,
) -> Result<HashVec<NodeSummary>> {
    let start = std::time::Instant::now();
    let nodes = collect_nodes_in_use(ctx, metadata_sm, roots, ignore_non_fatal);
    let duration = start.elapsed();
    ctx.report
        .info(&format!("reading internal nodes: {:?}", duration));

    let start = std::time::Instant::now();
    let (nodes, mut summaries) = read_leaf_nodes(ctx, nodes, data_sm, ignore_non_fatal)?;
    let duration = start.elapsed();
    ctx.report
        .info(&format!("reading leaf nodes: {:?}", duration));

    let start = std::time::Instant::now();
    count_mapped_blocks(roots, &nodes, &mut summaries, ignore_non_fatal);
    let duration = start.elapsed();
    ctx.report
        .info(&format!("counting mapped blocks: {:?}", duration));

    ctx.report
        .info(&format!("nr internal nodes: {}", nodes.internal_info.len()));
    ctx.report.info(&format!("nr leaves: {}", nodes.nr_leaves));
    ctx.report
        .info(&format!("nr errors: {}", nodes.node_errors.len()));

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

/*
fn read_leaf_nodes(
    ctx: &Context,
    nodes: NodeMap,
    data_sm: &Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    ignore_non_fatal: bool,
) -> (NodeMap, HashVec<NodeSummary>) {
    // Build a vec of the leaf locations.  These will be in disk location
    // order.
    let mut leaves = Vec::with_capacity(nodes.nr_leaves as usize);
    for loc in nodes.leaf_nodes.ones() {
        leaves.push(loc as u64);
    }

    // Process chunks of leaves at once so the io engine can aggregate reads.
    let summaries = Arc::new(Mutex::new(HashVec::with_capacity(nodes.len())));
    let leaves = Arc::new(leaves);
    let nodes = Arc::new(nodes);
    let mut chunk_start = 0;
    while chunk_start < leaves.len() {
        // 1024 is the maximum iovcnt of readv on most systems
        let len = std::cmp::min(1024, leaves.len() - chunk_start);
        let engine = ctx.engine.clone();
        let data_sm = data_sm.clone();
        let leaves = leaves.clone();
        let summaries = summaries.clone();

        ctx.pool.execute(move || {
            let c = &leaves[chunk_start..(chunk_start + len)];

            // TODO: Retry blocks ignored by vectored io
            if let Ok(blocks) = engine.read_many(c) {
                for (loc, b) in c.iter().zip(blocks) {
                    if b.is_err() {
                        continue;
                    }

                    let b = b.unwrap();
                    if verify_checksum(&b).is_err() {
                        continue;
                    }

                    // Allow underfull nodes in this phase.
                    // The underfull property will be check later based on the path context.
                    let path = Vec::new();
                    let node =
                        unpack_node::<BlockTime>(&path, b.get_data(), ignore_non_fatal, true);

                    if let Ok(Node::Leaf { keys, values, .. }) = node {
                        {
                            let mut data_sm = data_sm.lock().unwrap();
                            for v in values {
                                let _ = data_sm.inc(v.block, 1);
                            }
                        }

                        {
                            let sum = NodeSummary::from_leaf(&keys);
                            let mut sums = summaries.lock().unwrap();
                            sums.insert(*loc as u32, sum);
                        }
                    }
                }
            }
        });
        chunk_start += len;
    }
    ctx.pool.join();

    let nodes = Arc::try_unwrap(nodes).unwrap();
    let summaries = Arc::try_unwrap(summaries).unwrap().into_inner().unwrap();

    (nodes, summaries)
}
*/

fn unpacker(
    blocks_rx: &Arc<Mutex<mpsc::Receiver<Vec<Block>>>>,
    nodes_tx: SyncSender<Vec<Node<BlockTime>>>,
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

        for b in blocks {
            if verify_checksum(&b).is_err() {
                continue;
            }

            // Allow under full nodes in this phase.  The under full
            // property will be check later based on the path context.
            let path = Vec::new();
            if let Ok(node) = unpack_node::<BlockTime>(&path, b.get_data(), ignore_non_fatal, true)
            {
                nodes.push(node);
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
    let mut data_sm = data_sm.lock().unwrap();

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
                for v in values {
                    let _ = data_sm.inc(v.block, 1);
                }

                let sum = NodeSummary::from_leaf(&keys);
                summaries.insert(header.block as u32, sum);
            } else {
                // FIXME: report error
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
    const QUEUE_DEPTH: usize = 2;
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
    let leaves = Arc::new(leaves);
    let nodes = Arc::new(nodes);

    // Kick off the unpackers
    let mut unpackers = Vec::with_capacity(NR_UNPACKERS);
    for _i in 0..NR_UNPACKERS {
        let blocks_rx = blocks_rx.clone();
        let nodes_tx = nodes_tx.clone();
        unpackers.push(thread::spawn(move || {
            unpacker(&blocks_rx, nodes_tx, ignore_non_fatal)
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
                if verify_checksum(&b).is_err() {
                    continue;
                }
                bs.push(b);
            }

            blocks_tx
                .send(bs)
                .expect("couldn't send blocks to unpacker");
        } else {
            // FIXME: report error
        }
    }

    drop(blocks_tx);

    // Wait for child threads
    for tid in unpackers {
        tid.join().expect("couldn't join unpacker");
    }
    summariser_tid.join().expect("couldn't join summariser");

    // extract the results
    let nodes = Arc::try_unwrap(nodes).unwrap();
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

fn check_mapped_blocks(
    ctx: &Context,
    devs: &mut dyn Iterator<Item = (&u64, &u64, &DeviceDetail)>,
    summaries: &HashVec<NodeSummary>,
) -> Result<()> {
    let start = std::time::Instant::now();
    let mut failed = false;
    for (thin_id, root, details) in devs {
        if let Some(sum) = summaries.get(*root as u32) {
            if sum.nr_mappings != details.mapped_blocks {
                failed = true;
                ctx.report.fatal(&format!(
                    "Thin device {} has unexpected number of mappings, expected {}, actual {}",
                    thin_id, details.mapped_blocks, sum.nr_mappings
                ));
            }
        } else {
            ctx.report.fatal(&format!(
                "Thin device {} is missing root with {} mappings",
                thin_id, details.mapped_blocks
            ));
        }
    }
    let duration = start.elapsed();
    ctx.report
        .info(&format!("checking mapped blocks: {:?}", duration));

    if failed {
        Err(anyhow!("Check of mappings failed"))
    } else {
        Ok(())
    }
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
    let nr_threads = engine.suggest_nr_threads();
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

    // It's highly possible that the pool is damaged by multiple activation
    // if the two trees are inconsistent. In this situation, there's no need to
    // do further checking, and users should perform the repair process.
    // Here we don't use is_superblock_consistent() to avoid extra reads.
    if !roots.keys().eq(devs.keys()) {
        return Err(anyhow!(
            "Inconsistency between the details tree and the mapping tree"
        ));
    }

    if opts.skip_mappings {
        let cleared = clear_needs_check_flag(ctx.engine.clone())?;
        if cleared {
            ctx.report.info("Cleared needs_check flag");
        }
        return Ok(());
    }

    // List of all the roots in-use.
    // Might contain duplicate entries if there is a metadata snapshot.
    let mut all_roots: Vec<u64> = roots.values().map(|(_path, root)| *root).collect();

    // Check availability of metadata snapshot (error is allowed)
    let sb_snap = if sb.metadata_snap > 0 {
        // Check device id consistency regareless of reading nodes shared with
        // the main superblock
        Some(
            read_superblock(engine.as_ref(), sb.metadata_snap).and_then(|sbs| {
                is_superblock_consistent(sbs, engine.clone(), opts.ignore_non_fatal)
            }),
        )
    } else {
        None
    };

    // Collect the thins that are exclusive to metadata snapshot for further checking
    let thins_snap = if let Some(Ok(ref sbs)) = sb_snap {
        // Increase the ref count for metadata snap
        {
            let mut metadata_sm = metadata_sm.lock().unwrap();
            metadata_sm.inc(sb.metadata_snap, 1)?;
        }

        // Read the device details in non-shared nodes
        let devs_snap = btree_to_map_with_sm::<DeviceDetail>(
            &mut path,
            engine.clone(),
            metadata_sm.clone(),
            opts.ignore_non_fatal,
            sbs.details_root,
        )
        .unwrap();

        // Read the mapping tree roots in non-shared nodes
        let roots_snap = btree_to_map_with_sm::<u64>(
            &mut path,
            engine.clone(),
            metadata_sm.clone(),
            opts.ignore_non_fatal,
            sbs.mapping_root,
        )
        .unwrap();

        // Append *all* those roots to the list for further checking.
        // Roots duplicate to those in superblock are also added for ref counting purpose.
        roots_snap.values().for_each(|root| {
            all_roots.push(*root);
        });

        // The exclusive thins are selected by the root block address with the
        // concerns thin id reuse.
        // Note that the two sets might not have identical thins before filtering
        // due to their different value sizes.
        let roots_snap: BTreeMap<u64, u64> = roots_snap
            .into_iter()
            .filter(|(_thin_id, root)| roots.iter().any(|(_id, (_p, r))| root == r))
            .collect();
        let devs_snap: BTreeMap<u64, DeviceDetail> = devs_snap
            .into_iter()
            .filter(|(thin_id, _details)| roots_snap.contains_key(thin_id))
            .collect();

        if devs_snap.keys().eq(roots_snap.keys()) {
            let thins_snap: Vec<(u64, u64, DeviceDetail)> = roots_snap
                .into_iter()
                .zip(devs_snap.values())
                .map(|((id, root), details)| (id, root, *details))
                .collect();
            Some(Ok(thins_snap))
        } else {
            Some(Err(anyhow!("unexpected thin ids"))) // unlikely
        }
    } else {
        None
    };

    ctx.report
        .info(&format!("total number of roots: {}", all_roots.len()));

    // mapping bottom level
    let root = unpack::<SMRoot>(&sb.data_sm_root[0..])?;
    let data_sm = core_sm(root.nr_blocks, nr_devs as u32);
    let summaries = check_mapping_bottom_level(
        &ctx,
        &metadata_sm,
        &data_sm,
        &all_roots,
        opts.ignore_non_fatal,
    )?;

    // Check the number of mapped blocks
    let mut iter = roots
        .iter()
        .zip(devs.values())
        .map(|((thin_id, (_, root)), details)| (thin_id, root, details));
    check_mapped_blocks(&ctx, &mut iter, &summaries)?;

    if let Some(Err(e)) = sb_snap {
        return Err(anyhow!("metadata snapshot contains errors: {}", e));
    }

    match thins_snap {
        Some(Err(e)) => {
            return Err(anyhow!("metadata snapshot contains errors: {}", e));
        }
        Some(Ok(thins)) => {
            let mut iter = thins.iter().map(|(id, root, details)| (id, root, details));
            check_mapped_blocks(&ctx, &mut iter, &summaries)?;
        }
        None => {}
    }

    //-----------------------------------------

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
    )?;
    let duration = start.elapsed();
    ctx.report
        .info(&format!("checking data space map: {:?}", duration));

    //-----------------------------------------

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
    )?;
    let duration = start.elapsed();
    ctx.report
        .info(&format!("checking metadata space map: {:?}", duration));

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

    if !roots.keys().eq(devs.keys()) {
        return Err(anyhow!(
            "Inconsistency between the details tree and the mapping tree"
        ));
    }

    // mapping bottom level
    let root = unpack::<SMRoot>(&sb.data_sm_root[0..])?;
    let data_sm = core_sm(root.nr_blocks, nr_devs as u32);
    let all_roots: Vec<u64> = roots.values().map(|(_path, root)| *root).collect();
    let summaries = check_mapping_bottom_level(&ctx, &metadata_sm, &data_sm, &all_roots, false)?;

    let mut iter = roots
        .iter()
        .zip(devs.values())
        .map(|((thin_id, (_, root)), details)| (thin_id, root, details));
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

    monitor.stop();

    Ok(CheckMaps {
        metadata_sm: metadata_sm.clone(),
        data_sm: data_sm.clone(),
    })
}

//------------------------------------------
