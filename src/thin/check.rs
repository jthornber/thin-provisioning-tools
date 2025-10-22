use anyhow::{anyhow, Error, Result};
use fixedbitset::FixedBitSet;
use rand::seq::SliceRandom;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread;

use crate::commands::engine::*;
use crate::io_engine::*;
use crate::pdata::btree::*;
use crate::pdata::btree_layer_walker::*;
use crate::pdata::btree_utils::*;
use crate::pdata::space_map::aggregator::*;
use crate::pdata::space_map::aggregator_load::*;
use crate::pdata::space_map::aggregator_repair::*;
use crate::pdata::space_map::common::*;
use crate::pdata::unpack::*;
use crate::report::*;
use crate::thin::block_time::*;
use crate::thin::device_detail::*;
use crate::thin::metadata_repair::is_superblock_consistent;
use crate::thin::superblock::*;
use crate::utils::future::spawn_future;
use crate::utils::ranged_bitset_iter::*;

//------------------------------------------

fn get_memory_usage() -> Result<usize, std::io::Error> {
    let mut s = String::new();
    File::open("/proc/self/statm")?.read_to_string(&mut s)?;
    let pages = s
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse::<usize>()
        .unwrap();
    Ok((pages * 4096) / (1024 * 1024))
}

fn print_mem(report: &Report, msg: &str) {
    report.debug(&format!("{}: {} meg", msg, get_memory_usage().unwrap()));
}

//------------------------------------------
// minimum number of entries of a node with 64-bit mapped type
const MIN_ENTRIES: u8 = 84;

fn inc_superblock(sm: &Aggregator, metadata_snap: u64) -> Result<()> {
    sm.inc_single(SUPERBLOCK_LOCATION);
    if metadata_snap > 0 {
        sm.inc_single(metadata_snap);
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

#[derive(Debug, Copy, Clone, Default)]
struct NodeSummary {
    key_low: u64,     // min mapped block
    key_high: u64,    // max mapped block, inclusive
    nr_mappings: u64, // number of valid mappings in this subtree
    nr_entries: u8,   // number of entries in this node, up to 252 given it is the mapping tree
    nr_errors: u8,    // number of errors found in this subtree, up to 255
}

impl NodeSummary {
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
    internal_info: HashMap<u32, InternalNodeInfo>,

    // Stores errors of the node itself; errors in children are not included
    node_errors: HashMap<u32, NodeError>,
}

impl NodeMap {
    fn new(nr_blocks: u32) -> NodeMap {
        NodeMap {
            node_type: FixedBitSet::with_capacity((nr_blocks as usize) * 2),
            leaf_nodes: FixedBitSet::with_capacity(nr_blocks as usize),
            nr_leaves: 0,
            internal_info: HashMap::new(),
            node_errors: HashMap::new(),
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
}

//--------------------------------

struct NodeUpdate {
    loc: u32,
    info: NodeInfo,
}

enum NodeInfo {
    Leaf(),
    Internal(InternalNodeInfo),
    Error(NodeError),
}

const NODE_MAP_BATCH_SIZE: usize = 1024;

struct BatchedNodeMap {
    inner: Mutex<NodeMap>,
}

impl BatchedNodeMap {
    fn new(inner: NodeMap) -> Self {
        Self {
            inner: Mutex::new(inner),
        }
    }

    fn batch_update(&self, updates: Vec<NodeUpdate>) {
        let mut guard = self.inner.lock().unwrap();
        for update in updates {
            match update.info {
                NodeInfo::Leaf() => {
                    let _ = guard.insert_leaf(update.loc);
                }
                NodeInfo::Internal(info) => {
                    let _ = guard.insert_internal_node(update.loc, info);
                }
                NodeInfo::Error(error) => {
                    let _ = guard.insert_error(update.loc, error);
                }
            }
        }
    }
}

//--------------------------------

struct LayerHandler<'a> {
    is_root: bool,
    aggregator: &'a Aggregator,
    ignore_non_fatal: bool,
    nodes: Arc<BatchedNodeMap>,
    children: FixedBitSet,
    updates: Vec<NodeUpdate>,
}

impl<'a> LayerHandler<'a> {
    fn new(
        is_root: bool,
        aggregator: &'a Aggregator,
        ignore_non_fatal: bool,
        nodes: Arc<BatchedNodeMap>,
    ) -> Self {
        Self {
            is_root,
            aggregator,
            ignore_non_fatal,
            nodes,
            children: FixedBitSet::with_capacity(aggregator.get_nr_blocks()),
            updates: Vec::new(),
        }
    }

    fn flush_updates(&mut self) {
        if !self.updates.is_empty() {
            self.nodes.batch_update(std::mem::take(&mut self.updates));
        }
    }

    fn maybe_flush(&mut self) {
        if self.updates.len() >= NODE_MAP_BATCH_SIZE {
            self.flush_updates();
        }
    }

    fn push_error(&mut self, loc: u32, e: NodeError) {
        self.updates.push(NodeUpdate {
            loc,
            info: NodeInfo::Error(e),
        });
    }

    fn push_internal(&mut self, loc: u32, info: InternalNodeInfo) {
        self.updates.push(NodeUpdate {
            loc,
            info: NodeInfo::Internal(info),
        });
    }

    fn get_children(self) -> FixedBitSet {
        self.children
    }
}

impl<'a> ReadHandler for LayerHandler<'a> {
    fn handle(&mut self, loc: u64, data: std::io::Result<&[u8]>) {
        self.maybe_flush();

        match data {
            Ok(data) => {
                if let Err(e) = verify_checksum(data) {
                    self.push_error(loc as u32, e);
                    return;
                }

                let node = unpack_node_raw::<u64>(data, self.ignore_non_fatal, self.is_root);

                if let Err(e) = &node {
                    self.push_error(loc as u32, *e);
                    return;
                }

                let node = node.unwrap();
                if node.get_header().block != loc {
                    self.push_error(loc as u32, NodeError::BlockNrMismatch);
                    return;
                }

                if let Node::Internal { keys, values, .. } = node {
                    // insert the node info in pre-order fashion to better detect loops in the path
                    let children = values.iter().map(|v| *v as u32).collect::<Vec<u32>>(); // FIXME: slow
                    let info = InternalNodeInfo { keys, children };
                    self.push_internal(loc as u32, info);

                    let seen = self.aggregator.test_and_inc(&values);
                    let nr_blocks = self.aggregator.get_nr_blocks() as u64;

                    for (i, v) in values.iter().enumerate() {
                        if !seen.contains(i) && *v < nr_blocks {
                            self.children.insert(*v as usize);
                        }
                    }
                }
            }
            Err(_) => {
                self.push_error(loc as u32, NodeError::IoError);
            }
        }
    }

    fn complete(&mut self) {
        self.flush_updates();
    }
}

fn read_internal_nodes(
    ctx: &Context,
    io_buffers: &mut BufferPool,
    aggregator: &Aggregator,
    root: u32,
    ignore_non_fatal: bool,
    nodes: Arc<BatchedNodeMap>,
) -> Result<()> {
    let nr_blocks = aggregator.get_nr_blocks();
    if root as usize >= nr_blocks {
        return Err(anyhow::anyhow!("block {} out of space map boundary", root));
    }

    let seen = aggregator.test_and_inc(&[root as u64]);
    if seen.contains(0) {
        return Ok(());
    }

    let depth = get_depth::<BlockTime>(ctx.engine.as_ref(), root as u64)?;
    if depth == 0 {
        nodes.batch_update(vec![NodeUpdate {
            loc: root,
            info: NodeInfo::Leaf(),
        }]);
        return Ok(());
    }

    let mut current_layer = FixedBitSet::with_capacity(nr_blocks);

    current_layer.insert(root as usize);

    // Read the internal nodes, layer by layer.
    let mut is_root = true;
    for _d in (0..depth).rev() {
        let mut handler = LayerHandler::new(is_root, aggregator, ignore_non_fatal, nodes.clone());
        is_root = false;

        ctx.engine.read_blocks(
            io_buffers,
            &mut current_layer.ones().map(|n| n as u64),
            &mut handler,
        )?;
        current_layer = handler.get_children();
    }

    // insert leaves
    // FIXME: we've been trying to avoid creating huge arrays!
    let blocks = current_layer
        .ones()
        .map(|loc| NodeUpdate {
            loc: loc as u32,
            info: NodeInfo::Leaf(),
        })
        .collect();
    nodes.batch_update(blocks);

    Ok(())
}

struct SummarizeContext<'a> {
    nodes: &'a NodeMap,
    summaries: &'a mut HashMap<u32, NodeSummary>,
    ignore_non_fatal: bool,
}

#[inline(always)]
fn get_and_check_sum(ctx: &mut SummarizeContext, kr: &KeyRange, block: u32) -> Option<NodeSummary> {
    ctx.summaries.get(&block).map(|sum| {
        // Check the key range against the parent keys.
        if sum.nr_mappings > 0 {
            if let Some(start) = kr.start {
                // The parent key could be less than or equal to,
                // but not greater than the child's first key
                if start > sum.key_low {
                    return NodeSummary::error();
                }
            }
            if let Some(end) = kr.end {
                // note that KeyRange is a right-opened interval
                if end < sum.key_high {
                    return NodeSummary::error();
                }
            }
        }
        *sum
    })
}

// Summarize a subtree rooted at the specified block.
// Only a good internal node will have a summary stored.
// TODO: check the tree is balanced by comparing the height of visited nodes
fn summarize_tree_(
    ctx: &mut SummarizeContext,
    path: &mut Vec<u64>,
    kr: &KeyRange,
    block: u32,
    is_root: bool,
) -> NodeSummary {
    match ctx.nodes.get_type(block) {
        NodeType::Internal => {
            if let Some(info) = ctx.nodes.internal_info.get(&block) {
                // Check underfull
                if !ctx.ignore_non_fatal && !is_root && info.keys.len() < MIN_ENTRIES as usize {
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
                    let child_sums =
                        if let Some(child_sums) = get_and_check_sum(ctx, &child_keys[i], *b) {
                            child_sums
                        } else {
                            summarize_tree_(ctx, path, &child_keys[i], *b, false)
                        };
                    let _ = sum.append(&child_sums);
                    path.pop();
                }

                ctx.summaries.insert(block, sum);

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

fn summarize_tree(
    path: &mut Vec<u64>,
    kr: &KeyRange,
    root: u32,
    is_root: bool,
    nodes: &NodeMap,
    summaries: &mut HashMap<u32, NodeSummary>,
    ignore_non_fatal: bool,
) -> NodeSummary {
    let mut ctx = SummarizeContext {
        nodes,
        summaries,
        ignore_non_fatal,
    };
    if let Some(sum) = get_and_check_sum(&mut ctx, kr, root) {
        sum
    } else {
        summarize_tree_(&mut ctx, path, kr, root, is_root)
    }
}

// Check the mappings filling in the data_sm as we go.
fn check_mappings_bottom_level_(
    ctx: &Context,
    metadata_sm: &Aggregator,
    data_sm: &Arc<Aggregator>,
    roots: &[u64],
    ignore_non_fatal: bool,
) -> Result<HashMap<u32, NodeSummary>> {
    let report = &ctx.report;

    let start = std::time::Instant::now();
    let nodes = collect_nodes_in_use(ctx, metadata_sm, roots, ignore_non_fatal)?;
    let duration = start.elapsed();
    report.debug(&format!("reading internal nodes: {:?}", duration));

    print_mem(&ctx.report, "memory usage before read_leaf_nodes");
    let start = std::time::Instant::now();
    let (nodes, mut summaries) = read_leaf_nodes(ctx, nodes, data_sm, ignore_non_fatal)?;
    let duration = start.elapsed();
    print_mem(&ctx.report, "memory usage after read_leaf_nodes");
    report.debug(&format!("reading leaf nodes: {:?}", duration));
    report.debug(&format!(
        "summaries size: {} bytes",
        std::mem::size_of::<NodeSummary>() * summaries.len()
    ));
    report.debug(&format!(
        "data_sm size: {} meg in {} regions",
        data_sm.rep_size() / (1024 * 1024),
        data_sm.get_nr_regions()
    ));

    let start = std::time::Instant::now();
    count_mapped_blocks(roots, &nodes, &mut summaries, ignore_non_fatal);
    let duration = start.elapsed();
    report.debug(&format!("counting mapped blocks: {:?}", duration));

    report.info(&format!("nr internal nodes: {}", nodes.internal_info.len()));
    report.info(&format!("nr leaves: {}", nodes.nr_leaves));

    if !nodes.node_errors.is_empty() {
        let mut nr_io_errors = 0;
        let mut nr_checksum_errors = 0;
        // FIXME: is it important that the errors are visited in order?  That
        // seems to be the only use case for HashVec.
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
    aggregator: &Aggregator,
    roots: &[u64],
    ignore_non_fatal: bool,
) -> Result<NodeMap> {
    const NR_THREADS: usize = 4;

    let nodes = NodeMap::new(aggregator.get_nr_blocks() as u32);

    if roots.is_empty() {
        return Ok(nodes);
    }

    let batch_nodes = Arc::new(BatchedNodeMap::new(nodes));

    let mut roots: Vec<u64> = roots.to_vec();
    roots.shuffle(&mut rand::rng());

    let chunk_size = roots.len().div_ceil(NR_THREADS);
    let root_chunks: Vec<&[u64]> = roots.chunks(chunk_size).collect();

    thread::scope(|s| {
        for chunk in root_chunks {
            let batch_nodes = batch_nodes.clone();
            s.spawn(move || {
                // FIXME: factor out
                let io_block_size = BLOCK_SIZE;
                let buffer_size = 16 * 1024 * 1024;
                let nr_io_blocks = buffer_size / io_block_size;
                let mut pool = BufferPool::new(nr_io_blocks, io_block_size);

                for &root in chunk {
                    // Allow errors in this phase. Node errors will be captured
                    // later when counting the number of mapped blocks.
                    let _ = read_internal_nodes(
                        ctx,
                        &mut pool,
                        aggregator,
                        root as u32,
                        ignore_non_fatal,
                        batch_nodes.clone(),
                    );
                }
            });
        }
    });

    let nodes = Arc::into_inner(batch_nodes)
        .unwrap()
        .inner
        .into_inner()
        .unwrap();

    Ok(nodes)
}

//--------------------------------

use nom::{bytes::complete::take, number::complete::*, IResult};

#[inline(always)]
fn convert_result<V>(r: IResult<&[u8], V>) -> std::result::Result<(&[u8], V), NodeError> {
    r.map_err(|_e| NodeError::IncompleteData)
}

// Verify the checksum of a node
fn verify_checksum(data: &[u8]) -> std::result::Result<(), NodeError> {
    use crate::checksum;
    match checksum::metadata_block_type(data) {
        checksum::BT::NODE => Ok(()),
        checksum::BT::UNKNOWN => Err(NodeError::ChecksumError),
        _ => Err(NodeError::NotANode),
    }
}

fn examine_leaf_(
    loc: u64,
    data: &[u8],
    ignore_non_fatal: bool,
    data_sm_size: u64,
    data_blocks: &mut Vec<u64>,
) -> std::result::Result<NodeSummary, NodeError> {
    verify_checksum(data)?;

    let (i, header) = NodeHeader::unpack(data).map_err(|_e| NodeError::IncompleteData)?;

    if header.is_leaf && header.value_size != BlockTime::disk_size() {
        return Err(NodeError::ValueSizeMismatch);
    }

    let elt_size = header.value_size + 8;
    if elt_size as usize * header.max_entries as usize + NODE_HEADER_SIZE > BLOCK_SIZE {
        return Err(NodeError::MaxEntriesTooLarge);
    }

    if header.block != loc {
        return Err(NodeError::BlockNrMismatch);
    }

    if header.nr_entries > header.max_entries {
        return Err(NodeError::NumEntriesTooLarge);
    }

    if !ignore_non_fatal && header.max_entries % 3 != 0 {
        return Err(NodeError::MaxEntriesNotDivisible);
    }

    let mut key_low = 0;
    let mut key_high = 0;
    let mut input = i;
    if header.nr_entries > 0 {
        let (i, k) = convert_result(le_u64(input))?;
        input = i;
        key_low = k;
        key_high = k;

        let mut last = k;
        for idx in 1..header.nr_entries {
            let (i, k) = convert_result(le_u64(input))?;
            input = i;

            if k < last {
                return Err(NodeError::KeysOutOfOrder);
            }
            last = k;

            if idx == header.nr_entries - 1 {
                key_high = k;
            }
        }
    }
    let i = input;

    let nr_free = header.max_entries - header.nr_entries;
    let (i, _padding) = convert_result(take(nr_free * 8)(i))?;

    let mut input = i;
    let mut error_mappings = 0;
    for _ in 0..header.nr_entries {
        let (i, bt) = convert_result(BlockTime::unpack(input))?;
        input = i;
        if bt.block >= data_sm_size {
            error_mappings += 1;
            continue;
        }
        data_blocks.push(bt.block);
    }

    let sum = NodeSummary {
        key_low,
        key_high,
        nr_mappings: header.nr_entries as u64 - error_mappings as u64,
        nr_entries: header.nr_entries as u8 - error_mappings,
        nr_errors: if error_mappings > 0 { 1 } else { 0 },
    };

    Ok(sum)
}

struct LeafHandler {
    data_sm: Arc<Aggregator>,
    nodes: Arc<BatchedNodeMap>,
    summaries: Arc<Mutex<HashMap<u32, NodeSummary>>>,
    ignore_non_fatal: bool,

    inc_batch: Vec<u64>,
    summary_batch: Vec<(u32, NodeSummary)>,
    updates: Vec<NodeUpdate>,
}

const INC_BATCH_SIZE: usize = 1024;
const SUMMARY_BATCH_SIZE: usize = 1024;

impl LeafHandler {
    fn new(
        data_sm: Arc<Aggregator>,
        nodes: Arc<BatchedNodeMap>,
        summaries: Arc<Mutex<HashMap<u32, NodeSummary>>>,
        ignore_non_fatal: bool,
    ) -> Self {
        Self {
            data_sm,
            nodes,
            summaries,
            ignore_non_fatal,
            inc_batch: Vec::with_capacity(INC_BATCH_SIZE + 256),
            summary_batch: Vec::with_capacity(SUMMARY_BATCH_SIZE + 256),
            updates: Vec::new(),
        }
    }

    fn flush_incs(&mut self) {
        if self.inc_batch.is_empty() {
            return;
        }

        // Sadly this sort is too slow.

        // FIXME: magic numbers
        // self.inc_batch
        // .sort_unstable_by(|a, b| (a & !1023).cmp(&(b & !1023)));
        self.data_sm.increment(&self.inc_batch);
        self.inc_batch.clear();
    }

    fn maybe_flush_incs(&mut self) {
        if self.inc_batch.len() >= INC_BATCH_SIZE {
            self.flush_incs();
        }
    }

    fn flush_summaries(&mut self) {
        if self.summary_batch.is_empty() {
            return;
        }

        let mut summaries = self.summaries.lock().unwrap();
        for (loc, sum) in &self.summary_batch {
            summaries.insert(*loc, *sum);
        }
        self.summary_batch.clear();
    }

    fn maybe_flush_summaries(&mut self) {
        if self.summary_batch.len() >= SUMMARY_BATCH_SIZE {
            self.flush_summaries();
        }
    }

    fn flush_updates(&mut self) {
        if !self.updates.is_empty() {
            self.nodes.batch_update(std::mem::take(&mut self.updates));
        }
    }

    fn maybe_flush_updates(&mut self) {
        if self.updates.len() >= NODE_MAP_BATCH_SIZE {
            self.flush_updates();
        }
    }

    fn push_error(&mut self, loc: u32, e: NodeError) {
        self.updates.push(NodeUpdate {
            loc,
            info: NodeInfo::Error(e),
        });
    }
}

impl ReadHandler for LeafHandler {
    fn handle(&mut self, loc: u64, data: std::io::Result<&[u8]>) {
        match data {
            Ok(data) => {
                // Allow under full nodes in this phase.  The under full
                // property will be check later based on the path context.
                let sum = examine_leaf_(
                    loc,
                    data,
                    self.ignore_non_fatal,
                    self.data_sm.get_nr_blocks() as u64,
                    &mut self.inc_batch,
                );
                self.maybe_flush_incs();

                match sum {
                    Ok(sum) => {
                        self.summary_batch.push((loc as u32, sum));
                        self.maybe_flush_summaries();
                    }
                    Err(e) => {
                        self.maybe_flush_updates();
                        self.push_error(loc as u32, e);
                    }
                }
            }
            Err(_e) => {
                self.maybe_flush_updates();
                self.push_error(loc as u32, NodeError::IoError);
            }
        }
    }

    fn complete(&mut self) {
        self.flush_incs();
        self.flush_summaries();
        self.flush_updates();
    }
}

fn unpacker(
    engine: Arc<dyn IoEngine>,
    leaves: &mut dyn Iterator<Item = u64>,
    data_sm: Arc<Aggregator>,
    nodes: Arc<BatchedNodeMap>,
    summaries: Arc<Mutex<HashMap<u32, NodeSummary>>>,
    ignore_non_fatal: bool,
) -> Result<()> {
    let io_block_size = 64 * 1024;
    let buffer_size = 16 * 1024 * 1024; // 16m
    let nr_io_blocks = buffer_size / io_block_size;
    let mut buffers = BufferPool::new(nr_io_blocks, io_block_size);

    let mut handler = LeafHandler::new(data_sm, nodes, summaries, ignore_non_fatal);
    engine.read_blocks(&mut buffers, leaves, &mut handler)?;

    Ok(())
}

fn read_leaf_nodes(
    ctx: &Context,
    nodes: NodeMap,
    data_sm: &Arc<Aggregator>,
    ignore_non_fatal: bool,
) -> Result<(NodeMap, HashMap<u32, NodeSummary>)> {
    const NR_UNPACKERS: usize = 4;

    let leaves = nodes.leaf_nodes.clone();
    let summaries = Arc::new(Mutex::new(HashMap::new()));
    let batch_nodes = Arc::new(BatchedNodeMap::new(nodes));

    // Kick off the unpackers
    thread::scope(|s| {
        let chunk_size = leaves.len().div_ceil(NR_UNPACKERS);
        for i in 0..NR_UNPACKERS {
            let l_begin = i * chunk_size;
            let l_end = ((i + 1) * chunk_size).min(leaves.len());
            let mut leaves = RangedBitsetIter::new(&leaves, l_begin..l_end);
            let batch_nodes = batch_nodes.clone();
            let data_sm = data_sm.clone();
            let summaries = summaries.clone();

            s.spawn(move || {
                unpacker(
                    ctx.engine.clone(),
                    &mut leaves,
                    data_sm,
                    batch_nodes,
                    summaries,
                    ignore_non_fatal,
                )
            });
        }
    });

    // extract the results
    let nodes = Arc::into_inner(batch_nodes)
        .unwrap()
        .inner
        .into_inner()
        .unwrap();
    let summaries = Arc::into_inner(summaries).unwrap().into_inner().unwrap();

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
    summaries: &mut HashMap<u32, NodeSummary>,
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
    summaries: &HashMap<u32, NodeSummary>,
) -> Result<()> {
    let report = &ctx.report;

    let start = std::time::Instant::now();
    let mut failed = false;
    for (thin_id, root, details) in devs {
        if let Some(sum) = summaries.get(&(*root as u32)) {
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
        root.nr_blocks.saturating_sub(root.nr_allocated)
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

fn get_devices_(
    engine: &Arc<dyn IoEngine + Send + Sync>,
    sb: &Superblock,
    metadata_sm: &Aggregator,
    ignore_non_fatal: bool,
) -> Result<(BTreeMap<u64, DeviceDetail>, BTreeMap<u64, u64>)> {
    // Mapping top level
    let roots = btree_to_map_with_aggregator::<u64>(
        engine.as_ref(),
        metadata_sm,
        sb.mapping_root,
        ignore_non_fatal,
    )
    .map_err(|e| metadata_err("mapping top-level", e))?;

    // Device details
    let devs = btree_to_map_with_aggregator::<DeviceDetail>(
        engine.as_ref(),
        metadata_sm,
        sb.details_root,
        ignore_non_fatal,
    )
    .map_err(|e| metadata_err("device details tree", e))?;

    Ok((devs, roots))
}

fn get_thins_from_superblock(
    engine: &Arc<dyn IoEngine + Send + Sync>,
    sb: &Superblock,
    metadata_sm: &Aggregator,
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
    metadata_sm: &Aggregator,
    thins: &BTreeMap<u64, (u64, DeviceDetail)>,
    all_roots: &mut Vec<u64>,
    ignore_non_fatal: bool,
) -> Result<BTreeMap<u64, (u64, DeviceDetail)>> {
    // Check consistency between device details and the top-level tree
    is_superblock_consistent(sb_snap.clone(), engine.clone(), ignore_non_fatal)?;

    // Extract roots and details stored in non-shared leaves
    let (devs_snap, roots_snap) = get_devices_(engine, sb_snap, metadata_sm, ignore_non_fatal)?;

    // Select the thins that are exclusive to metadata snapshot for further device details
    // checking. The exclusive thins are those whose tuples (dev_id, root) not present in
    // the top-level tree of the main superblock. Here are some notes:
    //
    // * We must use the full tuple (dev_id, root) to identify exclusive devices in the
    //   context of metadata snapshot for these reasons:
    //   - The data mappings might have been changed after taking a metadata snasphot.
    //   - Thin id reuse: a long-lived metadata snapshot might contains deleted thins
    //     whose dev_id are now used by other new thins.
    //   - Similarly, there might be snapshots created or deleted while the metadata
    //     snapshot is present, resulting devices sharing the same root but with different
    //     dev_id. The tuple (dev_id, root) helps identify these devices.
    // * The roots and details extracted by btree_to_map_with_sm() are those stored in
    //   non-shared leaves. They might not represent the same subset of thins due to
    //   their different value sizes in btree, i.e., shadowing a top-level leaf clones
    //   more entries than shadowing a details tree leaf.
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
    metadata_sm: &Arc<Aggregator>,
    data_sm: &Arc<Aggregator>,
    roots: &[u64],
    ignore_non_fatal: bool,
) -> Result<HashMap<u32, NodeSummary>> {
    use crate::pdata::space_map::SpaceMap;
    let report = &ctx.report;

    let metadata_root = unpack::<SMRoot>(&sb.metadata_sm_root[0..])?;
    let data_root = unpack::<SMRoot>(&sb.data_sm_root[0..])?;

    let mon_meta_sm = metadata_sm.clone();
    let mon_data_sm = data_sm.clone();
    let monitor = ProgressMonitor::new(
        report.clone(),
        metadata_root.nr_allocated + (data_root.nr_allocated / 8),
        move || {
            mon_meta_sm.get_nr_allocated().unwrap() + (mon_data_sm.get_nr_allocated().unwrap() / 8)
        },
    );

    let summaries =
        check_mappings_bottom_level_(ctx, metadata_sm, data_sm, roots, ignore_non_fatal);

    monitor.stop();

    summaries
}

fn create_metadata_sm(sb: &Superblock) -> Result<Arc<Aggregator>> {
    let metadata_root = unpack::<SMRoot>(&sb.metadata_sm_root[0..])?;
    Ok(Arc::new(Aggregator::new(metadata_root.nr_blocks as usize)))
}

fn create_data_sm(sb: &Superblock) -> Result<Arc<Aggregator>> {
    let data_root = unpack::<SMRoot>(&sb.data_sm_root[0..])?;
    Ok(Arc::new(Aggregator::new(data_root.nr_blocks as usize)))
}

//------------------------------------------

struct DiffHandler<'a> {
    entries: &'a [IndexEntry],
    report: &'a Report,
    current_index: Option<u64>,
    leaked_entries: Vec<BitmapLeak>,
    nr_leaks: usize,
    bad_refs: usize,
    kind: &'static str,
}

impl<'a> DiffHandler<'a> {
    fn new(entries: &'a [IndexEntry], report: &'a Report, kind: &'static str) -> Self {
        Self {
            entries,
            report,
            current_index: None,
            leaked_entries: Vec::new(),
            nr_leaks: 0,
            bad_refs: 0,
            kind,
        }
    }

    fn handle(&mut self, block: u64, l_count: u32, r_count: u32) {
        if l_count == 0 && r_count == 1 {
            let bitmap_index = block / ENTRIES_PER_BITMAP as u64;
            if self.current_index != Some(bitmap_index) {
                self.leaked_entries.push(BitmapLeak {
                    loc: self.entries[bitmap_index as usize].blocknr,
                    blocknr: ENTRIES_PER_BITMAP as u64 * bitmap_index,
                });
                self.current_index = Some(bitmap_index);
            }
            self.nr_leaks += 1;
        } else {
            self.bad_refs += 1;
        }
        self.report.debug(&format!(
            "{} count difference for block {}: {}, {}",
            self.kind, block, l_count, r_count
        ));
    }

    fn complete(self) -> (Vec<BitmapLeak>, usize, usize) {
        (self.leaked_entries, self.nr_leaks, self.bad_refs)
    }
}

fn compare_space_maps(
    read_result: anyhow::Result<(Aggregator, Vec<IndexEntry>)>,
    sm: &Aggregator,
    report: &Report,
    kind: &'static str,
) -> Result<Vec<BitmapLeak>> {
    let (sm_on_disk, entries_on_disk) = read_result?;

    let mut handler = DiffHandler::new(&entries_on_disk, report, kind);

    sm.diff(&sm_on_disk, |block, l_count, r_count| {
        handler.handle(block, l_count, r_count)
    });

    let (leaks, nr_leaks, bad_refs) = handler.complete();

    if nr_leaks > 0 {
        report.non_fatal(&format!("{} {} blocks have leaked", nr_leaks, kind));
    }

    if bad_refs > 0 {
        return Err(anyhow!(
            "{} {} blocks have bad reference counts",
            bad_refs,
            kind,
        ));
    }

    Ok(leaks)
}

//------------------------------------------

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
    sb.mapping_root = opts.override_mapping_root.unwrap_or(sb.mapping_root);
    sb.details_root = opts.override_details_root.unwrap_or(sb.details_root);

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

    let metadata_sm = create_metadata_sm(&sb)?;

    inc_superblock(&metadata_sm, sb.metadata_snap)?;

    //------------------------------------
    // Check device details and the top-level tree

    report.set_sub_title("device details tree");

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

    // Collect exclusive thin devices reside in the metadata snapshot only,
    // i.e., devices reachable from the main superblock are not included.
    // Errors are allowed if the option -m is not applied.
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

    //-----------------------------------------
    // Kick off reading the data space map
    let data_root = unpack::<SMRoot>(&sb.data_sm_root[0..])?;
    let data_sm_on_disk_future = if opts.engine_opts.use_metadata_snap {
        None
    } else {
        let engine = ctx.engine.clone();
        let metadata_sm = metadata_sm.clone();
        let report = ctx.report.clone();

        Some(spawn_future(
            move || -> Result<(Aggregator, Vec<IndexEntry>), Error> {
                let start = std::time::Instant::now();
                let (data_sm_on_disk, data_entries_on_disk) =
                    read_data_space_map(engine, data_root, opts.ignore_non_fatal, &metadata_sm)?;
                let duration = start.elapsed();
                report.debug(&format!("reading data sm: {:?}", duration));
                Ok((data_sm_on_disk, data_entries_on_disk))
            },
        ))
    };

    //-----------------------------------------
    // Kick off reading the metadata space map
    let metadata_root = unpack::<SMRoot>(&sb.metadata_sm_root[0..])?;
    let metadata_sm_on_disk_future = if opts.engine_opts.use_metadata_snap {
        None
    } else {
        let engine = ctx.engine.clone();
        let metadata_sm = metadata_sm.clone();
        let report = ctx.report.clone();

        Some(spawn_future(
            move || -> Result<(Aggregator, Vec<IndexEntry>), Error> {
                let start = std::time::Instant::now();
                let (metadata_sm_on_disk, metadata_entries_on_disk) = read_metadata_space_map(
                    engine,
                    metadata_root,
                    opts.ignore_non_fatal,
                    &metadata_sm,
                )?;
                let duration = start.elapsed();
                report.debug(&format!("reading metadata sm: {:?}", duration));
                Ok((metadata_sm_on_disk, metadata_entries_on_disk))
            },
        ))
    };

    //----------------------------------------
    // Check data mappings

    report.set_sub_title("mapping tree");

    let nr_devices = thins.len();
    let nr_devices_snap = thins_snap.as_ref().map_or(0, |thins_snap| thins_snap.len());
    report.info(&format!(
        "number of devices to check: {}{}",
        nr_devices + nr_devices_snap,
        match nr_devices_snap {
            1.. => format!(" ({} exclusive in metadata snapshot)", nr_devices_snap),
            _ => String::new(),
        }
    ));

    let data_sm = create_data_sm(&sb)?;

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
    // Compare the data space maps
    let data_leaks = data_sm_on_disk_future
        .map_or_else(
            || Ok(Vec::new()),
            |future| compare_space_maps(future(), &data_sm, report, "data"),
        )
        .map_err(|e| metadata_err("data space map", e))?;

    //-----------------------------------------
    // Compare the metadata space maps
    let metadata_leaks = metadata_sm_on_disk_future
        .map_or_else(
            || Ok(Vec::new()),
            |future| compare_space_maps(future(), &metadata_sm, report, "metadata"),
        )
        .map_err(|e| metadata_err("metadata space map", e))?;

    //-----------------------------------------
    // Fix minor issues found in the metadata

    if !data_leaks.is_empty() {
        if opts.auto_repair || opts.clear_needs_check {
            report.warning("Repairing data leaks.");
            repair_space_map(engine.clone(), data_leaks, &data_sm)?;
        } else if !opts.ignore_non_fatal {
            return Err(anyhow!(concat!(
                "data space map contains leaks\n",
                "perhaps you wanted to run with --auto-repair"
            )));
        }
    }

    if !metadata_leaks.is_empty() {
        if opts.auto_repair || opts.clear_needs_check {
            report.warning("Repairing metadata leaks.");
            repair_space_map(engine.clone(), metadata_leaks, &metadata_sm)?;
        } else if !opts.ignore_non_fatal {
            return Err(anyhow!(concat!(
                "metadata space map contains leaks\n",
                "perhaps you wanted to run with --auto-repair"
            )));
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
    pub metadata_sm: Arc<Aggregator>,
    pub data_sm: Arc<Aggregator>,
}

pub fn check_with_maps(
    engine: Arc<dyn IoEngine + Send + Sync>,
    report: Arc<Report>,
) -> Result<CheckMaps> {
    let ctx = mk_context_(engine.clone(), report.clone())?;
    report.set_title("Checking thin metadata");

    let sb = read_superblock(engine.as_ref(), SUPERBLOCK_LOCATION)?;
    let metadata_sm = create_metadata_sm(&sb)?;
    inc_superblock(&metadata_sm, sb.metadata_snap)?;

    //-----------------------------------------

    report.set_sub_title("device details tree");

    let mut all_roots = Vec::<u64>::new();
    let thins = get_thins_from_superblock(&engine, &sb, &metadata_sm, &mut all_roots, false)?;

    //-----------------------------------------
    // Kick off reading the data space map
    let data_root = unpack::<SMRoot>(&sb.data_sm_root[0..])?;
    let data_sm_on_disk_future = {
        let engine = ctx.engine.clone();
        let metadata_sm = metadata_sm.clone();
        let report = report.clone();

        spawn_future(move || -> Result<Aggregator, Error> {
            let start = std::time::Instant::now();
            let (data_sm_on_disk, _) = read_data_space_map(engine, data_root, false, &metadata_sm)?;
            let duration = start.elapsed();
            report.debug(&format!("reading data sm: {:?}", duration));
            Ok(data_sm_on_disk)
        })
    };

    //-----------------------------------------
    // Kick off reading the metadata space map
    let metadata_root = unpack::<SMRoot>(&sb.metadata_sm_root[0..])?;
    let metadata_sm_on_disk_future = {
        let engine = ctx.engine.clone();
        let metadata_sm = metadata_sm.clone();
        let report = report.clone();

        spawn_future(move || -> Result<Aggregator, Error> {
            let start = std::time::Instant::now();
            let (metadata_sm_on_disk, _) =
                read_metadata_space_map(engine, metadata_root, false, &metadata_sm)?;
            let duration = start.elapsed();
            report.debug(&format!("reading metadata sm: {:?}", duration));
            Ok(metadata_sm_on_disk)
        })
    };

    //-----------------------------------------

    report.set_sub_title("mapping tree");

    let data_sm = create_data_sm(&sb)?;
    let summaries = check_mappings_bottom_level_(&ctx, &metadata_sm, &data_sm, &all_roots, false)?;

    // Check the number of mapped blocks
    let mut iter = thins
        .iter()
        .map(|(id, (root, details))| (id, root, details));
    check_mapped_blocks(&ctx, &mut iter, &summaries)?;

    //-----------------------------------------
    // Compare the data space maps
    let mut err = None;
    match data_sm_on_disk_future() {
        Ok(data_sm_on_disk) => {
            data_sm.diff(
                &data_sm_on_disk,
                |block: u64, l_count: u32, r_count: u32| {
                    err = Some(anyhow!("bad reference counts"));
                    report.debug(&format!(
                        "data count difference for block {}: {}, {}",
                        block, l_count, r_count
                    ));
                },
            );
        }
        Err(e) => {
            err = Some(e);
        }
    }
    if let Some(e) = err {
        return Err(metadata_err("data space map", e).into());
    }

    //-----------------------------------------
    // Compare the metadata space maps
    match metadata_sm_on_disk_future() {
        Ok(metadata_sm_on_disk) => {
            metadata_sm.diff(
                &metadata_sm_on_disk,
                |block: u64, l_count: u32, r_count: u32| {
                    err = Some(anyhow!("bad reference counts"));
                    report.debug(&format!(
                        "metadata count difference for block {}: {}, {}",
                        block, l_count, r_count
                    ));
                },
            );
        }
        Err(e) => {
            err = Some(e);
        }
    }
    if let Some(e) = err {
        return Err(metadata_err("metadata space map", e).into());
    }

    //-----------------------------------------

    Ok(CheckMaps {
        metadata_sm: metadata_sm.clone(),
        data_sm: data_sm.clone(),
    })
}

//------------------------------------------
