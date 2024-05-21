use anyhow::{anyhow, Result};
use fixedbitset::FixedBitSet;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use crate::checksum;
use crate::io_engine::IoEngine;
use crate::pdata::btree::*;
use crate::pdata::btree_walker::*;
use crate::pdata::space_map::common::*;
use crate::pdata::unpack::{unpack, Unpack};
use crate::report::Report;
use crate::thin::block_time::*;
use crate::thin::device_detail::*;
use crate::thin::metadata::{CoreSuperblock, ThinSuperblock};
use crate::thin::superblock::*;

#[cfg(test)]
mod sorting_roots_tests;

//------------------------------------------

#[derive(Copy, Clone, Default)]
pub struct SuperblockOverrides {
    pub transaction_id: Option<u64>,
    pub data_block_size: Option<u32>,
    pub nr_data_blocks: Option<u64>,
}

struct RootPair {
    mapping_root: u64,
    details_root: u64,
}

enum TreeRoots {
    OnDisk(RootPair),
    InCore(BTreeMap<u64, (u64, u64)>), // maps dev_id to (root, mapped_blocks)
}

struct FoundRoots {
    // Following time and transaction_id are suggested values for superblock,
    // given the mapping tree and details tree roots.
    devices: TreeRoots,
    time: u32,
    transaction_id: u64,
    nr_data_blocks: u64,
}

fn devices_identical(
    engine: Arc<dyn IoEngine + Send + Sync>,
    dev_root: u64,
    details_root: u64,
    ignore_non_fatal: bool,
) -> bool {
    let mut path = vec![0];
    let ids1 = btree_to_key_set::<u64>(&mut path, engine.clone(), ignore_non_fatal, dev_root);

    path = vec![0];
    let ids2 = btree_to_key_set::<DeviceDetail>(&mut path, engine, ignore_non_fatal, details_root);

    if ids1.is_err() || ids2.is_err() || ids1.unwrap() != ids2.unwrap() {
        return false;
    }

    true
}

// TODO: generalizing the types
fn lower_bound(infos: &[&DetailsInfo], key: u64) -> usize {
    let mut range = std::ops::Range {
        start: 0,
        end: infos.len(),
    };

    while !range.is_empty() {
        let mid = range.start + range.len() / 2;
        if infos[mid].nr_mappings < key {
            range.start = mid + 1; // FIXME: overflow
        } else {
            range.end = mid;
        }
    }

    range.start
}

// TODO: generalizing the types
fn upper_bound(infos: &[&DetailsInfo], key: u64) -> usize {
    let mut range = std::ops::Range {
        start: 0,
        end: infos.len(),
    };

    while !range.is_empty() {
        let mid = range.start + range.len() / 2;
        if infos[mid].nr_mappings <= key {
            range.start = mid + 1; // FIXME: overflow
        } else {
            range.end = mid;
        }
    }

    range.start
}

fn merge_time_counts(lhs: &mut BTreeMap<u32, u32>, rhs: &BTreeMap<u32, u32>) -> Result<()> {
    for (t, c) in rhs.iter() {
        *lhs.entry(*t).or_insert(0) += c;
    }
    Ok(())
}

struct DevInfo {
    b: u64,
    nr_devices: u64,
    nr_mappings: u64,
    key_low: u64,  // min dev_id
    key_high: u64, // max dev_id, inclusive
    time_counts: BTreeMap<u32, u32>,
    age: u32,
    highest_mapped_data_block: u64,
    pushed: bool,
}

impl DevInfo {
    fn new(b: u64) -> DevInfo {
        DevInfo {
            b,
            nr_devices: 0,
            nr_mappings: 0,
            key_low: 0,
            key_high: 0,
            time_counts: BTreeMap::<u32, u32>::new(),
            age: 0,
            highest_mapped_data_block: 0,
            pushed: false,
        }
    }

    fn push_child(&mut self, child: &DevInfo) -> Result<()> {
        if self.pushed {
            if child.key_low <= self.key_high {
                return Err(anyhow!("incompatible child"));
            }
        } else {
            self.key_low = child.key_low;
            self.pushed = true;
        }
        self.key_high = child.key_high;
        self.nr_devices += child.nr_devices;
        self.nr_mappings += child.nr_mappings;
        merge_time_counts(&mut self.time_counts, &child.time_counts)?;
        self.age = std::cmp::max(self.age, child.age);
        self.highest_mapped_data_block = std::cmp::max(
            self.highest_mapped_data_block,
            child.highest_mapped_data_block,
        );

        Ok(())
    }
}

struct MappingsInfo {
    _b: u64,
    nr_mappings: u64,
    key_low: u64,  // min mapped block
    key_high: u64, // max mapped block, inclusive
    time_counts: BTreeMap<u32, u32>,
    age: u32,
    highest_mapped_data_block: u64,
    pushed: bool,
}

impl MappingsInfo {
    fn new(b: u64) -> MappingsInfo {
        MappingsInfo {
            _b: b,
            nr_mappings: 0,
            key_low: 0,
            key_high: 0,
            time_counts: BTreeMap::<u32, u32>::new(),
            age: 0,
            highest_mapped_data_block: 0,
            pushed: false,
        }
    }

    fn push_child(&mut self, child: &MappingsInfo) -> Result<()> {
        if self.pushed {
            if child.key_low <= self.key_high {
                return Err(anyhow!("incompatible child"));
            }
        } else {
            self.key_low = child.key_low;
            self.pushed = true;
        }
        self.key_high = child.key_high;
        self.nr_mappings += child.nr_mappings;
        merge_time_counts(&mut self.time_counts, &child.time_counts)?;
        self.age = std::cmp::max(self.age, child.age);
        self.highest_mapped_data_block = std::cmp::max(
            self.highest_mapped_data_block,
            child.highest_mapped_data_block,
        );

        Ok(())
    }
}

struct DetailsInfo {
    _b: u64,
    nr_devices: u64,
    nr_mappings: u64,
    key_low: u64,  // min dev_id
    key_high: u64, // max dev_id, inclusive
    max_tid: u64,
    age: u32,
    pushed: bool,
}

impl DetailsInfo {
    fn new(b: u64) -> DetailsInfo {
        DetailsInfo {
            _b: b,
            nr_devices: 0,
            nr_mappings: 0,
            key_low: 0,
            key_high: 0,
            max_tid: 0,
            age: 0,
            pushed: false,
        }
    }

    fn push_child(&mut self, child: &DetailsInfo) -> Result<()> {
        if self.pushed {
            if child.key_low <= self.key_high {
                return Err(anyhow!("incompatible child"));
            }
        } else {
            self.key_low = child.key_low;
            self.pushed = true;
        }
        self.key_high = child.key_high;
        self.nr_devices += child.nr_devices;
        self.nr_mappings += child.nr_mappings;
        self.max_tid = std::cmp::max(self.max_tid, child.max_tid);
        self.age = std::cmp::max(self.age, child.age);

        Ok(())
    }
}

enum NodeInfo {
    Dev(DevInfo),
    Mappings(MappingsInfo),
    Details(DetailsInfo),
}

struct NodeCollector {
    engine: Arc<dyn IoEngine + Send + Sync>,
    nr_blocks: u64,
    examined: FixedBitSet,
    referenced: FixedBitSet,
    infos: BTreeMap<u64, NodeInfo>,
    _report: Arc<Report>,
}

impl NodeCollector {
    fn new(engine: Arc<dyn IoEngine + Send + Sync>, report: Arc<Report>) -> NodeCollector {
        let nr_blocks = engine.get_nr_blocks();
        NodeCollector {
            engine,
            nr_blocks,
            examined: FixedBitSet::with_capacity(nr_blocks as usize),
            referenced: FixedBitSet::with_capacity(nr_blocks as usize),
            infos: BTreeMap::<u64, NodeInfo>::new(),
            _report: report,
        }
    }

    fn gather_dev_subtree_info(
        &mut self,
        header: &NodeHeader,
        _keys: &[u64],
        values: &[u64],
    ) -> Result<NodeInfo> {
        let mut info = DevInfo::new(header.block);

        for b in values {
            let child = self.get_info(*b)?;
            if let NodeInfo::Dev(ref child) = child {
                info.push_child(child)?;
            } else {
                return Err(anyhow!("incompatible child type"));
            }
        }

        Ok(NodeInfo::Dev(info))
    }

    fn gather_mappings_subtree_info(
        &mut self,
        header: &NodeHeader,
        _keys: &[u64],
        values: &[u64],
    ) -> Result<NodeInfo> {
        let mut info = MappingsInfo::new(header.block);

        for b in values {
            let child = self.get_info(*b)?;
            if let NodeInfo::Mappings(ref child) = child {
                info.push_child(child)?;
            } else {
                return Err(anyhow!("incompatible child type"));
            }
        }

        Ok(NodeInfo::Mappings(info))
    }

    fn gather_details_subtree_info(
        &mut self,
        header: &NodeHeader,
        _keys: &[u64],
        values: &[u64],
    ) -> Result<NodeInfo> {
        let mut info = DetailsInfo::new(header.block);

        for b in values {
            let child = self.get_info(*b)?;
            if let NodeInfo::Details(ref child) = child {
                info.push_child(child)?;
            } else {
                return Err(anyhow!("incompatible child type"));
            }
        }

        Ok(NodeInfo::Details(info))
    }

    fn gather_subtree_info(
        &mut self,
        header: &NodeHeader,
        keys: &[u64],
        values: &[u64],
    ) -> Result<NodeInfo> {
        // An internal node should have at least one entry
        if header.nr_entries == 0 {
            return Err(anyhow!("unexpected nr_entries"));
        }

        let first_child = self.get_info(values[0])?;
        let info = match first_child {
            NodeInfo::Dev { .. } => self.gather_dev_subtree_info(header, keys, values),
            NodeInfo::Mappings { .. } => self.gather_mappings_subtree_info(header, keys, values),
            NodeInfo::Details { .. } => self.gather_details_subtree_info(header, keys, values),
        }?;

        // the node is a valid parent, so mark the children as non-orphan
        for b in values {
            self.referenced.set(*b as usize, true);
        }

        Ok(info)
    }

    fn gather_dev_leaf_info(
        &mut self,
        header: &NodeHeader,
        keys: &[u64],
        values: &[u64],
    ) -> Result<NodeInfo> {
        let mut info = DevInfo::new(header.block);
        info.nr_devices = header.nr_entries as u64;

        // min & max device id
        if header.nr_entries > 0 {
            info.key_low = keys[0];
            info.key_high = keys[keys.len() - 1];
        }

        for b in values {
            let child = self.get_info(*b)?; // subtree root
            if let NodeInfo::Mappings(ref child) = child {
                info.nr_mappings += child.nr_mappings;
                merge_time_counts(&mut info.time_counts, &child.time_counts)?;
                info.age = std::cmp::max(info.age, child.age);
                info.highest_mapped_data_block = std::cmp::max(
                    info.highest_mapped_data_block,
                    child.highest_mapped_data_block,
                );
            } else {
                return Err(anyhow!("not a data mapping subtree root"));
            }
        }

        Ok(NodeInfo::Dev(info))
    }

    fn gather_mapping_leaf_info(
        &self,
        header: &NodeHeader,
        keys: &[u64],
        values: &[BlockTime],
    ) -> Result<NodeInfo> {
        let mut info = MappingsInfo::new(header.block);
        info.nr_mappings = header.nr_entries as u64;

        // min & max logical block address
        if header.nr_entries > 0 {
            info.key_low = keys[0];
            info.key_high = keys[keys.len() - 1];
        }

        for bt in values {
            info.highest_mapped_data_block =
                std::cmp::max(bt.block, info.highest_mapped_data_block);
            *info.time_counts.entry(bt.time).or_insert(0) += 1;
            info.age = std::cmp::max(info.age, bt.time);
        }

        Ok(NodeInfo::Mappings(info))
    }

    fn gather_details_leaf_info(
        &self,
        header: &NodeHeader,
        keys: &[u64],
        values: &[DeviceDetail],
    ) -> Result<NodeInfo> {
        let mut info = DetailsInfo::new(header.block);
        info.nr_devices = header.nr_entries as u64;

        // min & max device id
        if header.nr_entries > 0 {
            info.key_low = keys[0];
            info.key_high = keys[keys.len() - 1];
        }

        for details in values {
            info.nr_mappings += details.mapped_blocks;
            info.max_tid = std::cmp::max(info.max_tid, details.transaction_id);
            info.age = std::cmp::max(info.age, details.creation_time);
            info.age = std::cmp::max(info.age, details.snapshotted_time);
        }

        Ok(NodeInfo::Details(info))
    }

    fn maybe_top_level(&self, values: &[u64]) -> bool {
        if values.is_empty() {
            return false;
        }

        for v in values {
            if *v > self.nr_blocks {
                return false;
            }
        }

        true
    }

    fn to_dev_leaf_info(&self, data: &[u8]) -> Result<NodeInfo> {
        let node = unpack_node::<BlockTime>(&[0], data, true, true)?;
        if let Node::Leaf {
            ref header,
            ref keys,
            ref values,
        } = node
        {
            self.gather_mapping_leaf_info(header, keys, values)
        } else {
            Err(anyhow!("unexpected internal node"))
        }
    }

    fn to_details_leaf_info(&self, data: &[u8]) -> Result<NodeInfo> {
        let node = unpack_node::<DeviceDetail>(&[0], data, true, true)?;
        if let Node::Leaf {
            ref header,
            ref keys,
            ref values,
        } = node
        {
            self.gather_details_leaf_info(header, keys, values)
        } else {
            Err(anyhow!("unexpected value size within an internal node"))
        }
    }

    fn gather_info(&mut self, b: u64) -> Result<NodeInfo> {
        let blk = self.engine.read(b)?;

        let bt = checksum::metadata_block_type(blk.get_data());
        if bt != checksum::BT::NODE {
            return Err(anyhow!("not a btree node"));
        }

        let (_, hdr) = NodeHeader::unpack(blk.get_data())?;
        if hdr.value_size as usize == std::mem::size_of::<u64>() {
            let node = unpack_node::<u64>(&[0], blk.get_data(), true, true)?;
            match node {
                Node::Internal {
                    ref header,
                    ref keys,
                    ref values,
                } => self.gather_subtree_info(header, keys, values),
                Node::Leaf {
                    ref header,
                    ref keys,
                    ref values,
                } => {
                    if self.maybe_top_level(values) {
                        self.gather_dev_leaf_info(header, keys, values)
                    } else {
                        // FIXME: convert the values only, to avoid unpacking the node twice
                        self.to_dev_leaf_info(blk.get_data())
                    }
                }
            }
        } else if hdr.value_size == DeviceDetail::disk_size() {
            self.to_details_leaf_info(blk.get_data())
        } else {
            Err(anyhow!("not the value size of interest"))
        }
    }

    fn get_ro_info(&self, b: u64) -> Result<&NodeInfo> {
        self.infos
            .get(&b)
            .ok_or_else(|| anyhow!("block {} is not yet examined", b))
    }

    fn get_info(&mut self, b: u64) -> Result<&NodeInfo> {
        if self.examined.contains(b as usize) {
            // TODO: use an extra 'valid' bitset for faster lookup?
            self.infos
                .get(&b)
                .ok_or_else(|| anyhow!("block {} was examined as invalid", b))
        } else {
            self.examined.set(b as usize, true);
            let info = self.gather_info(b)?;
            Ok(self.infos.entry(b).or_insert(info))
        }
    }

    fn collect_infos(&mut self) -> Result<()> {
        for b in 0..self.nr_blocks {
            let _ret = self.get_info(b);
        }
        Ok(())
    }

    fn gather_roots(&self) -> Result<(Vec<&DevInfo>, Vec<&DetailsInfo>)> {
        let mut dev_roots = Vec::<&DevInfo>::new();
        let mut details_roots = Vec::<&DetailsInfo>::new();

        for (b, info) in self.infos.iter() {
            // skip non-root blocks
            if self.referenced.contains(*b as usize) {
                continue;
            }

            match info {
                NodeInfo::Dev(i) => dev_roots.push(i),
                NodeInfo::Details(i) => details_roots.push(i),
                _ => {}
            };
        }

        Ok((dev_roots, details_roots))
    }
}

//------------------------------------------

// sort the time_counts in descending ordering
fn compare_time_counts(lhs: &BTreeMap<u32, u32>, rhs: &BTreeMap<u32, u32>) -> Ordering {
    let mut lhs_it = lhs.iter().rev();
    let mut rhs_it = rhs.iter().rev();
    let mut lhs_end = false;
    let mut rhs_end = false;

    while !lhs_end && !rhs_end {
        let lhs_elem = lhs_it.next();
        let rhs_elem = rhs_it.next();

        lhs_end = lhs_elem.is_none();
        rhs_end = rhs_elem.is_none();

        if let Some((lhs_time, lhs_count)) = lhs_elem {
            if let Some((rhs_time, rhs_count)) = rhs_elem {
                if lhs_time > rhs_time {
                    return Ordering::Less;
                } else if rhs_time > lhs_time {
                    return Ordering::Greater;
                } else if lhs_count > rhs_count {
                    return Ordering::Less;
                } else if rhs_count > lhs_count {
                    return Ordering::Greater;
                }
            }
        }
    }

    if lhs_end && !rhs_end {
        Ordering::Greater
    } else if !lhs_end && rhs_end {
        Ordering::Less
    } else {
        Ordering::Equal
    }
}

fn filter_details_roots<'a>(
    roots: &[&'a DetailsInfo],
    nr_devices: &[u64],
) -> Result<Vec<&'a DetailsInfo>> {
    let mut filtered = Vec::new();

    for root in roots {
        if nr_devices.contains(&root.nr_devices) {
            filtered.push(*root);
        }
    }

    Ok(filtered)
}

fn find_root_pairs<'a>(
    engine: Arc<dyn IoEngine + Send + Sync>,
    dev_roots: &mut [&'a DevInfo],
    details_roots: &[&'a DetailsInfo],
) -> Result<Vec<(&'a DevInfo, &'a DetailsInfo)>> {
    dev_roots.sort_unstable_by(|lhs, rhs| compare_time_counts(&lhs.time_counts, &rhs.time_counts));

    let mut nr_devices = Vec::new();
    for n in dev_roots.iter().map(|i| i.nr_devices) {
        if !nr_devices.contains(&n) {
            nr_devices.push(n);
        }
    }

    // TODO: use partial sort to avoid extra allocation
    let mut details_infos = filter_details_roots(details_roots, &nr_devices)?;
    details_infos.sort_unstable_by_key(|i| i.nr_mappings);

    let mut pairs = Vec::new();
    for dev in dev_roots {
        // use lowerbound search in case there are duplicated numbers of mappings
        let lower = lower_bound(&details_infos, dev.nr_mappings);
        let upper = upper_bound(&details_infos, dev.nr_mappings);

        for details in &details_infos[lower..upper] {
            if devices_identical(engine.clone(), dev.b, details._b, true) {
                pairs.push((*dev, *details));
            }
        }
    }

    Ok(pairs)
}

fn to_found_roots(dev_root: &DevInfo, details_root: &DetailsInfo) -> Result<FoundRoots> {
    Ok(FoundRoots {
        devices: TreeRoots::OnDisk(RootPair {
            mapping_root: dev_root.b,
            details_root: details_root._b,
        }),
        time: std::cmp::max(dev_root.age, details_root.age),
        transaction_id: details_root.max_tid + 1, // tid in superblock is ahead by 1
        nr_data_blocks: dev_root.highest_mapped_data_block + 1,
    })
}

fn to_partial_found_roots(
    engine: Arc<dyn IoEngine + Send + Sync>,
    dev_root: &DevInfo,
    c: &NodeCollector,
) -> Result<FoundRoots> {
    let roots = btree_to_map(&mut vec![], engine, true, dev_root.b)?;
    let devices = roots
        .iter()
        .map(|(dev_id, root)| {
            if let NodeInfo::Mappings(m) = c.get_ro_info(*root)? {
                Ok((*dev_id, (*root, m.nr_mappings)))
            } else {
                Err(anyhow!("not a subtree root"))
            }
        })
        .collect::<Result<BTreeMap<_, _>>>()?;

    // We'll have to regenerate the device details for all the thins given
    // that the details tree broke, where the transaction_id and timestamps
    // matter:
    // 1. To ensure data safety, we assume all the data mappings are shared,
    //    as the number of shared or exclusive blocks in each device is unknowwn
    //    (or we need a second pass of scanning). Consequently, we convert all
    //    devices into shared snapshots by setting the snapshotted_time greater
    //    than the tree ages.
    // 2. We assume all the devices are created at transaction#0, and the
    //    metadata is at transaction#1. This suggested value then will be
    //    compared with that in the on-disk superblock if available.
    Ok(FoundRoots {
        devices: TreeRoots::InCore(devices),
        time: dev_root.age + 1,
        transaction_id: 1,
        nr_data_blocks: dev_root.highest_mapped_data_block + 1,
    })
}

fn log_results(
    report: Arc<Report>,
    dev_roots: &[&DevInfo],
    details_roots: &[&DetailsInfo],
    pairs: &[(&DevInfo, &DetailsInfo)],
) {
    report.info(&format!("mapping candidates ({}):", dev_roots.len()));
    for info in dev_roots {
        report.info(&format!(
            "b={}, nr_devices={}, nr_mappings={}, highest_mapped={}, age={}, time_counts={:?}",
            info.b,
            info.nr_devices,
            info.nr_mappings,
            info.highest_mapped_data_block,
            info.age,
            info.time_counts
        ));
    }

    report.info(&format!("\ndevice candidates ({}):", details_roots.len()));
    for info in details_roots {
        report.info(&format!(
            "b={}, nr_devices={}, nr_mappings={}, max_tid={}, age={}",
            info._b, info.nr_devices, info.nr_mappings, info.max_tid, info.age
        ));
    }

    report.info(&format!("\ncompatible roots ({}):", pairs.len()));
    for pair in pairs {
        report.info(&format!("({}, {})", pair.0.b, pair.1._b));
    }
}

fn find_roots(
    engine: Arc<dyn IoEngine + Send + Sync>,
    report: Arc<Report>,
) -> Result<Vec<FoundRoots>> {
    let mut c = NodeCollector::new(engine.clone(), report.clone());
    c.collect_infos()?;

    let (mut dev_roots, details_roots) = c.gather_roots()?;

    let pairs = find_root_pairs(engine.clone(), &mut dev_roots, &details_roots)?;
    log_results(report.clone(), &dev_roots, &details_roots, &pairs);

    if pairs.is_empty() {
        if dev_roots.is_empty() {
            return Err(anyhow!("no compatible roots found"));
        }

        report.warning(&format!(
            "found {} mapping trees without device details",
            dev_roots.len()
        ));

        return dev_roots
            .iter()
            .map(|dev| to_partial_found_roots(engine.clone(), dev, &c))
            .collect();
    }

    pairs
        .iter()
        .map(|(dev, details)| to_found_roots(dev, details))
        .collect()
}

fn check_data_block_size(bs: u32) -> Result<u32> {
    if !(128..=2097152).contains(&bs) || (bs & 0x7F != 0) {
        return Err(anyhow!("invalid data block size"));
    }
    Ok(bs)
}

//------------------------------------------

#[derive(Debug)]
struct SuperblockError {
    failed_sb: Option<Superblock>,
}

impl fmt::Display for SuperblockError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.failed_sb)
    }
}

impl std::error::Error for SuperblockError {}

pub fn is_superblock_consistent(
    sb: Superblock,
    engine: Arc<dyn IoEngine + Send + Sync>,
    ignore_non_fatal: bool,
) -> Result<Superblock> {
    if !devices_identical(engine, sb.mapping_root, sb.details_root, ignore_non_fatal) {
        return Err(anyhow::Error::new(SuperblockError {
            failed_sb: Some(sb),
        })
        .context("inconsistent device ids"));
    }

    Ok(sb)
}

fn is_superblock_consistent_(sb: Superblock, found_roots: &[FoundRoots]) -> Result<Superblock> {
    if !found_roots.iter().any(|roots| {
        matches!(&roots.devices, TreeRoots::OnDisk(r) if sb.mapping_root == r.mapping_root && sb.details_root == r.details_root)
    }) {
        return Err(anyhow::Error::new(SuperblockError {
            failed_sb: Some(sb),
        })
        .context("inconsistent superblock"));
    }

    Ok(sb)
}

fn build_devices(
    devs: &BTreeMap<u64, (u64, u64)>,
    tid: u64,
    time: u32,
) -> BTreeMap<u64, (u64, DeviceDetail)> {
    devs.iter()
        .map(|(&dev_id, &(root, mapped_blocks))| {
            let detail = DeviceDetail {
                mapped_blocks,
                transaction_id: tid,
                creation_time: time,
                snapshotted_time: time,
            };
            (dev_id, (root, detail))
        })
        .collect()
}

fn rebuild_superblock(
    roots: &FoundRoots,
    ref_sb: Option<Superblock>,
    opts: &SuperblockOverrides,
) -> Result<ThinSuperblock> {
    // 1. Takes the user overrides
    // 2. Takes the reference if there's no user overrides
    // 3. Returns Err if none of the values above are present
    // 4. Validates the taken value
    let data_block_size = opts
        .data_block_size
        .or_else(|| ref_sb.as_ref().map(|sb| sb.data_block_size))
        .ok_or_else(|| {
            anyhow!("data block size needs to be provided due to corruption in the superblock")
        })
        .and_then(check_data_block_size)?;

    let transaction_id = opts
        .transaction_id
        .or_else(|| ref_sb.as_ref().map(|sb| sb.transaction_id))
        .filter(|tid| *tid > roots.transaction_id)
        .unwrap_or(roots.transaction_id);

    let nr_data_blocks = opts
        .nr_data_blocks
        .or_else(|| {
            ref_sb.as_ref().and_then(|sb| {
                unpack::<SMRoot>(&sb.data_sm_root)
                    .ok()
                    .map(|root| root.nr_blocks)
            })
        })
        .filter(|n| *n > roots.nr_data_blocks)
        .unwrap_or(roots.nr_data_blocks);

    // The minimal timestamp for the rebuilt superblock is the age derived
    // from the data mapping tree or the details tree, which ensures the newly
    // inserted mappings are not shared. However, the actual timestamp in
    // superblock might be later than this age if there are deleted devices.
    // Therefore, we choose the greater value to retain the actual timestamp
    // whenever possible.
    let time = ref_sb
        .as_ref()
        .map_or(roots.time, |sb| std::cmp::max(sb.time, roots.time));

    let sm_root = SMRoot {
        nr_blocks: nr_data_blocks,
        nr_allocated: 0,
        bitmap_root: 0,
        ref_count_root: 0,
    };
    let data_sm_root = pack_root(&sm_root, SPACE_MAP_ROOT_SIZE)?;

    match &roots.devices {
        TreeRoots::OnDisk(r) => Ok(ThinSuperblock::OnDisk(Superblock {
            flags: SuperblockFlags { needs_check: false },
            block: SUPERBLOCK_LOCATION,
            version: 2,
            time,
            transaction_id,
            metadata_snap: 0,
            data_sm_root,
            metadata_sm_root: vec![0u8; SPACE_MAP_ROOT_SIZE],
            mapping_root: r.mapping_root,
            details_root: r.details_root,
            data_block_size,
            nr_metadata_blocks: 0,
        })),
        TreeRoots::InCore(devs) => {
            // the maximal tid among devices should be less than that in superblock
            let devices = build_devices(devs, transaction_id.saturating_sub(1), roots.time);
            Ok(ThinSuperblock::InCore(CoreSuperblock {
                devices,
                flags: SuperblockFlags { needs_check: false },
                version: 2,
                time,
                transaction_id,
                data_block_size,
                nr_data_blocks,
            }))
        }
    }
}

pub trait Override {
    fn overrides(self, opts: &SuperblockOverrides) -> Result<Superblock>;
}

impl Override for Superblock {
    fn overrides(mut self, opts: &SuperblockOverrides) -> Result<Superblock> {
        if let Some(tid) = opts.transaction_id {
            self.transaction_id = std::cmp::max(tid, self.transaction_id);
        }

        if let Some(bs) = opts.data_block_size {
            self.data_block_size = bs;
        }

        if let Some(nr_data_blocks) = opts.nr_data_blocks {
            let sm_root = unpack::<SMRoot>(&self.data_sm_root).map(|mut root| {
                root.nr_blocks = std::cmp::max(nr_data_blocks, root.nr_blocks);
                root
            })?;
            self.data_sm_root = pack_root(&sm_root, SPACE_MAP_ROOT_SIZE)?;
        }

        Ok(self)
    }
}

pub fn read_or_rebuild_superblock(
    engine: Arc<dyn IoEngine + Send + Sync>,
    report: Arc<Report>,
    loc: u64,
    opts: &SuperblockOverrides,
) -> Result<ThinSuperblock> {
    let found_roots = find_roots(engine.clone(), report)?;

    read_superblock(engine.as_ref(), loc)
        .and_then(|sb| is_superblock_consistent_(sb, &found_roots))
        .and_then(|sb| sb.overrides(opts))
        .map_or_else(
            |e| {
                let ref_sb = e
                    .downcast_ref::<SuperblockError>()
                    .and_then(|err| err.failed_sb.clone());
                rebuild_superblock(&found_roots[0], ref_sb, opts)
            },
            |sb| Ok(ThinSuperblock::OnDisk(sb)),
        )
}

//------------------------------------------
