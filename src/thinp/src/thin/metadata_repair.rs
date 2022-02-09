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
use crate::pdata::space_map_common::*;
use crate::pdata::unpack::Unpack;
use crate::report::Report;
use crate::thin::block_time::*;
use crate::thin::device_detail::*;
use crate::thin::superblock::*;

//------------------------------------------

#[derive(Copy, Clone, Default)]
pub struct SuperblockOverrides {
    pub transaction_id: Option<u64>,
    pub data_block_size: Option<u32>,
    pub nr_data_blocks: Option<u64>,
}

pub struct FoundRoots {
    mapping_root: u64,
    details_root: u64,
    time: u32,
    transaction_id: u64,
    nr_data_blocks: u64,
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
        if self.key_high > 0 && child.key_low <= self.key_high {
            return Err(anyhow!("incompatible child"));
        }
        if !self.pushed {
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
        if self.key_high > 0 && child.key_low <= self.key_high {
            return Err(anyhow!("incompatible child"));
        }
        if !self.pushed {
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
        if self.key_high > 0 && child.key_low <= self.key_high {
            return Err(anyhow!("incompatible child"));
        }
        if !self.pushed {
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
    report: Arc<Report>,
}

impl NodeCollector {
    pub fn new(engine: Arc<dyn IoEngine + Send + Sync>, report: Arc<Report>) -> NodeCollector {
        let nr_blocks = engine.get_nr_blocks();
        NodeCollector {
            engine,
            nr_blocks,
            examined: FixedBitSet::with_capacity(nr_blocks as usize),
            referenced: FixedBitSet::with_capacity(nr_blocks as usize),
            infos: BTreeMap::<u64, NodeInfo>::new(),
            report,
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

    fn is_top_level(&self, values: &[u64]) -> bool {
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
                    if self.is_top_level(values) {
                        self.gather_dev_leaf_info(header, keys, values)
                    } else {
                        // FIXME: convert the values only, to avoid unpacking the node twice
                        let node = unpack_node::<BlockTime>(&[0], blk.get_data(), true, true)?;
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
                }
            }
        } else if hdr.value_size == DeviceDetail::disk_size() {
            let node = unpack_node::<DeviceDetail>(&[0], blk.get_data(), true, true)?;
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
        } else {
            Err(anyhow!("not the value size of interest"))
        }
    }

    fn read_info(&self, b: u64) -> Result<&NodeInfo> {
        if self.examined.contains(b as usize) {
            // TODO: use an extra 'valid' bitset for faster lookup?
            self.infos
                .get(&b)
                .ok_or_else(|| anyhow!("block {} was examined as invalid", b))
        } else {
            Err(anyhow!("not examined"))
        }
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

    fn gather_roots(&self) -> Result<(Vec<u64>, Vec<u64>)> {
        let mut dev_roots = Vec::<u64>::new();
        let mut details_roots = Vec::<u64>::new();

        for b in 0..self.nr_blocks {
            // skip non-root blocks
            if self.referenced.contains(b as usize) {
                continue;
            }

            // skip blocks we're not interested in
            let info = self.read_info(b as u64);
            if info.is_err() {
                continue;
            }
            let info = info.unwrap();

            match info {
                NodeInfo::Dev(_) => dev_roots.push(b),
                NodeInfo::Details(_) => details_roots.push(b),
                _ => {}
            };
        }

        Ok((dev_roots, details_roots))
    }

    fn find_roots_with_compatible_ids(
        &mut self,
        dev_roots: &[u64],
        details_roots: &[u64],
    ) -> Result<Vec<(u64, u64)>> {
        let mut root_pairs = Vec::<(u64, u64)>::new();

        for dev_root in dev_roots {
            let mut path = vec![0];
            let ids1 = btree_to_key_set::<u64>(&mut path, self.engine.clone(), true, *dev_root)?;

            for details_root in details_roots {
                let mut path = vec![0];
                let ids2 = btree_to_key_set::<DeviceDetail>(
                    &mut path,
                    self.engine.clone(),
                    true,
                    *details_root,
                )?;

                if ids1 != ids2 {
                    continue;
                }

                root_pairs.push((*dev_root, *details_root));
            }
        }

        Ok(root_pairs)
    }

    fn filter_roots_with_incompatible_mapped_blocks(
        &self,
        root_pairs: &[(u64, u64)],
    ) -> Result<Vec<(u64, u64)>> {
        let mut filtered = Vec::<(u64, u64)>::new();

        for (dev_root, details_root) in root_pairs {
            let dev_info;
            if let NodeInfo::Dev(i) = self.read_info(*dev_root)? {
                dev_info = i;
            } else {
                continue;
            }

            let details_info;
            if let NodeInfo::Details(i) = self.read_info(*details_root)? {
                details_info = i;
            } else {
                continue;
            }

            // FIXME: compare the ages
            if dev_info.nr_devices != details_info.nr_devices
                || dev_info.nr_mappings != details_info.nr_mappings
            {
                continue;
            }

            filtered.push((*dev_root, *details_root));
        }

        Ok(filtered)
    }

    fn compare_time_counts(lhs: &BTreeMap<u32, u32>, rhs: &BTreeMap<u32, u32>) -> Ordering {
        let mut lhs_it = lhs.iter().rev();
        let mut rhs_it = rhs.iter().rev();
        let mut lhs_end = false;
        let mut rhs_end = false;

        while !lhs_end && !rhs_end {
            if let Some((lhs_time, lhs_count)) = lhs_it.next() {
                if let Some((rhs_time, rhs_count)) = rhs_it.next() {
                    if lhs_time > rhs_time {
                        return Ordering::Less;
                    } else if rhs_time > lhs_time {
                        return Ordering::Greater;
                    } else if lhs_count > rhs_count {
                        return Ordering::Less;
                    } else if rhs_count > lhs_count {
                        return Ordering::Greater;
                    }
                } else {
                    rhs_end = true;
                }
            } else {
                lhs_end = true;
            }
        }

        if lhs_end && !rhs_end {
            Ordering::Less
        } else if !lhs_end && rhs_end {
            Ordering::Greater
        } else {
            Ordering::Equal
        }
    }

    fn compare_roots(lhs: &(&DevInfo, u64), rhs: &(&DevInfo, u64)) -> Ordering {
        // TODO: sort by other criteria?
        Self::compare_time_counts(&lhs.0.time_counts, &rhs.0.time_counts)
    }

    fn sort_roots(&self, root_pairs: &[(u64, u64)]) -> Result<Vec<(u64, u64)>> {
        let mut infos = Vec::<(&DevInfo, u64)>::new();

        for (dev_root, details_root) in root_pairs {
            let dev_info;
            if let NodeInfo::Dev(i) = self.read_info(*dev_root)? {
                dev_info = i;
            } else {
                continue;
            }

            infos.push((dev_info, *details_root));
        }

        infos.sort_by(Self::compare_roots);

        let mut sorted = Vec::<(u64, u64)>::new();
        for (dev_info, details_root) in infos {
            sorted.push((dev_info.b, details_root));
        }

        Ok(sorted)
    }

    fn find_root_pairs(
        &mut self,
        dev_roots: &[u64],
        details_roots: &[u64],
    ) -> Result<Vec<(u64, u64)>> {
        let pairs = self.find_roots_with_compatible_ids(dev_roots, details_roots)?;
        let pairs = self.filter_roots_with_incompatible_mapped_blocks(&pairs)?;
        self.sort_roots(&pairs)
    }

    fn to_found_roots(&self, dev_root: u64, details_root: u64) -> Result<FoundRoots> {
        let dev_info;
        if let NodeInfo::Dev(i) = self.read_info(dev_root)? {
            dev_info = i;
        } else {
            return Err(anyhow!("not a top-level root"));
        }

        let details_info;
        if let NodeInfo::Details(i) = self.read_info(details_root)? {
            details_info = i;
        } else {
            return Err(anyhow!("not a details root"));
        }

        Ok(FoundRoots {
            mapping_root: dev_root,
            details_root,
            time: std::cmp::max(dev_info.age, details_info.age),
            transaction_id: details_info.max_tid + 1, // tid in superblock is ahead by 1
            nr_data_blocks: dev_info.highest_mapped_data_block + 1,
        })
    }

    fn log_results(&self, dev_roots: &[u64], details_roots: &[u64], pairs: &[(u64, u64)]) {
        self.report
            .info(&format!("mapping candidates ({}):", dev_roots.len()));
        for dev_root in dev_roots {
            if let Ok(NodeInfo::Dev(info)) = self.read_info(*dev_root) {
                self.report.info(&format!("b={}, nr_devices={}, nr_mappings={}, highest_mapped={}, age={}, time_counts={:?}",
                info.b, info.nr_devices, info.nr_mappings, info.highest_mapped_data_block, info.age, info.time_counts));
            }
        }

        self.report
            .info(&format!("\ndevice candidates ({}):", details_roots.len()));
        for details_root in details_roots {
            if let Ok(NodeInfo::Details(info)) = self.read_info(*details_root) {
                self.report.info(&format!(
                    "b={}, nr_devices={}, nr_mappings={}, max_tid={}, age={}",
                    info._b, info.nr_devices, info.nr_mappings, info.max_tid, info.age
                ));
            }
        }

        self.report
            .info(&format!("\ncompatible roots ({}):", pairs.len()));
        for pair in pairs {
            self.report.info(&format!("({}, {})", pair.0, pair.1));
        }
    }

    pub fn find_roots(mut self) -> Result<FoundRoots> {
        self.collect_infos()?;
        let (dev_roots, details_roots) = self.gather_roots()?;
        let pairs = self.find_root_pairs(&dev_roots, &details_roots)?;
        self.log_results(&dev_roots, &details_roots, &pairs);

        if pairs.is_empty() {
            return Err(anyhow!("no compatible roots found"));
        }

        self.to_found_roots(pairs[0].0, pairs[0].1)
    }
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
    let mut path = vec![0];
    let ids1 =
        btree_to_key_set::<u64>(&mut path, engine.clone(), ignore_non_fatal, sb.mapping_root);

    path = vec![0];
    let ids2 = btree_to_key_set::<DeviceDetail>(
        &mut path,
        engine.clone(),
        ignore_non_fatal,
        sb.details_root,
    );

    if ids1.is_err() || ids2.is_err() || ids1.unwrap() != ids2.unwrap() {
        return Err(anyhow::Error::new(SuperblockError {
            failed_sb: Some(sb),
        })
        .context("inconsistent device ids"));
    }

    Ok(sb)
}

pub fn rebuild_superblock(
    engine: Arc<dyn IoEngine + Send + Sync>,
    report: Arc<Report>,
    ref_sb: Option<Superblock>,
    opts: &SuperblockOverrides,
) -> Result<Superblock> {
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

    let c = NodeCollector::new(engine.clone(), report);
    let roots = c.find_roots()?;

    let transaction_id = opts
        .transaction_id
        .or_else(|| ref_sb.as_ref().map(|sb| sb.transaction_id))
        .filter(|tid| *tid > roots.transaction_id)
        .unwrap_or(roots.transaction_id);

    let nr_data_blocks = opts
        .nr_data_blocks
        .or_else(|| {
            ref_sb.as_ref().and_then(|sb| {
                unpack_root(&sb.data_sm_root)
                    .ok()
                    .map(|root| root.nr_blocks)
            })
        })
        .filter(|n| *n > roots.nr_data_blocks)
        .unwrap_or(roots.nr_data_blocks);

    let sm_root = SMRoot {
        nr_blocks: nr_data_blocks,
        nr_allocated: 0,
        bitmap_root: 0,
        ref_count_root: 0,
    };
    let data_sm_root = pack_root(&sm_root, SPACE_MAP_ROOT_SIZE)?;

    Ok(Superblock {
        flags: SuperblockFlags { needs_check: false },
        block: SUPERBLOCK_LOCATION,
        version: 2,
        time: roots.time,
        transaction_id,
        metadata_snap: 0,
        data_sm_root,
        metadata_sm_root: vec![0u8; SPACE_MAP_ROOT_SIZE],
        mapping_root: roots.mapping_root,
        details_root: roots.details_root,
        data_block_size,
        nr_metadata_blocks: 0,
    })
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
            let sm_root = unpack_root(&self.data_sm_root).map(|mut root| {
                root.nr_blocks = std::cmp::max(nr_data_blocks, root.nr_blocks);
                root
            })?;
            self.data_sm_root = pack_root(&sm_root, SPACE_MAP_ROOT_SIZE)?;
        }

        Ok(self)
    }
}

pub fn override_superblock(mut sb: Superblock, opts: &SuperblockOverrides) -> Result<Superblock> {
    if let Some(tid) = opts.transaction_id {
        sb.transaction_id = std::cmp::max(tid, sb.transaction_id);
    }

    if let Some(bs) = opts.data_block_size {
        sb.data_block_size = bs;
    }

    if let Some(nr_data_blocks) = opts.nr_data_blocks {
        let sm_root = unpack_root(&sb.data_sm_root).map(|mut root| {
            root.nr_blocks = std::cmp::max(nr_data_blocks, root.nr_blocks);
            root
        })?;
        sb.data_sm_root = pack_root(&sm_root, SPACE_MAP_ROOT_SIZE)?;
    }

    Ok(sb)
}

pub fn read_or_rebuild_superblock(
    engine: Arc<dyn IoEngine + Send + Sync>,
    report: Arc<Report>,
    loc: u64,
    opts: &SuperblockOverrides,
) -> Result<Superblock> {
    read_superblock(engine.as_ref(), loc)
        .and_then(|sb| is_superblock_consistent(sb, engine.clone(), true))
        .and_then(|sb| sb.overrides(opts))
        .or_else(|e| {
            let ref_sb = e
                .downcast_ref::<SuperblockError>()
                .and_then(|err| err.failed_sb.clone());
            rebuild_superblock(engine, report, ref_sb, opts)
        })
}

//------------------------------------------
