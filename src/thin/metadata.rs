use anyhow::Result;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::io_engine::IoEngine;
use crate::pdata::btree::{self, *};
use crate::pdata::btree_leaf_walker::*;
use crate::pdata::btree_walker::*;
use crate::pdata::space_map::*;
use crate::thin::block_time::*;
use crate::thin::device_detail::*;
use crate::thin::runs::*;
use crate::thin::superblock::*;

//------------------------------------------

type DefId = u64;
type ThinId = u32;

#[derive(Clone)]
pub enum Entry {
    Leaf(u64),
    Ref(DefId),
}

#[derive(Clone)]
pub struct Mapping {
    pub kr: KeyRange,
    pub entries: Vec<Entry>,
}

#[derive(Clone)]
pub struct Device {
    pub thin_id: ThinId,
    pub detail: DeviceDetail,
    pub map: Mapping,
}

#[derive(Clone)]
pub struct Def {
    pub def_id: DefId,
    pub map: Mapping,
}

#[derive(Clone)]
pub struct Metadata {
    pub defs: Vec<Def>,
    pub devs: Vec<Device>,
    nr_blocks: u64,
}

//------------------------------------------

pub struct CoreSuperblock {
    pub devices: BTreeMap<u64, (u64, DeviceDetail)>,
    pub flags: SuperblockFlags,
    pub version: u32,
    pub time: u32,
    pub transaction_id: u64,
    pub data_block_size: u32,
    pub nr_data_blocks: u64,
}

pub enum ThinSuperblock {
    OnDisk(Superblock),
    InCore(CoreSuperblock),
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

pub fn build_metadata(
    engine: Arc<dyn IoEngine + Send + Sync>,
    sb: &ThinSuperblock,
) -> Result<Metadata> {
    build_metadata_with_dev(engine, sb, None)
}

fn build_metadata_with_dev_(
    engine: Arc<dyn IoEngine + Send + Sync>,
    devices: &BTreeMap<u64, (u64, DeviceDetail)>,
) -> Result<Metadata> {
    let mapping_roots: BTreeSet<u64> = devices.values().map(|(root, _)| *root).collect();
    let entry_map = collect_leaves(engine.clone(), &mapping_roots)?;

    let devs: Vec<Device> = devices
        .iter()
        .map(|(&thin_id, &(root, detail))| {
            let kr = KeyRange::new(); // FIXME: finish
            let es = entry_map.get(&root).unwrap();
            Device {
                thin_id: thin_id as u32,
                detail,
                map: Mapping {
                    kr,
                    entries: es.to_vec(),
                },
            }
        })
        .collect();

    Ok(Metadata {
        defs: Vec::new(),
        devs,
        nr_blocks: engine.get_nr_blocks(),
    })
}

fn devices_iter(
    engine: Arc<dyn IoEngine + Send + Sync>,
    sb: &Superblock,
) -> Result<impl Iterator<Item = (u64, (u64, DeviceDetail))>> {
    // report.set_title("Reading device details");
    let details =
        btree_to_map::<DeviceDetail>(&mut vec![0], engine.clone(), true, sb.details_root)?;
    let roots = btree_to_map::<u64>(&mut vec![0], engine.clone(), true, sb.mapping_root)?;
    Ok(roots
        .into_iter()
        .zip(details.into_values())
        .map(|((thin_id, root), detail)| (thin_id, (root, detail))))
}

pub fn build_metadata_with_dev(
    engine: Arc<dyn IoEngine + Send + Sync>,
    sb: &ThinSuperblock,
    selected_dev: Option<Vec<u64>>,
) -> Result<Metadata> {
    let devs: BTreeMap<u64, (u64, DeviceDetail)> = match sb {
        ThinSuperblock::OnDisk(sb) => {
            let iter = devices_iter(engine.clone(), sb)?;
            if let Some(mut devs) = selected_dev {
                devs.sort_unstable();
                iter.filter(|(k, _)| devs.binary_search(k).is_ok())
                    .collect()
            } else {
                iter.collect()
            }
        }
        ThinSuperblock::InCore(sb) => {
            let iter = sb.devices.iter();
            if let Some(mut devs) = selected_dev {
                devs.sort_unstable();
                iter.filter(|(k, _)| devs.binary_search(k).is_ok())
                    .map(|(&k, &(r, d))| (k, (r, d)))
                    .collect()
            } else {
                iter.map(|(&k, &(r, d))| (k, (r, d))).collect()
            }
        }
    };

    build_metadata_with_dev_(engine, &devs)
}

fn build_metadata_without_mappings_(
    engine: Arc<dyn IoEngine + Send + Sync>,
    details: &mut dyn Iterator<Item = (&u64, &DeviceDetail)>,
) -> Result<Metadata> {
    let to_empty_dev = |(thin_id, detail): (&u64, &DeviceDetail)| Device {
        thin_id: *thin_id as u32,
        detail: *detail,
        map: Mapping {
            kr: KeyRange::new(),
            entries: Vec::new(),
        },
    };

    let devs = details.map(to_empty_dev).collect::<Vec<Device>>();

    Ok(Metadata {
        defs: Vec::new(),
        devs,
        nr_blocks: engine.get_nr_blocks(),
    })
}

pub fn build_metadata_without_mappings(
    engine: Arc<dyn IoEngine + Send + Sync>,
    sb: &ThinSuperblock,
) -> Result<Metadata> {
    match sb {
        ThinSuperblock::OnDisk(sb) => {
            let details =
                btree_to_map::<DeviceDetail>(&mut vec![0], engine.clone(), true, sb.details_root)?;
            build_metadata_without_mappings_(engine, &mut details.iter())
        }
        ThinSuperblock::InCore(sb) => build_metadata_without_mappings_(
            engine,
            &mut sb
                .devices
                .iter()
                .map(|(thin_id, (_, detail))| (thin_id, detail)),
        ),
    }
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

fn build_runs(devs: &[Device], nr_blocks: u64) -> BTreeMap<u64, (Vec<u64>, bool)> {
    let mut g = Gatherer::new(nr_blocks);

    for d in devs {
        gather_entries(&mut g, &d.map.entries);
    }

    // The runs become defs that just contain leaves.
    let mut runs = BTreeMap::new();
    for (run, shared) in g.gather() {
        runs.insert(run[0], (run, shared));
    }

    runs
}

fn entries_to_runs(runs: &BTreeMap<u64, (Vec<u64>, bool)>, es: &[Entry]) -> Vec<Entry> {
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
                if let Some((run, shared)) = runs.get(&b) {
                    if *shared {
                        result.push(Ref(b));
                    } else {
                        run.iter().for_each(|v| result.push(Leaf(*v)));
                    }
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

fn build_defs(runs: BTreeMap<u64, (Vec<u64>, bool)>) -> Vec<Def> {
    let mut defs = Vec::new();
    for (head, (run, shared)) in runs.iter() {
        if !shared {
            continue;
        }
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
pub fn optimise_metadata(md: Metadata) -> Result<Metadata> {
    let runs = build_runs(&md.devs, md.nr_blocks);

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

    Ok(Metadata {
        defs,
        devs,
        nr_blocks: md.nr_blocks,
    })
}

//------------------------------------------
