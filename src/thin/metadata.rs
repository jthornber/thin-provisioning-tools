use anyhow::Result;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

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
    sb: &Superblock,
) -> Result<Metadata> {
    let mut path = vec![0];

    // report.set_title("Reading device details");
    let details = btree_to_map::<DeviceDetail>(&mut path, engine.clone(), true, sb.details_root)?;

    // report.set_title("Reading mappings roots");
    let roots;
    {
        let sm = Arc::new(Mutex::new(RestrictedSpaceMap::new(engine.get_nr_blocks())));
        roots =
            btree_to_map_with_path::<u64>(&mut path, engine.clone(), sm, true, sb.mapping_root)?;
    }

    // report.set_title(&format!("Collecting leaves for {} roots", roots.len()));
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
pub fn optimise_metadata(md: Metadata) -> Result<Metadata> {
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
