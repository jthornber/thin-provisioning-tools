use anyhow::Result;
use std::sync::{Arc, Mutex};

use crate::io_engine::*;
use crate::pdata::btree;
use crate::pdata::btree::*;
use crate::pdata::btree_error::*;
use crate::pdata::btree_walker::*;
use crate::pdata::space_map::*;
use crate::pdata::unpack::*;
use crate::write_batcher::*;

//------------------------------------------

// The subtrees will often consist of a single under populated leaf node.  Given this
// we're going to merge by:
// i) Building an ordered list of all leaf nodes across all subtrees.
// ii) Merge leaf nodes where they can be packed more efficiently (non destructively to original subtrees).
// iii) Build higher levels from scratch.  There are very few of these internal nodes compared to leaves anyway.

#[allow(dead_code)]
struct NodeSummary {
    block: u64,
    nr_entries: usize,
    key_low: u64,
    key_high: u64, // inclusive
}

#[allow(dead_code)]
struct LVInner {
    last_key: Option<u64>,
    leaves: Vec<NodeSummary>,
}

struct LeafVisitor {
    inner: Mutex<LVInner>,
}

impl LeafVisitor {
    fn new() -> LeafVisitor {
        LeafVisitor {
            inner: Mutex::new(LVInner {
                last_key: None,
                leaves: Vec::new(),
            }),
        }
    }
}

impl<V: Unpack> NodeVisitor<V> for LeafVisitor {
    fn visit(
        &self,
        path: &[u64],
        _kr: &KeyRange,
        _header: &NodeHeader,
        keys: &[u64],
        _values: &[V],
    ) -> btree::Result<()> {
        // ignore empty nodes
        if keys.is_empty() {
            return Ok(());
        }

        let mut inner = self.inner.lock().unwrap();

        // Check keys are ordered.
        if !inner.leaves.is_empty() {
            let last_key = inner.leaves.last().unwrap().key_high;
            if keys[0] <= last_key {
                return Err(context_err(
                    path,
                    "unable to merge btrees: sub trees out of order",
                ));
            }
        }

        let l = NodeSummary {
            block: *path.last().unwrap(),
            nr_entries: keys.len(),
            key_low: keys[0],
            key_high: *keys.last().unwrap(),
        };

        inner.leaves.push(l);
        Ok(())
    }

    fn visit_again(&self, _path: &[u64], _b: u64) -> btree::Result<()> {
        Ok(())
    }

    fn end_walk(&self) -> btree::Result<()> {
        Ok(())
    }
}

pub type AEngine = Arc<dyn IoEngine + Send + Sync>;

fn collect_leaves<V: Unpack>(engine: AEngine, roots: &[u64]) -> Result<Vec<NodeSummary>> {
    let lv = LeafVisitor::new();
    let walker = BTreeWalker::new(engine, false);

    let mut path = Vec::new();
    for root in roots {
        walker.walk::<LeafVisitor, V>(&mut path, &lv, *root)?;
    }

    Ok(lv.inner.into_inner().unwrap().leaves)
}

//------------------------------------------

#[allow(clippy::extra_unused_type_parameters)]
fn optimise_leaves<V: Unpack + Pack>(
    _batcher: &mut WriteBatcher,
    lvs: Vec<NodeSummary>,
) -> Result<Vec<NodeSummary>> {
    // FIXME: implement
    Ok(lvs)
}

//------------------------------------------

pub fn merge<V: Unpack + Pack>(
    engine: AEngine,
    sm: Arc<Mutex<dyn SpaceMap>>,
    roots: &[u64],
) -> Result<u64> {
    let lvs = collect_leaves::<V>(engine.clone(), roots)?;

    let mut batcher = WriteBatcher::new(engine, sm, 256);
    let _lvs = optimise_leaves::<V>(&mut batcher, lvs)?;

    todo!();
}

//------------------------------------------
