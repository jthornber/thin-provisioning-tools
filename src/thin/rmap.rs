use anyhow::Result;
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use std::io::BufWriter;
use std::io::Write;
use std::ops::DerefMut;
use std::ops::Range;
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::commands::engine::*;
use crate::io_engine::*;
use crate::pdata::btree::{self, KeyRange, NodeHeader};
use crate::pdata::btree_walker::{btree_to_map, BTreeWalker, NodeVisitor};
use crate::pdata::space_map::RestrictedSpaceMap;
use crate::report::Report;
use crate::thin::block_time::BlockTime;
use crate::thin::superblock::*;

//------------------------------------------

#[derive(Clone, Copy, Default)]
struct RmapRegion {
    begin: u64,
    end: u64,
    dev_id: u32,
    thin_begin: u64,
}

impl RmapRegion {
    fn adjacent(&mut self, dev_id: u32, thin_block: u64, data_block: u64) -> bool {
        let run_len = self.end - self.begin;

        if run_len == 0 {
            self.begin = data_block;
            self.end = data_block + 1;
            self.dev_id = dev_id;
            self.thin_begin = thin_block;
            return true;
        }

        if dev_id != self.dev_id
            || data_block != self.end
            || thin_block != self.thin_begin + run_len
        {
            return false;
        }

        self.end += 1;
        true
    }

    fn reset(&mut self, dev_id: u32, thin_block: u64, data_block: u64) {
        self.dev_id = dev_id;
        self.begin = data_block;
        self.end = data_block + 1;
        self.thin_begin = thin_block;
    }

    fn compare(lhs: &Self, rhs: &Self) -> Ordering {
        if lhs.begin < rhs.begin {
            Ordering::Less
        } else if lhs.begin > rhs.begin {
            Ordering::Greater
        } else if lhs.end < rhs.end {
            Ordering::Less
        } else if lhs.end > rhs.end {
            Ordering::Greater
        } else if lhs.dev_id < rhs.dev_id {
            Ordering::Less
        } else if lhs.dev_id > rhs.dev_id {
            Ordering::Greater
        } else if lhs.thin_begin < rhs.thin_begin {
            Ordering::Less
        } else if lhs.thin_begin > rhs.thin_begin {
            Ordering::Greater
        } else {
            Ordering::Equal
        }
    }
}

impl Display for RmapRegion {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "data {}..{} -> thin({}) {}..{}",
            self.begin,
            self.end,
            self.dev_id,
            self.thin_begin,
            self.thin_begin + (self.end - self.begin)
        )
    }
}

//------------------------------------------

struct RmapInner {
    rmap: Vec<RmapRegion>,
    current: RmapRegion,
    dev_id: u32,
}

struct RmapVisitor {
    inner: Mutex<RmapInner>,
    regions: Vec<Range<u64>>,
}

impl RmapVisitor {
    fn new(regions: Vec<Range<u64>>) -> RmapVisitor {
        RmapVisitor {
            inner: Mutex::new(RmapInner {
                rmap: Vec::new(),
                current: RmapRegion::default(),
                dev_id: 0,
            }),
            regions,
        }
    }

    fn set_dev_id(&self, dev_id: u32) {
        let mut inner = self.inner.lock().unwrap();
        inner.dev_id = dev_id;
    }

    fn in_regions(&self, b: u64) -> bool {
        for range in self.regions.iter() {
            if range.contains(&b) {
                return true;
            }
        }
        false
    }

    fn complete(self) -> Result<Vec<RmapRegion>> {
        let mut inner = self.inner.into_inner()?;

        if inner.current.end > inner.current.begin {
            inner.rmap.push(inner.current);
        }

        let mut rmap = Vec::new();
        std::mem::swap(&mut inner.rmap, &mut rmap);
        rmap.sort_by(RmapRegion::compare);

        Ok(rmap)
    }
}

impl NodeVisitor<BlockTime> for RmapVisitor {
    fn visit(
        &self,
        _path: &[u64],
        _kr: &KeyRange,
        _h: &NodeHeader,
        keys: &[u64],
        values: &[BlockTime],
    ) -> btree::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let inner = inner.deref_mut();
        for (k, v) in keys.iter().zip(values) {
            if !self.in_regions(v.block) {
                continue;
            }

            if !inner.current.adjacent(inner.dev_id, *k, v.block) {
                inner.rmap.push(inner.current);
                inner.current.reset(inner.dev_id, *k, v.block);
            }
        }

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

pub struct ThinRmapOptions<'a> {
    pub input: &'a Path,
    pub engine_opts: EngineOptions,
    pub regions: Vec<Range<u64>>,
    pub report: Arc<Report>,
}

struct Context {
    engine: Arc<dyn IoEngine + Send + Sync>,
    _report: Arc<Report>, // TODO: report the scanning progress
}

fn mk_context(opts: &ThinRmapOptions) -> Result<Context> {
    let engine = EngineBuilder::new(opts.input, &opts.engine_opts).build()?;

    Ok(Context {
        engine,
        _report: opts.report.clone(),
    })
}

pub fn rmap(opts: ThinRmapOptions) -> Result<()> {
    let ctx = mk_context(&opts)?;

    let sb = read_superblock(ctx.engine.as_ref(), SUPERBLOCK_LOCATION)?;
    let metadata_sm = Arc::new(Mutex::new(RestrictedSpaceMap::new(
        ctx.engine.get_nr_blocks(),
    )));

    let mut path = Vec::new();
    let roots = btree_to_map(&mut path, ctx.engine.clone(), false, sb.mapping_root)?;

    let rv = RmapVisitor::new(opts.regions);
    let w = Arc::new(BTreeWalker::new_with_sm(
        ctx.engine.clone(),
        metadata_sm,
        false,
    )?);
    for (dev_id, root) in roots.iter() {
        // TODO: multi-threaded
        rv.set_dev_id(*dev_id as u32);
        path.clear();
        w.walk(&mut path, &rv, *root)?;
    }

    let rmap = rv.complete()?;
    let mut writer = BufWriter::new(std::io::stdout());
    for m in rmap.iter() {
        writer.write_all(m.to_string().as_bytes())?;
        writer.write_all(b"\n")?;
    }

    Ok(())
}
