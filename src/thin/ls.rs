use anyhow::{anyhow, Result};
use std::io::Write;
use std::ops::Deref;
use std::path::Path;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use threadpool::ThreadPool;

use crate::grid_layout::GridLayout;
use crate::io_engine::SECTOR_SHIFT;
use crate::io_engine::{AsyncIoEngine, IoEngine, SyncIoEngine};
use crate::pdata::btree::{self, *};
use crate::pdata::btree_walker::*;
use crate::pdata::space_map::*;
use crate::pdata::space_map_common::SMRoot;
use crate::pdata::unpack::unpack;
use crate::report::Report;
use crate::thin::block_time::BlockTime;
use crate::thin::device_detail::DeviceDetail;
use crate::thin::metadata_repair::is_superblock_consistent;
use crate::thin::superblock::*;
use crate::units::*;

//------------------------------------------

pub enum OutputField {
    DeviceId,

    MappedBlocks,
    ExclusiveBlocks,
    SharedBlocks,

    MappedSectors,
    ExclusiveSectors,
    SharedSectors,

    MappedBytes,
    ExclusiveBytes,
    SharedBytes,

    Mapped,
    Exclusive,
    Shared,

    TransactionId,
    CreationTime,
    SnapshottedTime,
}

impl FromStr for OutputField {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use OutputField::*;

        match s {
            "DEV" => Ok(DeviceId),
            "MAPPED_BLOCKS" => Ok(MappedBlocks),
            "EXCLUSIVE_BLOCKS" => Ok(ExclusiveBlocks),
            "SHARED_BLOCKS" => Ok(SharedBlocks),

            "MAPPED_SECTORS" => Ok(MappedSectors),
            "EXCLUSIVE_SECTORS" => Ok(ExclusiveSectors),
            "SHARED_SECTORS" => Ok(SharedSectors),

            "MAPPED_BYTES" => Ok(MappedBytes),
            "EXCLUSIVE_BYTES" => Ok(ExclusiveBytes),
            "SHARED_BYTES" => Ok(SharedBytes),

            "MAPPED" => Ok(Mapped),
            "EXCLUSIVE" => Ok(Exclusive),
            "SHARED" => Ok(Shared),

            "TRANSACTION" => Ok(TransactionId),
            "CREATE_TIME" => Ok(CreationTime),
            "SNAP_TIME" => Ok(SnapshottedTime),

            _ => Err(anyhow!("Unknown field")),
        }
    }
}

impl ToString for OutputField {
    fn to_string(&self) -> String {
        use OutputField::*;

        String::from(match self {
            DeviceId => "DEV",
            MappedBlocks => "MAPPED_BLOCKS",
            ExclusiveBlocks => "EXCLUSIVE_BLOCKS",
            SharedBlocks => "SHARED_BLOCKS",

            MappedSectors => "MAPPED_SECTORS",
            ExclusiveSectors => "EXCLUSIVE_SECTORS",
            SharedSectors => "SHARED_SECTORS",

            MappedBytes => "MAPPED_BYTES",
            ExclusiveBytes => "EXCLUSIVE_BYTES",
            SharedBytes => "SHARED_BYTES",

            Mapped => "MAPPED",
            Exclusive => "EXCLUSIVE",
            Shared => "SHARED",

            TransactionId => "TRANSACTION",
            CreationTime => "CREATE_TIME",
            SnapshottedTime => "SNAP_TIME",
        })
    }
}

//------------------------------------------

// FIXME: duplication of thin::check
struct BottomLevelVisitor {
    data_sm: ASpaceMap,
}

impl NodeVisitor<BlockTime> for BottomLevelVisitor {
    fn visit(
        &self,
        _path: &[u64],
        _kr: &KeyRange,
        _h: &NodeHeader,
        _k: &[u64],
        values: &[BlockTime],
    ) -> btree::Result<()> {
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

pub struct LsTable<'a> {
    fields: &'a [OutputField],
    grid: GridLayout,
    data_block_size: u64,
}

impl<'a> LsTable<'a> {
    fn new(fields: &'a [OutputField], nr_rows: usize, bs: u32) -> LsTable {
        let grid = GridLayout::new_with_size(nr_rows, fields.len());

        LsTable {
            fields,
            grid,
            data_block_size: bs as u64,
        }
    }

    fn push_headers(&mut self) {
        if self.fields.is_empty() {
            return;
        }

        for i in self.fields {
            self.grid.field(i.to_string());
        }
        self.grid.new_row();
    }

    fn push_row(
        &mut self,
        dev_id: u64,
        detail: &DeviceDetail,
        mapped_blocks: u64,
        shared_blocks: u64,
    ) {
        use OutputField::*;

        if self.fields.is_empty() {
            return;
        }

        let bs = self.data_block_size;
        let ex_blocks = mapped_blocks - shared_blocks;

        for field in self.fields {
            let val: u64 = match field {
                DeviceId => dev_id,
                TransactionId => detail.transaction_id,
                CreationTime => detail.creation_time as u64,
                SnapshottedTime => detail.snapshotted_time as u64,
                MappedBlocks => mapped_blocks,
                MappedSectors => mapped_blocks * bs,
                MappedBytes | Mapped => (mapped_blocks * bs) << SECTOR_SHIFT as u64,
                ExclusiveBlocks => ex_blocks,
                ExclusiveSectors => ex_blocks * bs,
                ExclusiveBytes | Exclusive => (ex_blocks * bs) << SECTOR_SHIFT as u64,
                SharedBlocks => shared_blocks,
                SharedSectors => shared_blocks * bs,
                SharedBytes | Shared => (shared_blocks * bs) << SECTOR_SHIFT as u64,
            };

            let cell = match field {
                Mapped | Exclusive | Shared => {
                    let (val, unit) = to_pretty_print_units(val);
                    let mut s = val.to_string();
                    s.push_str(&unit.to_string_short());
                    s
                }
                _ => val.to_string(),
            };

            self.grid.field(cell);
        }
        self.grid.new_row();
    }

    // grid
    pub fn render(&self, w: &mut dyn Write) -> Result<()> {
        self.grid.render(w)
    }
}

//------------------------------------------

const MAX_CONCURRENT_IO: u32 = 1024;

pub struct ThinLsOptions<'a> {
    pub input: &'a Path,
    pub async_io: bool,
    pub use_metadata_snap: bool,
    pub fields: Vec<OutputField>,
    pub no_headers: bool,
    pub report: Arc<Report>,
}

struct Context {
    engine: Arc<dyn IoEngine + Send + Sync>,
    pool: ThreadPool,
    _report: Arc<Report>, // TODO: report the scanning progress
}

fn mk_context(opts: &ThinLsOptions) -> Result<Context> {
    let nr_threads = std::cmp::max(8, num_cpus::get() * 2);

    let engine: Arc<dyn IoEngine + Send + Sync> = if opts.async_io {
        Arc::new(AsyncIoEngine::new(opts.input, MAX_CONCURRENT_IO, false)?)
    } else {
        Arc::new(SyncIoEngine::new(opts.input, nr_threads, false)?)
    };

    let pool = ThreadPool::new(nr_threads);

    Ok(Context {
        engine,
        pool,
        _report: opts.report.clone(),
    })
}

#[derive(Clone, Copy)]
struct NodeInfo {
    nr_mapped: u64,
    nr_shared: u64,
}

impl NodeInfo {
    #[inline]
    fn is_initialized(&self) -> bool {
        self.nr_shared <= self.nr_mapped
    }
}

impl Default for NodeInfo {
    fn default() -> Self {
        NodeInfo {
            nr_mapped: 0,
            nr_shared: 1, // set to uninitialized
        }
    }
}

struct MappingsCollator<'a> {
    engine: Arc<dyn IoEngine>,
    info: Vec<NodeInfo>,
    metadata_sm: &'a dyn SpaceMap,
    data_sm: &'a dyn SpaceMap,
}

impl<'a> MappingsCollator<'a> {
    fn new(
        engine: Arc<dyn IoEngine>,
        metadata_sm: &'a dyn SpaceMap,
        data_sm: &'a dyn SpaceMap,
    ) -> MappingsCollator<'a> {
        let info = vec![NodeInfo::default(); engine.get_nr_blocks() as usize];
        MappingsCollator {
            engine,
            info,
            metadata_sm,
            data_sm,
        }
    }

    fn get_info(&mut self, blocknr: u64) -> Result<(u64, u64)> {
        let info = self.info[blocknr as usize];
        if info.is_initialized() {
            return Ok((info.nr_mapped, info.nr_shared));
        }

        let blk = self.engine.read(blocknr)?;
        let node = unpack_node::<BlockTime>(&[0], blk.get_data(), false, true)?;
        let (nr_mapped, nr_shared) = match node {
            Node::Internal {
                header: _,
                keys: _,
                ref values,
            } => {
                let mut nr_mapped = 0;
                let mut nr_shared = 0;
                for b in values {
                    let r = self.get_info(*b)?;
                    nr_mapped += r.0;
                    nr_shared += r.1;
                }
                if self.metadata_sm.get(blocknr)? > 1 {
                    nr_shared = nr_mapped;
                }
                (nr_mapped, nr_shared)
            }
            Node::Leaf {
                header: _,
                keys: _,
                ref values,
            } => {
                let nr_shared = if self.metadata_sm.get(blocknr)? > 1 {
                    values.len() as u64
                } else {
                    let mut cnt: u64 = 0;
                    for bt in values {
                        if self.data_sm.get(bt.block)? > 1 {
                            cnt += 1;
                        }
                    }
                    cnt
                };
                (values.len() as u64, nr_shared)
            }
        };

        self.info[blocknr as usize] = NodeInfo {
            nr_mapped,
            nr_shared,
        };

        Ok((nr_mapped, nr_shared))
    }
}

fn count_mapped_blocks(ctx: &Context, sb: &Superblock) -> Result<Vec<(u64, u64)>> {
    let mut path = vec![0];
    let sm_root = unpack::<SMRoot>(&sb.data_sm_root[..])?;
    let data_sm = Arc::new(Mutex::new(RestrictedTwoSpaceMap::new(sm_root.nr_blocks)));
    let metadata_sm = Arc::new(Mutex::new(RestrictedTwoSpaceMap::new(
        ctx.engine.get_nr_blocks(),
    )));

    // 1st pass
    let w = Arc::new(BTreeWalker::new_with_sm(
        ctx.engine.clone(),
        metadata_sm.clone(),
        false,
    )?);
    let roots = btree_to_map::<u64>(&mut path, ctx.engine.clone(), false, sb.mapping_root)?;
    for root in roots.values() {
        let v = Arc::new(BottomLevelVisitor {
            data_sm: data_sm.clone(),
        });
        walk_threaded(&mut path, w.clone(), &ctx.pool, v, *root)?;
    }

    // 2nd pass
    // TODO: multi-threaded?
    let metadata_sm = metadata_sm.lock().unwrap();
    let data_sm = data_sm.lock().unwrap();
    let mut c = MappingsCollator::new(ctx.engine.clone(), metadata_sm.deref(), data_sm.deref());
    let mut mapped = Vec::with_capacity(roots.len());
    for root in roots.values() {
        mapped.push(c.get_info(*root)?);
    }

    Ok(mapped)
}

fn some_counting_fields(fields: &[OutputField]) -> bool {
    use OutputField::*;

    for field in fields.iter() {
        match field {
            DeviceId | TransactionId | CreationTime | SnapshottedTime => {
                continue;
            }
            _ => {
                return true;
            }
        }
    }

    false
}

pub fn ls(opts: ThinLsOptions) -> Result<()> {
    let ctx = mk_context(&opts)?;

    let sb = if opts.use_metadata_snap {
        read_superblock_snap(ctx.engine.as_ref())?
    } else {
        read_superblock(ctx.engine.as_ref(), SUPERBLOCK_LOCATION)?
    };

    // ensure the metadata is consistent
    is_superblock_consistent(sb.clone(), ctx.engine.clone(), false)?;

    let mut path = vec![0];
    let details =
        btree_to_map::<DeviceDetail>(&mut path, ctx.engine.clone(), false, sb.details_root)?;

    let mut table = LsTable::new(&opts.fields, details.len(), sb.data_block_size);
    if !opts.no_headers {
        table.push_headers();
    }

    if some_counting_fields(&opts.fields) {
        let mapped = count_mapped_blocks(&ctx, &sb)?;
        for ((dev_id, detail), (actual_mapped, nr_shared)) in details.iter().zip(mapped) {
            table.push_row(*dev_id, detail, actual_mapped, nr_shared);
        }
    } else {
        for (dev_id, detail) in details.iter() {
            table.push_row(*dev_id, detail, 0, 0);
        }
    }

    table.render(&mut std::io::stdout())
}

//------------------------------------------
