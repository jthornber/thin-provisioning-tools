use anyhow::{anyhow, Result};
use std::io::Write;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use crate::grid_layout::GridLayout;
use crate::io_engine::SECTOR_SHIFT;
use crate::io_engine::{AsyncIoEngine, IoEngine, SyncIoEngine};
use crate::pdata::btree_walker::btree_to_map;
use crate::report::Report;
use crate::thin::device_detail::DeviceDetail;
use crate::thin::metadata_repair::is_superblock_consistent;
use crate::thin::superblock::{read_superblock, Superblock, SUPERBLOCK_LOCATION};
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

    fn push_row(&mut self, dev_id: u64, detail: &DeviceDetail, shared_blocks: u64) {
        use OutputField::*;

        if self.fields.is_empty() {
            return;
        }

        let bs = self.data_block_size;
        let ex_blocks = detail.mapped_blocks - shared_blocks;

        for field in self.fields {
            let val: u64 = match field {
                DeviceId => dev_id,
                TransactionId => detail.transaction_id,
                CreationTime => detail.creation_time as u64,
                SnapshottedTime => detail.snapshotted_time as u64,
                MappedBlocks => detail.mapped_blocks,
                MappedSectors => detail.mapped_blocks * bs,
                MappedBytes | Mapped => (detail.mapped_blocks * bs) << SECTOR_SHIFT as u64,
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
    _report: Arc<Report>, // TODO: report the scanning progress
}

fn mk_context(opts: &ThinLsOptions) -> Result<Context> {
    let engine: Arc<dyn IoEngine + Send + Sync>;

    if opts.async_io {
        engine = Arc::new(AsyncIoEngine::new(opts.input, MAX_CONCURRENT_IO, false)?);
    } else {
        let nr_threads = std::cmp::max(8, num_cpus::get() * 2);
        engine = Arc::new(SyncIoEngine::new(opts.input, nr_threads, false)?);
    }

    Ok(Context {
        engine,
        _report: opts.report.clone(),
    })
}

fn count_shared_blocks(
    engine: Arc<dyn IoEngine + Send + Sync>,
    sb: &Superblock,
) -> Result<Vec<u64>> {
    let mut path = vec![0];
    let roots = btree_to_map::<u64>(&mut path, engine.clone(), false, sb.mapping_root)?;

    // TODO
    Ok(vec![0; roots.len()])
}

pub fn ls(opts: ThinLsOptions) -> Result<()> {
    let ctx = mk_context(&opts)?;

    let mut sb = read_superblock(ctx.engine.as_ref(), SUPERBLOCK_LOCATION)?;
    if opts.use_metadata_snap {
        sb = read_superblock(ctx.engine.as_ref(), sb.metadata_snap)?;
    }

    // ensure the metadata is consistent
    is_superblock_consistent(sb.clone(), ctx.engine.clone())?;

    let mut path = vec![0];
    let details =
        btree_to_map::<DeviceDetail>(&mut path, ctx.engine.clone(), false, sb.details_root)?;
    let shared = count_shared_blocks(ctx.engine.clone(), &sb)?;

    let mut table = LsTable::new(&opts.fields, details.len(), sb.data_block_size);
    if !opts.no_headers {
        table.push_headers();
    }

    for ((dev_id, detail), nr_shared) in details.iter().zip(shared) {
        table.push_row(*dev_id, detail, nr_shared);
    }

    table.render(&mut std::io::stdout())
}

//------------------------------------------
