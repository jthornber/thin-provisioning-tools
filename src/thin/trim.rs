use anyhow::{anyhow, Result};
use std::fs::OpenOptions;
use std::ops::Range;
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::sync::Arc;

use crate::commands::engine::*;
use crate::file_utils::file_size;
use crate::io_engine::*;
use crate::pdata::btree_walker::*;
use crate::pdata::space_map::common::*;
use crate::pdata::unpack::unpack;
use crate::report::Report;
use crate::thin::superblock::{read_superblock, Superblock, SUPERBLOCK_LOCATION};

//------------------------------------------

struct RangeIterator<'a> {
    bitmaps: &'a [Block],
    nr_blocks: u64,
    ie_index: u64,
    bitmap: Bitmap,
    current: u64,
}

impl<'a> RangeIterator<'a> {
    fn new(bitmaps: &'a [Block], nr_blocks: u64) -> Result<RangeIterator> {
        if bitmaps.is_empty() || nr_blocks > bitmaps.len() as u64 * ENTRIES_PER_BITMAP as u64 {
            return Err(anyhow!("invalid parameter"));
        }

        let bitmap = unpack::<Bitmap>(bitmaps[0].get_data())?;
        Ok(RangeIterator {
            bitmaps,
            nr_blocks,
            ie_index: 0,
            bitmap,
            current: 0,
        })
    }
}

fn find_first_set(entries: &[BitmapEntry]) -> usize {
    let mut i = 0;
    for entry in entries {
        if !matches!(entry, BitmapEntry::Small(0)) {
            return i;
        }
        i += 1;
    }
    i
}

fn find_first_unset(entries: &[BitmapEntry]) -> usize {
    let mut i = 0;
    for entry in entries {
        if matches!(entry, BitmapEntry::Small(0)) {
            return i;
        }
        i += 1;
    }
    i
}

impl<'a> Iterator for RangeIterator<'a> {
    type Item = std::io::Result<std::ops::Range<u64>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.nr_blocks {
            return None;
        }

        // skip unused entries
        loop {
            let offset = self.current % ENTRIES_PER_BITMAP as u64;
            let zeros = find_first_set(&self.bitmap.entries[offset as usize..]);
            self.current = std::cmp::min(self.current + zeros as u64, self.nr_blocks);

            if self.current == self.nr_blocks {
                return None;
            }

            let ie = self.current / ENTRIES_PER_BITMAP as u64;
            if ie != self.ie_index {
                match unpack::<Bitmap>(self.bitmaps[ie as usize].get_data()) {
                    Ok(v) => self.bitmap = v,
                    Err(e) => {
                        self.current = self.nr_blocks;
                        return Some(Err(e));
                    }
                }
                self.ie_index = ie;
            } else {
                break;
            }
        }

        let start = self.current;

        // skip used entries
        loop {
            let offset = self.current % ENTRIES_PER_BITMAP as u64;
            let ones = find_first_unset(&self.bitmap.entries[offset as usize..]);
            self.current = std::cmp::min(self.current + ones as u64, self.nr_blocks);

            if self.current == self.nr_blocks {
                break;
            }

            let ie = self.current / ENTRIES_PER_BITMAP as u64;
            if ie != self.ie_index {
                match unpack::<Bitmap>(self.bitmaps[ie as usize].get_data()) {
                    Ok(v) => self.bitmap = v,
                    Err(e) => {
                        self.current = self.nr_blocks;
                        return Some(Err(e));
                    }
                }
                self.ie_index = ie;
            } else {
                break;
            }
        }

        Some(Ok(Range {
            start,
            end: self.current,
        }))
    }
}

//------------------------------------------

const BLKDISCARD: u32 = 0x1277;
fn ioctl_blkdiscard(fd: i32, range: &[u64; 2]) -> std::io::Result<()> {
    #[cfg(target_env = "musl")]
    type RequestType = libc::c_int;
    #[cfg(not(target_env = "musl"))]
    type RequestType = libc::c_ulong;

    unsafe {
        if libc::ioctl(fd, BLKDISCARD as RequestType, range) == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }
}

// Read all the bitmaps at once
// There might be more than 64k bitmap blocks in a 16GB metadata, if the pool size
// exceeds the maximum number of mappings allowed.
fn read_bitmaps(engine: Arc<dyn IoEngine + Send + Sync>, bitmap_root: u64) -> Result<Vec<Block>> {
    let mut path = vec![0];
    let entries = btree_to_map::<IndexEntry>(&mut path, engine.clone(), false, bitmap_root)?;

    let blocknr = entries.values().map(|ie| ie.blocknr).collect::<Vec<u64>>();
    let blocks = engine.read_many(&blocknr[..])?;
    let mut bitmaps = Vec::new();
    for b in blocks {
        match b {
            Ok(bm) => bitmaps.push(bm),
            Err(e) => return Err(anyhow!(format!("{}", e))),
        }
    }

    Ok(bitmaps)
}

fn trim_data_device(ctx: &Context, sb: &Superblock, data_dev: &Path) -> Result<()> {
    let root = unpack::<SMRoot>(&sb.data_sm_root[..])?;
    let bs = (sb.data_block_size as u64) << SECTOR_SHIFT; // in bytes
    let expected = root.nr_blocks * bs;
    if expected > file_size(data_dev)? {
        return Err(anyhow!(
            "unexpected data device size, wanted {} bytes",
            expected
        ));
    }

    let bitmaps = read_bitmaps(ctx.engine.clone(), root.bitmap_root)?;

    let dev_file = OpenOptions::new().read(false).write(true).open(data_dev)?;
    let fd = dev_file.as_raw_fd();
    let mut last_seen = 0;
    for r in RangeIterator::new(&bitmaps[..], root.nr_blocks)? {
        match r {
            Ok(range) => {
                if range.start > last_seen {
                    let len = range.start - last_seen;
                    ctx.report.debug(&format!(
                        "emitting discard for blocks [{}, {}]",
                        last_seen,
                        last_seen + len - 1
                    ));
                    ioctl_blkdiscard(fd, &[last_seen * bs, len * bs])?;
                }
                last_seen = range.end;
            }
            Err(e) => return Err(anyhow!(format!("{}", e))),
        }
    }

    if root.nr_blocks > last_seen {
        let len = root.nr_blocks - last_seen;
        ioctl_blkdiscard(fd, &[last_seen * bs, len * bs])?;
    }

    Ok(())
}

//------------------------------------------

pub struct ThinTrimOptions<'a> {
    pub metadata_dev: &'a Path,
    pub data_dev: &'a Path,
    pub engine_opts: EngineOptions,
    pub report: Arc<Report>,
}

struct Context {
    report: Arc<Report>,
    engine: Arc<dyn IoEngine + Send + Sync>,
}

fn mk_context(opts: &ThinTrimOptions) -> Result<Context> {
    let engine = EngineBuilder::new(opts.metadata_dev, &opts.engine_opts).build()?;
    Ok(Context {
        report: opts.report.clone(),
        engine,
    })
}

//------------------------------------------

pub fn trim(opts: ThinTrimOptions) -> Result<()> {
    let ctx = mk_context(&opts)?;
    let sb = read_superblock(ctx.engine.as_ref(), SUPERBLOCK_LOCATION)?;

    trim_data_device(&ctx, &sb, opts.data_dev)
}

//------------------------------------------
