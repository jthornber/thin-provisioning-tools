use anyhow::{anyhow, Result};
use std::fs::OpenOptions;
use std::ops::Range;
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::sync::Arc;

use crate::file_utils::file_size;
use crate::io_engine::{AsyncIoEngine, Block, IoEngine, SyncIoEngine, SECTOR_SHIFT};
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

// defined in include/uapi/linux/fs.h
const BLK_IOC_CODE: u8 = 0x12;
const BLKDISCARD_SEQ: u8 = 119;
ioctl_write_ptr_bad!(
    ioctl_blkdiscard,
    nix::request_code_none!(BLK_IOC_CODE, BLKDISCARD_SEQ),
    [u64; 2]
);

// Read all the bitmaps at once
// There might be more than 64k bitmap blocks in a 16GB metadata, if the pool size
// exceeds the maximum number of mappings allowed.
fn read_bitmaps(engine: Arc<dyn IoEngine + Send + Sync>, bitmap_root: u64) -> Result<Vec<Block>> {
    let mut path = vec![0];
    let entries = btree_to_map::<IndexEntry>(&mut path, engine.clone(), false, bitmap_root)?;

    let blocknr = entries
        .iter()
        .map(|(_, ie)| ie.blocknr)
        .collect::<Vec<u64>>();
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

fn trim_data_device(
    engine: Arc<dyn IoEngine + Send + Sync>,
    sb: &Superblock,
    data_dev: &Path,
) -> Result<()> {
    let root = unpack::<SMRoot>(&sb.data_sm_root[..])?;
    let bs = (sb.data_block_size as u64) << SECTOR_SHIFT; // in bytes
    let expected = root.nr_blocks * bs;
    if expected > file_size(data_dev)? {
        return Err(anyhow!(
            "unexpected data device size, wanted {} bytes",
            expected
        ));
    }

    let bitmaps = read_bitmaps(engine, root.bitmap_root)?;

    let dev_file = OpenOptions::new().read(false).write(true).open(data_dev)?;
    let fd = dev_file.as_raw_fd();
    let mut last_seen = 0;
    for r in RangeIterator::new(&bitmaps[..], root.nr_blocks)? {
        match r {
            Ok(range) => {
                if range.start > last_seen {
                    let len = range.start - last_seen;
                    unsafe {
                        ioctl_blkdiscard(fd, &[last_seen * bs, len * bs])?;
                    }
                }
                last_seen = range.end;
            }
            Err(e) => return Err(anyhow!(format!("{}", e))),
        }
    }

    if root.nr_blocks > last_seen {
        let len = root.nr_blocks - last_seen;
        unsafe {
            ioctl_blkdiscard(fd, &[last_seen * bs, len * bs])?;
        }
    }

    Ok(())
}

//------------------------------------------

const MAX_CONCURRENT_IO: u32 = 1024;

pub struct ThinTrimOptions<'a> {
    pub metadata_dev: &'a Path,
    pub data_dev: &'a Path,
    pub async_io: bool,
    pub report: Arc<Report>,
}

struct Context {
    _report: Arc<Report>,
    engine: Arc<dyn IoEngine + Send + Sync>,
}

fn mk_context(opts: &ThinTrimOptions) -> Result<Context> {
    let engine: Arc<dyn IoEngine + Send + Sync> = if opts.async_io {
        Arc::new(AsyncIoEngine::new(
            opts.metadata_dev,
            MAX_CONCURRENT_IO,
            false,
        )?)
    } else {
        let nr_threads = std::cmp::max(8, num_cpus::get() * 2);
        Arc::new(SyncIoEngine::new(opts.metadata_dev, nr_threads, false)?)
    };

    Ok(Context {
        _report: opts.report.clone(),
        engine,
    })
}

//------------------------------------------

pub fn trim(opts: ThinTrimOptions) -> Result<()> {
    let ctx = mk_context(&opts)?;
    let sb = read_superblock(ctx.engine.as_ref(), SUPERBLOCK_LOCATION)?;

    trim_data_device(ctx.engine, &sb, opts.data_dev)
}

//------------------------------------------
