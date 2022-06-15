use anyhow::{anyhow, Result};

use rangemap::RangeSet;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{BufWriter, Read, SeekFrom};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::io_engine::{IoEngine, SyncIoEngine, SECTOR_SHIFT};
use crate::pdata::space_map_metadata::core_metadata_sm;
use crate::report::Report;
use crate::sync_copier::SyncCopier;
use crate::thin::dump::dump_metadata;
use crate::thin::ir::{self, MetadataVisitor, Visit};
use crate::thin::metadata::*;
use crate::thin::restore::Restorer;
use crate::thin::superblock::{read_superblock, Superblock, SUPERBLOCK_LOCATION};
use crate::thin::xml;
use crate::write_batcher::WriteBatcher;

//---------------------------------------

type BlockRange = std::ops::Range<u64>;

fn range_len(r: &BlockRange) -> u64 {
    r.end - r.start
}

fn is_empty(r: &BlockRange) -> bool {
    r.start == r.end
}

//---------------------------------------

fn build_remaps<T1, T2>(ranges: T1, free: T2) -> Result<Vec<(BlockRange, u64)>>
where
    T1: IntoIterator<Item = BlockRange>,
    T2: IntoIterator<Item = BlockRange>,
{
    use std::cmp::Ordering;

    let mut remaps = Vec::new();
    let mut range_iter = ranges.into_iter();
    let mut free_iter = free.into_iter();

    let mut r = range_iter.next().unwrap_or(u64::MAX..u64::MAX);
    let mut f = free_iter.next().unwrap_or(u64::MAX..u64::MAX);

    while !r.is_empty() && !f.is_empty() {
        let rlen = range_len(&r);
        let flen = range_len(&f);

        match rlen.cmp(&flen) {
            Ordering::Less => {
                // range fits into the free chunk
                remaps.push((r, f.start));
                f.start += rlen;
                r = range_iter.next().unwrap_or(u64::MAX..u64::MAX);
            }
            Ordering::Equal => {
                remaps.push((r, f.start));
                r = range_iter.next().unwrap_or(u64::MAX..u64::MAX);
                f = free_iter.next().unwrap_or(u64::MAX..u64::MAX);
            }
            Ordering::Greater => {
                remaps.push((r.start..(r.start + flen), f.start));
                r.start += flen;
                f = free_iter.next().unwrap_or(u64::MAX..u64::MAX);
            }
        }
    }

    if r.is_empty() {
        Ok(remaps)
    } else {
        Err(anyhow!("Insufficient free space"))
    }
}

#[test]
fn test_build_remaps() {
    struct Test {
        ranges: Vec<BlockRange>,
        free: Vec<BlockRange>,
        result: Vec<(BlockRange, u64)>,
    }

    let tests = vec![
        // noop
        Test {
            ranges: vec![],
            free: vec![],
            result: vec![],
        },
        // no inputs
        Test {
            ranges: vec![],
            free: vec![0..100],
            result: vec![],
        },
        // fit a single range
        Test {
            ranges: vec![1000..1002],
            free: vec![0..100],
            result: vec![(1000..1002, 0)],
        },
        // fit multiple ranges
        Test {
            ranges: vec![1000..1002, 1100..1110],
            free: vec![0..100],
            result: vec![(1000..1002, 0), (1100..1110, 2)],
        },
        // split an input range
        Test {
            ranges: vec![100..120],
            free: vec![0..5, 20..23, 30..50],
            result: vec![(100..105, 0), (105..108, 20), (108..120, 30)],
        },
    ];

    for t in tests {
        assert_eq!(build_remaps(t.ranges, t.free).unwrap(), t.result);
    }
}

//---------------------------------------

fn overlaps(r1: &BlockRange, r2: &BlockRange, index: usize) -> Option<usize> {
    if r1.start >= r2.end {
        return None;
    }

    if r2.start >= r1.end {
        return None;
    }

    Some(index)
}

// Finds the index of the first entry that overlaps r.
// TODO: return the found remapping to save further accesses.
fn find_first(r: &BlockRange, remaps: &[(BlockRange, u64)]) -> Option<usize> {
    if remaps.is_empty() {
        return None;
    }

    match remaps.binary_search_by_key(&r.start, |(from, _)| from.start) {
        Ok(n) => Some(n),
        Err(n) => {
            if n == 0 {
                let (from, _) = &remaps[n];
                overlaps(r, from, n)
            } else if n == remaps.len() {
                let (from, _) = &remaps[n - 1];
                overlaps(r, from, n - 1)
            } else {
                // Need to check the previous entry
                let (from, _) = &remaps[n - 1];
                overlaps(r, from, n - 1).or_else(|| {
                    let (from, _) = &remaps[n];
                    overlaps(r, from, n)
                })
            }
        }
    }
}

// remaps must be in sorted order by from.start.
fn remap(r: &BlockRange, remaps: &[(BlockRange, u64)]) -> Vec<BlockRange> {
    let mut mapped = Vec::new();
    let mut r = r.start..r.end;

    if let Some(index) = find_first(&r, remaps) {
        let mut index = index;
        loop {
            let (from, to) = &remaps[index];

            // There may be a prefix that doesn't overlap with 'from'
            if r.start < from.start {
                let len = u64::min(range_len(&r), from.start - r.start);
                mapped.push(r.start..(r.start + len));
                r = (r.start + len)..r.end;

                if is_empty(&r) {
                    break;
                }
            }

            let remap_start = to + (r.start - from.start);
            let from = r.start..from.end;
            let rlen = range_len(&r);
            let flen = range_len(&from);

            let len = u64::min(rlen, flen);
            mapped.push(remap_start..(remap_start + len));

            r = (r.start + len)..r.end;
            if is_empty(&r) {
                break;
            }

            if len == flen {
                index += 1;
            }

            if index == remaps.len() {
                mapped.push(r.start..r.end);
                break;
            }
        }
    } else {
        mapped.push(r.start..r.end);
    }

    mapped
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remap_test() {
        struct Test {
            remaps: Vec<(BlockRange, u64)>,
            input: BlockRange,
            output: Vec<BlockRange>,
        }

        let tests = [
            // no remaps
            Test {
                remaps: vec![],
                input: 0..1,
                output: vec![0..1],
            },
            // no remaps
            Test {
                remaps: vec![],
                input: 100..1000,
                output: vec![100..1000],
            },
            // preceeding to remaps
            Test {
                remaps: vec![(10..20, 110)],
                input: 0..5,
                output: vec![0..5],
            },
            // fully overlapped to a remap
            Test {
                remaps: vec![(10..20, 110)],
                input: 10..20,
                output: vec![110..120],
            },
            // tail overlapped with a remap
            Test {
                remaps: vec![(10..20, 110)],
                input: 5..15,
                output: vec![5..10, 110..115],
            },
            // stride across a remap
            Test {
                remaps: vec![(10..20, 110)],
                input: 5..25,
                output: vec![5..10, 110..120, 20..25],
            },
            // head overlapped with a remap
            Test {
                remaps: vec![(10..20, 110)],
                input: 15..25,
                output: vec![115..120, 20..25],
            },
            // succeeding to a remap
            Test {
                remaps: vec![(10..20, 110)],
                input: 25..35,
                output: vec![25..35],
            },
            // stride across multiple remaps
            Test {
                remaps: vec![(10..20, 110), (30..40, 230)],
                input: 0..50,
                output: vec![0..10, 110..120, 20..30, 230..240, 40..50],
            },
        ];

        for t in &tests {
            let rs = remap(&t.input, &t.remaps);
            assert_eq!(rs, t.output);
        }
    }
}

fn copy_regions(
    data_dev: &Path,
    remaps: &[(BlockRange, u64)],
    block_size: u64,
) -> std::io::Result<()> {
    let copier = SyncCopier::in_file(data_dev)?;

    for (from, to) in remaps {
        let src = from.start * block_size;
        let dest = to * block_size;
        let len = range_len(from) * block_size;
        copier.copy(src, dest, len)?;
    }

    Ok(())
}

//---------------------------------------

struct MappingCollector {
    nr_blocks: u64,
    below: RangeSet<u64>,
    above: RangeSet<u64>,
}

impl MappingCollector {
    fn new(nr_blocks: u64) -> MappingCollector {
        MappingCollector {
            nr_blocks,
            below: RangeSet::new(),
            above: RangeSet::new(),
        }
    }

    fn get_remaps(self) -> Result<Vec<(BlockRange, u64)>> {
        let new_range = 0..self.nr_blocks;
        let free = self.below.gaps(&new_range);
        build_remaps(self.above, free)
    }
}

impl MetadataVisitor for MappingCollector {
    fn superblock_b(&mut self, _sb: &ir::Superblock) -> Result<Visit> {
        Ok(Visit::Continue)
    }

    fn superblock_e(&mut self) -> Result<Visit> {
        Ok(Visit::Continue)
    }

    fn def_shared_b(&mut self, _name: &str) -> Result<Visit> {
        Ok(Visit::Continue)
    }

    fn def_shared_e(&mut self) -> Result<Visit> {
        Ok(Visit::Continue)
    }

    fn device_b(&mut self, _d: &ir::Device) -> Result<Visit> {
        Ok(Visit::Continue)
    }

    fn device_e(&mut self) -> Result<Visit> {
        Ok(Visit::Continue)
    }

    fn map(&mut self, m: &ir::Map) -> Result<Visit> {
        if m.data_begin >= self.nr_blocks {
            self.above.insert(std::ops::Range {
                start: m.data_begin,
                end: m.data_begin + m.len,
            });
        } else if m.data_begin + m.len <= self.nr_blocks {
            self.below.insert(std::ops::Range {
                start: m.data_begin,
                end: m.data_begin + m.len,
            });
        } else {
            self.below.insert(std::ops::Range {
                start: m.data_begin,
                end: self.nr_blocks,
            });
            self.above.insert(std::ops::Range {
                start: self.nr_blocks,
                end: m.data_begin + m.len,
            });
        }
        Ok(Visit::Continue)
    }

    fn ref_shared(&mut self, _name: &str) -> Result<Visit> {
        Ok(Visit::Continue)
    }

    fn eof(&mut self) -> Result<Visit> {
        Ok(Visit::Continue)
    }
}

//---------------------------------------

struct DataRemapper<'a> {
    writer: &'a mut dyn MetadataVisitor,
    nr_blocks: u64,
    remaps: Vec<(BlockRange, u64)>,
}

impl<'a> DataRemapper<'a> {
    fn new(
        writer: &'a mut dyn MetadataVisitor,
        nr_blocks: u64,
        remaps: Vec<(BlockRange, u64)>,
    ) -> DataRemapper<'a> {
        DataRemapper {
            writer,
            nr_blocks,
            remaps,
        }
    }
}

impl<'a> MetadataVisitor for DataRemapper<'a> {
    fn superblock_b(&mut self, sb: &ir::Superblock) -> Result<Visit> {
        self.writer.superblock_b(sb)
    }

    fn superblock_e(&mut self) -> Result<Visit> {
        self.writer.superblock_e()
    }

    fn def_shared_b(&mut self, name: &str) -> Result<Visit> {
        self.writer.def_shared_b(name)
    }

    fn def_shared_e(&mut self) -> Result<Visit> {
        self.writer.def_shared_e()
    }

    fn device_b(&mut self, d: &ir::Device) -> Result<Visit> {
        self.writer.device_b(d)
    }

    fn device_e(&mut self) -> Result<Visit> {
        self.writer.device_e()
    }

    fn map(&mut self, m: &ir::Map) -> Result<Visit> {
        if m.data_begin + m.len < self.nr_blocks {
            // no remapping needed.
            self.writer.map(m)?;
        } else {
            let r = m.data_begin..(m.data_begin + m.len);
            let remaps = remap(&r, &self.remaps);
            let mut written = 0;

            for r in remaps {
                self.writer.map(&ir::Map {
                    thin_begin: m.thin_begin + written,
                    data_begin: r.start,
                    time: m.time,
                    len: range_len(&r),
                })?;
                written += range_len(&r);
            }
        }

        Ok(Visit::Continue)
    }

    fn ref_shared(&mut self, name: &str) -> Result<Visit> {
        self.writer.ref_shared(name)
    }

    fn eof(&mut self) -> Result<Visit> {
        self.writer.eof()
    }
}

//---------------------------------------

fn build_remaps_from_metadata(
    engine: Arc<dyn IoEngine>,
    sb: &Superblock,
    md: &Metadata,
    nr_blocks: u64,
) -> Result<Vec<(BlockRange, u64)>> {
    let mut collector = MappingCollector::new(nr_blocks);
    dump_metadata(engine, &mut collector, sb, md)?;
    collector.get_remaps()
}

fn build_remaps_from_xml<R: Read>(input: R, nr_blocks: u64) -> Result<Vec<(BlockRange, u64)>> {
    let mut collector = MappingCollector::new(nr_blocks);
    xml::read(input, &mut collector)?;
    collector.get_remaps()
}

pub struct ThinShrinkOptions {
    pub input: PathBuf,
    pub output: PathBuf,
    pub data_device: PathBuf,
    pub nr_blocks: u64,
    pub do_copy: bool,
    pub binary_mode: bool,
    pub report: Arc<Report>,
}

fn rewrite_xml(opts: ThinShrinkOptions) -> Result<()> {
    use std::io::Seek;

    // 1st pass
    let mut input = OpenOptions::new()
        .read(true)
        .write(false)
        .custom_flags(libc::O_EXCL)
        .open(&opts.input)?;
    let sb = xml::read_superblock(input.try_clone()?)?;
    input.seek(SeekFrom::Start(0))?;
    let remaps = build_remaps_from_xml(input.try_clone()?, opts.nr_blocks)?;

    if opts.do_copy {
        let bs = (sb.data_block_size as u64) << SECTOR_SHIFT as u64;
        copy_regions(&opts.data_device, &remaps, bs)?;
    }

    // 2nd pass
    let writer = BufWriter::new(File::create(&opts.output)?);
    let mut xml_writer = xml::XmlWriter::new(writer);
    let mut remapper = DataRemapper::new(&mut xml_writer, opts.nr_blocks, remaps);
    input.seek(SeekFrom::Start(0))?;
    xml::read(input, &mut remapper)
}

fn rebuild_metadata(opts: ThinShrinkOptions) -> Result<()> {
    let input = Arc::new(SyncIoEngine::new(&opts.input, 1, false)?);
    let sb = read_superblock(input.as_ref(), SUPERBLOCK_LOCATION)?;
    let md = build_metadata(input.clone(), &sb)?;
    let md = optimise_metadata(md)?;

    // 1st pass
    let remaps = build_remaps_from_metadata(input.clone(), &sb, &md, opts.nr_blocks)?;

    if opts.do_copy {
        let bs = (sb.data_block_size as u64) << SECTOR_SHIFT as u64;
        copy_regions(&opts.data_device, &remaps, bs)?;
    }

    // 2nd pass
    let output = Arc::new(SyncIoEngine::new(&opts.output, 1, true)?);
    let sm = core_metadata_sm(output.get_nr_blocks(), u32::MAX);
    let mut w = WriteBatcher::new(output.clone(), sm, output.get_batch_size());
    let mut restorer = Restorer::new(&mut w, opts.report);
    let mut remapper = DataRemapper::new(&mut restorer, opts.nr_blocks, remaps);
    dump_metadata(input, &mut remapper, &sb, &md)
}

pub fn shrink(opts: ThinShrinkOptions) -> Result<()> {
    if opts.binary_mode {
        rebuild_metadata(opts)
    } else {
        rewrite_xml(opts)
    }
}

//---------------------------------------
