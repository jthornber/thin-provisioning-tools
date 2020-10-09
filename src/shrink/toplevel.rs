use anyhow::{anyhow, Result};
use fixedbitset::FixedBitSet;
use std::fs::OpenOptions;
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

use crate::shrink::copier::{self, Region};
use crate::thin::xml::{self, Visit};

//---------------------------------------

#[derive(Debug)]
struct Pass1 {
    // FIXME: Inefficient, use a range_set of some description
    allocated_blocks: FixedBitSet,

    nr_blocks: u64,

    /// High blocks are beyond the new, reduced end of the pool.  These
    /// will need to be moved.
    nr_high_blocks: u64,
    block_size: Option<u64>,
}

impl Pass1 {
    fn new(nr_blocks: u64) -> Pass1 {
        Pass1 {
            allocated_blocks: FixedBitSet::with_capacity(0),
            nr_blocks,
            nr_high_blocks: 0,
            block_size: None,
        }
    }
}

impl xml::MetadataVisitor for Pass1 {
    fn superblock_b(&mut self, sb: &xml::Superblock) -> Result<Visit> {
        self.allocated_blocks.grow(sb.nr_data_blocks as usize);
        self.block_size = Some(sb.data_block_size as u64);
        Ok(Visit::Continue)
    }

    fn superblock_e(&mut self) -> Result<Visit> {
        Ok(Visit::Continue)
    }

    fn def_shared_b(&mut self, _name: &str) -> Result<Visit> {
        todo!();
    }

    fn def_shared_e(&mut self) -> Result<Visit> {
        todo!();
    }

    fn device_b(&mut self, _d: &xml::Device) -> Result<Visit> {
        Ok(Visit::Continue)
    }

    fn device_e(&mut self) -> Result<Visit> {
        Ok(Visit::Continue)
    }

    fn map(&mut self, m: &xml::Map) -> Result<Visit> {
        for i in m.data_begin..(m.data_begin + m.len) {
            if i > self.nr_blocks {
                self.nr_high_blocks += 1;
            }
            self.allocated_blocks.insert(i as usize);
        }
        Ok(Visit::Continue)
    }

    fn ref_shared(&mut self, _name: &str) -> Result<Visit> {
        todo!();
    }

    fn eof(&mut self) -> Result<Visit> {
        Ok(Visit::Continue)
    }
}

//---------------------------------------

// Writes remapped xml
struct Pass2<W: Write> {
    writer: xml::XmlWriter<W>,
    nr_blocks: u64,
    remaps: Vec<(BlockRange, BlockRange)>,
}

impl<W: Write> Pass2<W> {
    fn new(w: W, nr_blocks: u64, remaps: Vec<(BlockRange, BlockRange)>) -> Pass2<W> {
        Pass2 {
            writer: xml::XmlWriter::new(w),
            nr_blocks,
            remaps,
        }
    }
}

impl<W: Write> xml::MetadataVisitor for Pass2<W> {
    fn superblock_b(&mut self, sb: &xml::Superblock) -> Result<Visit> {
        self.writer.superblock_b(sb)
    }

    fn superblock_e(&mut self) -> Result<Visit> {
        self.writer.superblock_e()
    }

    fn def_shared_b(&mut self, _name: &str) -> Result<Visit> {
        todo!();
    }

    fn def_shared_e(&mut self) -> Result<Visit> {
        todo!();
    }

    fn device_b(&mut self, d: &xml::Device) -> Result<Visit> {
        self.writer.device_b(d)
    }

    fn device_e(&mut self) -> Result<Visit> {
        self.writer.device_e()
    }

    fn map(&mut self, m: &xml::Map) -> Result<Visit> {
        if m.data_begin + m.len < self.nr_blocks {
            // no remapping needed.
            self.writer.map(m)?;
        } else {
            let r = m.data_begin..(m.data_begin + m.len);
            let remaps = remap(&r, &self.remaps);
            let mut written = 0;

            for r in remaps {
                self.writer.map(&xml::Map {
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

    fn ref_shared(&mut self, _name: &str) -> Result<Visit> {
        todo!();
    }

    fn eof(&mut self) -> Result<Visit> {
        self.writer.eof()
    }
}

//---------------------------------------

type BlockRange = std::ops::Range<u64>;

fn bits_to_ranges(bits: &FixedBitSet) -> Vec<BlockRange> {
    let mut ranges = Vec::new();
    let mut start = None;

    for i in 0..bits.len() {
        match (bits[i], start) {
            (false, None) => {}
            (true, None) => {
                start = Some((i as u64, 1));
            }
            (false, Some((b, len))) => {
                ranges.push(b..(b + len));
                start = None;
            }
            (true, Some((b, len))) => {
                start = Some((b, len + 1));
            }
        }
    }

    if let Some((b, len)) = start {
        ranges.push(b..(b + len));
    }

    ranges
}

// Splits the ranges into those below threshold, and those equal or
// above threshold below threshold, and those equal or above threshold
fn ranges_split(ranges: &[BlockRange], threshold: u64) -> (Vec<BlockRange>, Vec<BlockRange>) {
    use std::ops::Range;

    let mut below = Vec::new();
    let mut above = Vec::new();
    for r in ranges {
        match r {
            Range { start, end } if *end <= threshold => below.push(*start..*end),
            Range { start, end } if *start < threshold => {
                below.push(*start..threshold);
                above.push(threshold..*end);
            }
            Range { start, end } => above.push(*start..*end),
        }
    }
    (below, above)
}

fn negate_ranges(ranges: &[BlockRange], upper_limit: u64) -> Vec<BlockRange> {
    use std::ops::Range;

    let mut result = Vec::new();
    let mut cursor = 0;

    for r in ranges {
        match r {
            Range { start, end } if cursor < *start => {
                result.push(cursor..*start);
                cursor = *end;
            }
            Range { start: _, end } => {
                cursor = *end;
            }
        }
    }

    if cursor < upper_limit {
        result.push(cursor..upper_limit);
    }

    result
}

fn range_len(r: &BlockRange) -> u64 {
    r.end - r.start
}

fn ranges_total(rs: &[BlockRange]) -> u64 {
    rs.iter().fold(0, |sum, r| sum + range_len(r))
}

// Assumes there is enough space to remap.
fn build_remaps(ranges: Vec<BlockRange>, free: Vec<BlockRange>) -> Vec<(BlockRange, BlockRange)> {
    use std::cmp::Ordering;

    let mut remap = Vec::new();
    let mut range_iter = ranges.into_iter();
    let mut free_iter = free.into_iter();

    let mut r_ = range_iter.next();
    let mut f_ = free_iter.next();

    while let (Some(r), Some(f)) = (r_, f_) {
        let rlen = range_len(&r);
        let flen = range_len(&f);

        match rlen.cmp(&flen) {
            Ordering::Less => {
                // range fits into the free chunk
                remap.push((r, f.start..(f.start + rlen)));
                f_ = Some((f.start + rlen)..f.end);
                r_ = range_iter.next();
            }
            Ordering::Equal => {
                remap.push((r, f));
                r_ = range_iter.next();
                f_ = free_iter.next();
            }
            Ordering::Greater => {
                remap.push((r.start..(r.start + flen), f));
                r_ = Some((r.start + flen)..r.end);
                f_ = free_iter.next();
            }
        }
    }

    remap
}

#[test]
fn test_build_remaps() {
    struct Test {
        ranges: Vec<BlockRange>,
        free: Vec<BlockRange>,
        result: Vec<(BlockRange, BlockRange)>,
    }

    let tests = vec![
        Test {
            ranges: vec![],
            free: vec![],
            result: vec![],
        },
        Test {
            ranges: vec![],
            free: vec![0..100],
            result: vec![],
        },
        Test {
            ranges: vec![1000..1002],
            free: vec![0..100],
            result: vec![(1000..1002, 0..2)],
        },
        Test {
            ranges: vec![1000..1002, 1100..1110],
            free: vec![0..100],
            result: vec![(1000..1002, 0..2), (1100..1110, 2..12)],
        },
        Test {
            ranges: vec![100..120],
            free: vec![0..5, 20..23, 30..50],
            result: vec![(100..105, 0..5), (105..108, 20..23), (108..120, 30..42)],
        },
    ];

    for t in tests {
        assert_eq!(build_remaps(t.ranges, t.free), t.result);
    }
}

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
fn find_first(r: &BlockRange, remaps: &[(BlockRange, BlockRange)]) -> Option<usize> {
    if remaps.is_empty() {
        return None;
    }

    match remaps.binary_search_by_key(&r.start, |(from, _)| from.start) {
        Ok(n) => Some(n),
        Err(n) => {
            if n == 0 {
                let (from, _) = &remaps[n];
                overlaps(&r, &from, n)
            } else if n == remaps.len() {
                let (from, _) = &remaps[n - 1];
                overlaps(&r, from, n - 1)
            } else {
                // Need to check the previous entry
                let (from, _) = &remaps[n - 1];
                overlaps(&r, &from, n - 1).or_else(|| {
                    let (from, _) = &remaps[n];
                    overlaps(&r, &from, n)
                })
            }
        }
    }
}

fn is_empty(r: &BlockRange) -> bool {
    r.start == r.end
}

// remaps must be in sorted order by from.start.
fn remap(r: &BlockRange, remaps: &[(BlockRange, BlockRange)]) -> Vec<BlockRange> {
    let mut remap = Vec::new();
    let mut r = r.start..r.end;

    if let Some(index) = find_first(&r, &remaps) {
        let mut index = index;
        loop {
            let (from, to) = &remaps[index];

            // There may be a prefix that doesn't overlap with 'from'
            if r.start < from.start {
                let len = u64::min(range_len(&r), from.start - r.start);
                remap.push(r.start..(r.start + len));
                r = (r.start + len)..r.end;

                if is_empty(&r) {
                    break;
                }
            }

            let to = (to.start + (r.start - from.start))..to.end;
            let from = r.start..from.end;
            let rlen = range_len(&r);
            let flen = range_len(&from);

            let len = u64::min(rlen, flen);
            remap.push(to.start..(to.start + len));

            r = (r.start + len)..r.end;
            if is_empty(&r) {
                break;
            }

            if len == flen {
                index += 1;
            }

            if index == remaps.len() {
                remap.push(r.start..r.end);
                break;
            }
        }
    } else {
        remap.push(r.start..r.end);
    }

    remap
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remap_test() {
        struct Test {
            remaps: Vec<(BlockRange, BlockRange)>,
            input: BlockRange,
            output: Vec<BlockRange>,
        }

        let tests = [
            Test {
                remaps: vec![],
                input: 0..1,
                output: vec![0..1],
            },
            Test {
                remaps: vec![],
                input: 100..1000,
                output: vec![100..1000],
            },
            Test {
                remaps: vec![(10..20, 110..120)],
                input: 0..5,
                output: vec![0..5],
            },
            Test {
                remaps: vec![(10..20, 110..120)],
                input: 10..20,
                output: vec![110..120],
            },
            Test {
                remaps: vec![(10..20, 110..120)],
                input: 5..15,
                output: vec![5..10, 110..115],
            },
            Test {
                remaps: vec![(10..20, 110..120)],
                input: 5..25,
                output: vec![5..10, 110..120, 20..25],
            },
            Test {
                remaps: vec![(10..20, 110..120)],
                input: 15..25,
                output: vec![115..120, 20..25],
            },
            Test {
                remaps: vec![(10..20, 110..120)],
                input: 25..35,
                output: vec![25..35],
            },
            Test {
                remaps: vec![(10..20, 110..120), (30..40, 230..240)],
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

fn build_copy_regions(remaps: &[(BlockRange, BlockRange)], block_size: u64) -> Vec<Region> {
    let mut rs = Vec::new();

    for (from, to) in remaps {
        rs.push(Region {
            src: from.start * block_size,
            dest: to.start * block_size,
            len: range_len(&from) * block_size,
        });
    }

    rs
}

fn process_xml<MV: xml::MetadataVisitor>(input_path: &Path, pass: &mut MV) -> Result<()> {
    let input = OpenOptions::new()
        .read(true)
        .write(false)
        .custom_flags(libc::O_EXCL)
        .open(input_path)?;

    xml::read(input, pass)?;
    Ok(())
}

pub fn shrink(
    input_path: &Path,
    output_path: &Path,
    data_path: &Path,
    nr_blocks: u64,
    do_copy: bool,
) -> Result<()> {
    let mut pass1 = Pass1::new(nr_blocks);
    eprint!("Reading xml...");
    process_xml(input_path, &mut pass1)?;
    eprintln!("done");
    eprintln!("{} blocks need moving", pass1.nr_high_blocks);

    let ranges = bits_to_ranges(&pass1.allocated_blocks);
    let (below, above) = ranges_split(&ranges, nr_blocks);

    let free = negate_ranges(&below, nr_blocks);
    let free_blocks = ranges_total(&free);
    eprintln!("{} free blocks.", free_blocks);

    if free_blocks < pass1.nr_high_blocks {
        return Err(anyhow!("Insufficient space"));
    }

    let remaps = build_remaps(above, free);

    if do_copy {
        let regions = build_copy_regions(&remaps, pass1.block_size.unwrap() as u64);
        copier::copy(data_path, &regions)?;
    } else {
        eprintln!("skipping copy");
    }

    let output = OpenOptions::new()
        .read(false)
        .write(true)
        .create(true)
        .open(output_path)?;
    let mut pass2 = Pass2::new(output, nr_blocks, remaps);
    eprint!("writing new xml...");
    process_xml(input_path, &mut pass2)?;
    eprintln!("done.");

    Ok(())
}

//---------------------------------------
