use anyhow::Result;
use fixedbitset::{FixedBitSet, IndexRange};
use std::fs::OpenOptions;
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;

use crate::shrink::xml;
use crate::shrink::copier::{self, Region};

//---------------------------------------

#[derive(Debug)]
struct Pass1 {
    // FIXME: Inefficient, use a range_set of some description
    allocated_blocks: FixedBitSet,

    nr_blocks: u64,

    /// High blocks are beyond the new, reduced end of the pool.  These
    /// will need to be moved.
    nr_high_blocks: u64,
}

impl Pass1 {
    fn new(nr_blocks: u64) -> Pass1 {
        Pass1 {
            allocated_blocks: FixedBitSet::with_capacity(0),
            nr_blocks,
            nr_high_blocks: 0,
        }
    }
}

impl xml::MetadataVisitor for Pass1 {
    fn superblock_b(&mut self, sb: &xml::Superblock) -> Result<()> {
        self.allocated_blocks.grow(sb.nr_data_blocks as usize);
        Ok(())
    }

    fn superblock_e(&mut self) -> Result<()> {
        Ok(())
    }

    fn device_b(&mut self, _d: &xml::Device) -> Result<()> {
        Ok(())
    }

    fn device_e(&mut self) -> Result<()> {
        Ok(())
    }

    fn map(&mut self, m: &xml::Map) -> Result<()> {
        for i in m.data_begin..(m.data_begin + m.len) {
            if i > self.nr_blocks {
                self.nr_high_blocks += 1;
            }
            self.allocated_blocks.insert(i as usize);
        }
        Ok(())
    }

    fn eof(&mut self) -> Result<()> {
        Ok(())
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

    fn remap(&self, r: BlockRange) -> Vec<BlockRange> {
        let mut rmap = Vec::new();

        // id
        rmap.push(r.clone());

        rmap
    }
}

impl<W: Write> xml::MetadataVisitor for Pass2<W> {
    fn superblock_b(&mut self, sb: &xml::Superblock) -> Result<()> {
        self.writer.superblock_b(sb)
    }

    fn superblock_e(&mut self) -> Result<()> {
        self.writer.superblock_e()
    }

    fn device_b(&mut self, d: &xml::Device) -> Result<()> {
        self.writer.device_b(d)
    }

    fn device_e(&mut self) -> Result<()> {
        self.writer.device_e()
    }

    fn map(&mut self, m: &xml::Map) -> Result<()> {
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

        Ok(())
    }

    fn eof(&mut self) -> Result<()> {
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
fn ranges_split(ranges: &Vec<BlockRange>, threshold: u64) -> (Vec<BlockRange>, Vec<BlockRange>) {
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

fn negate_ranges(ranges: &Vec<BlockRange>) -> Vec<BlockRange> {
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

    result
}

fn range_len(r: &BlockRange) -> u64 {
    r.end - r.start
}

fn ranges_total(rs: &Vec<BlockRange>) -> u64 {
    rs.into_iter().fold(0, |sum, r| sum + range_len(r))
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
fn find_first(r: &BlockRange, remaps: &Vec<(BlockRange, BlockRange)>) -> Option<usize> {
    if remaps.len() == 0 {
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
                    let (from, to) = &remaps[n];
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
fn remap(r: &BlockRange, remaps: &Vec<(BlockRange, BlockRange)>) -> Vec<BlockRange> {
    let mut remap = Vec::new();
    let mut r = r.start..r.end;

    if let Some(index) = find_first(&r, &remaps) {
        let mut index = index;
        loop {
            let (from, to) = &remaps[index];
            println!("from = {:?}", from);

            // There may be a prefix that doesn't overlap with 'from'
            if r.start < from.start {
                println!("pushing prefix");
                let len = u64::min(range_len(&r), from.start - r.start);
                remap.push(r.start..(r.start + len));
                r = (r.start + len)..r.end;

                if is_empty(&r) {
                    break;
                }
            }

            let to = (to.start + (r.start - from.start))..to.end;
            let from = r.start..from.end;
            println!("to = {:?}", to);
            let rlen = range_len(&r);
            let flen = range_len(&from);

            let len = u64::min(rlen, flen);
            println!("pushing overlap");
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

fn build_copy_regions(remaps: &Vec<(BlockRange, BlockRange)>) -> Vec<Region> {
    let rs = Vec::new();
    rs
}

fn process_xml<MV: xml::MetadataVisitor>(input_path: &str, pass: &mut MV) -> Result<()> {
    let input = OpenOptions::new()
        .read(true)
        .write(false)
        .custom_flags(libc::O_EXCL)
        .open(input_path)?;

    xml::read(input, pass)?;
    Ok(())
}

pub fn shrink(input_path: &str, output_path: &str, data_path: &str, nr_blocks: u64) -> Result<()> {
    let mut pass1 = Pass1::new(nr_blocks);
    process_xml(input_path, &mut pass1);
    eprintln!("{} blocks need moving", pass1.nr_high_blocks);

    let mut free_blocks = 0u64;
    for i in 0..pass1.allocated_blocks.len() {
        if !pass1.allocated_blocks[i] {
            free_blocks += 1;
        }
    }
    eprintln!("{} free blocks below new end.", free_blocks);

    let ranges = bits_to_ranges(&pass1.allocated_blocks);
    eprintln!("{} allocated ranges:", ranges.len());

    eprintln!("{:?}", &ranges);

    let (below, above) = ranges_split(&ranges, nr_blocks);
    eprintln!("ranges split at {}: ({:?}, {:?})", nr_blocks, below, above);

    let free = negate_ranges(&below);
    eprintln!("free {:?}.", free);

    let nr_moving = ranges_total(&above);
    eprintln!("{} blocks need to be remapped.", nr_moving);

    let free_blocks = ranges_total(&free);
    eprintln!("{} free blocks.", free_blocks);

    if free_blocks < nr_moving {
        panic!("Insufficient space");
    }

    let remaps = build_remaps(above, free);
    eprintln!("remappings {:?}.", remaps);

    let regions = build_copy_regions(&remaps);
    eprint!("Copying data...");
    copier::copy(data_path, &regions);
    eprintln!("done.");

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
