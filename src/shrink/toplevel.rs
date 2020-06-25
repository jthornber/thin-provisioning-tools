use anyhow::Result;
use fixedbitset::{FixedBitSet, IndexRange};
use std::fs::OpenOptions;
use std::os::unix::fs::OpenOptionsExt;

use crate::shrink::xml;

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

    fn map(&mut self, m: xml::Map) -> Result<()> {
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
fn remap_ranges(ranges: Vec<BlockRange>, free: Vec<BlockRange>) -> Vec<(BlockRange, BlockRange)> {
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
            },
            Ordering::Equal => {
                remap.push((r, f));
                r_ = range_iter.next();
                f_ = free_iter.next();
            },
            Ordering::Greater => {
                remap.push((r.start..(r.start + flen), f));
                r_ = Some((r.start + flen)..r.end);
                f_ = free_iter.next();
            }
        }
    }

    remap
}

pub fn shrink(input_file: &str, _output_file: &str, nr_blocks: u64) -> Result<()> {
    let input = OpenOptions::new()
        .read(true)
        .write(false)
        .custom_flags(libc::O_EXCL)
        .open(input_file)?;

    // let mut visitor = xml::XmlWriter::new(std::io::stdout());
    // let mut visitor = xml::NoopVisitor::new();
    let mut pass1 = Pass1::new(nr_blocks);
    xml::read(input, &mut pass1)?;
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

    let remaps = remap_ranges(above, free);
    eprintln!("remappings {:?}.", remaps);

    Ok(())
}

//---------------------------------------
