use super::*;
use rand::prelude::*;
use rand::Rng;
use std::collections::BTreeMap;
use std::ops::Range;
use std::os::unix::fs::FileExt;

use crate::copier::test_utils::*;
use crate::io_engine::base::PAGE_SIZE;
use crate::io_engine::ramdisk::Ramdisk;
use crate::math::div_up;
use crate::random::Generator;

//------------------------------------------

const SRC_SALT: u64 = 0x6b8b16c70a82b5b5;
const BUFFER_SIZE: usize = 16777216; // 16 MiB
const BLOCK_SIZE: u32 = 32768; // 32 KiB

fn mk_ops<T1: IntoIterator<Item = u64>, T2: IntoIterator<Item = u64>>(
    src_seq: T1,
    dst_seq: T2,
) -> Vec<CopyOp> {
    src_seq
        .into_iter()
        .zip(dst_seq)
        .map(|(src, dst)| CopyOp { src, dst })
        .collect()
}

fn mk_random_ops(src: Range<u64>, dst: Range<u64>, nr_ops: usize) -> Vec<CopyOp> {
    let mut src_blocks: Vec<u64> = src.into_iter().collect();
    let mut dst_blocks: Vec<u64> = dst.into_iter().collect();

    src_blocks.shuffle(&mut rand::thread_rng());
    dst_blocks.shuffle(&mut rand::thread_rng());

    src_blocks
        .into_iter()
        .take(nr_ops)
        .zip(dst_blocks)
        .map(|(src, dst)| CopyOp { src, dst })
        .collect()
}

//------------------------------------------

trait SourceBlockIndicator {
    fn source(&self, block: u64) -> Option<u64>;
}

struct CopySourceIndicator {
    rmap: BTreeMap<u64, u64>,
}

impl CopySourceIndicator {
    fn new(rmap: BTreeMap<u64, u64>) -> Self {
        Self { rmap }
    }
}

impl SourceBlockIndicator for CopySourceIndicator {
    fn source(&self, block: u64) -> Option<u64> {
        self.rmap.get(&block).copied()
    }
}

//------------------------------------------

struct CopyVerifier {
    dev: Ramdisk,
    block_size: usize,
    seed: u64,
    indicator: Box<dyn SourceBlockIndicator>,
    buf: Buffer,
    faulty_src_blocks: RoaringBitmap,
    faulty_dst_blocks: RoaringBitmap,
}

impl CopyVerifier {
    fn new(
        dev: Ramdisk,
        block_size: usize,
        seed: u64,
        indicator: Box<dyn SourceBlockIndicator>,
    ) -> Self {
        let buf = Buffer::new(block_size, PAGE_SIZE);

        Self {
            dev,
            block_size,
            seed,
            indicator,
            buf,
            faulty_src_blocks: RoaringBitmap::new(),
            faulty_dst_blocks: RoaringBitmap::new(),
        }
    }
}

impl BlockVisitor for CopyVerifier {
    fn visit(&mut self, block: u64) -> Result<()> {
        if self.faulty_dst_blocks.contains(block as u32) {
            return Ok(()); // skip unwritable blocks
        }

        let seed = match self.indicator.source(block) {
            Some(src) => {
                if self.faulty_src_blocks.contains(src as u32) {
                    // blocks with unreadable sources should not be touched
                    self.seed ^ block
                } else {
                    self.seed ^ src ^ SRC_SALT
                }
            }
            None => self.seed ^ block,
        };

        let offset = block * self.block_size as u64;
        self.dev.read_exact_at(self.buf.get_data(), offset)?;

        let mut gen = Generator::new();
        if !gen.verify_buffer(seed, self.buf.get_data())? {
            return Err(anyhow!("data verification failed for block {}", block));
        }

        Ok(())
    }
}

//------------------------------------------

struct CopierTest {
    src: Ramdisk,
    dst: Ramdisk,
    block_size: usize,
    nr_src_blocks: u64,
    nr_dst_blocks: u64,
    seed: u64,
    faulty_src_blocks: RoaringBitmap,
    faulty_dst_blocks: RoaringBitmap,
}

impl CopierTest {
    fn new(block_size: u32, nr_src_blocks: u32, nr_dst_blocks: u32) -> Self {
        let src = Ramdisk::new(block_size * nr_src_blocks);
        let dst = Ramdisk::new(block_size * nr_dst_blocks);

        Self {
            src,
            dst,
            block_size: block_size as usize,
            nr_src_blocks: nr_src_blocks as u64,
            nr_dst_blocks: nr_dst_blocks as u64,
            seed: rand::thread_rng().gen::<u64>(),
            faulty_src_blocks: RoaringBitmap::new(),
            faulty_dst_blocks: RoaringBitmap::new(),
        }
    }

    fn stamp_src_dev(&self) -> Result<()> {
        let src = self.src.try_clone()?;
        let mut stamper = Stamper::new(src, self.seed ^ SRC_SALT, self.block_size);
        visit_blocks(self.nr_src_blocks, &mut stamper)
    }

    fn stamp_dst_dev(&self) -> Result<()> {
        let dst = self.dst.try_clone()?;
        let mut stamper = Stamper::new(dst, self.seed, self.block_size);
        visit_blocks(self.nr_dst_blocks, &mut stamper)
    }

    fn invalidate_src_dev(&mut self, bytes: Range<u32>) -> Result<()> {
        let block_begin = bytes.start / self.block_size as u32;
        let block_end = div_up(bytes.end, self.block_size as u32);

        self.src.invalidate(bytes);

        for b in block_begin..block_end {
            self.faulty_src_blocks.push(b);
        }

        Ok(())
    }

    fn invalidate_dst_dev(&mut self, bytes: Range<u32>) -> Result<()> {
        let block_begin = bytes.start / self.block_size as u32;
        let block_end = div_up(bytes.end, self.block_size as u32);

        self.dst.invalidate(bytes);

        for b in block_begin..block_end {
            self.faulty_dst_blocks.push(b);
        }

        Ok(())
    }

    fn verify(&self, ops: &[CopyOp]) -> Result<()> {
        let rmap: BTreeMap<u64, u64> = ops.iter().map(|op| (op.dst, op.src)).collect();
        let indicator = Box::new(CopySourceIndicator::new(rmap));

        let dst = self.dst.try_clone()?;
        let mut verifier = CopyVerifier::new(dst, self.block_size, self.seed, indicator);
        verifier.faulty_src_blocks = self.faulty_src_blocks.clone();
        verifier.faulty_dst_blocks = self.faulty_dst_blocks.clone();
        visit_blocks(self.nr_dst_blocks, &mut verifier)
    }

    fn copy(&self, ops: &[CopyOp], buffer_size: usize) -> Result<CopyStats> {
        let mut copier = SyncCopier::<SimpleBlockIo<Ramdisk>>::new(
            buffer_size,
            self.block_size,
            self.src.try_clone().unwrap().into(),
            self.dst.try_clone().unwrap().into(),
        )
        .unwrap();

        let progress = Arc::new(IgnoreProgress {});
        copier.copy(ops, progress)
    }
}

//------------------------------------------

// mirroring a device greater than the buffer size
#[test]
fn mirroring() -> Result<()> {
    const NR_BLOCKS: u64 = 1024;

    let t = CopierTest::new(BLOCK_SIZE, NR_BLOCKS as u32, NR_BLOCKS as u32);
    t.stamp_src_dev()?;
    t.stamp_dst_dev()?;

    let ops = mk_ops(0..NR_BLOCKS, 0..NR_BLOCKS);
    let stats = t.copy(&ops, BUFFER_SIZE).unwrap();
    assert_eq!(stats.nr_blocks, ops.len() as u64);
    assert_eq!(stats.nr_copied, ops.len() as u64);
    assert!(stats.read_errors.is_empty());
    assert!(stats.write_errors.is_empty());

    t.verify(&ops)?;

    Ok(())
}

#[test]
fn copy_with_ops_sorted_by_src() -> Result<()> {
    const NR_BLOCKS: u64 = 1024;

    let t = CopierTest::new(BLOCK_SIZE, NR_BLOCKS as u32, NR_BLOCKS as u32);
    t.stamp_src_dev()?;
    t.stamp_dst_dev()?;

    let ops = mk_ops(0..NR_BLOCKS, (0..NR_BLOCKS).rev());
    let stats = t.copy(&ops, BUFFER_SIZE).unwrap();
    assert_eq!(stats.nr_blocks, NR_BLOCKS);
    assert_eq!(stats.nr_copied, NR_BLOCKS);
    assert!(stats.read_errors.is_empty());
    assert!(stats.write_errors.is_empty());

    t.verify(&ops)?;

    Ok(())
}

#[test]
fn copy_with_ops_sorted_by_dst() -> Result<()> {
    const NR_BLOCKS: u64 = 1024;

    let t = CopierTest::new(BLOCK_SIZE, NR_BLOCKS as u32, NR_BLOCKS as u32);
    t.stamp_src_dev()?;
    t.stamp_dst_dev()?;

    let ops = mk_ops((0..NR_BLOCKS).rev(), 0..NR_BLOCKS);
    let stats = t.copy(&ops, BUFFER_SIZE).unwrap();
    assert_eq!(stats.nr_blocks, ops.len() as u64);
    assert_eq!(stats.nr_copied, ops.len() as u64);
    assert!(stats.read_errors.is_empty());
    assert!(stats.write_errors.is_empty());

    t.verify(&ops)
}

#[test]
fn copy_randomly() -> Result<()> {
    const NR_BLOCKS: u64 = 1024;

    let t = CopierTest::new(BLOCK_SIZE, NR_BLOCKS as u32, NR_BLOCKS as u32);
    t.stamp_src_dev()?;
    t.stamp_dst_dev()?;

    let nr_ops = (NR_BLOCKS / 2) as usize;
    let ops = mk_random_ops(0..NR_BLOCKS, 0..NR_BLOCKS, nr_ops);
    let stats = t.copy(&ops, BUFFER_SIZE).unwrap();
    assert_eq!(stats.nr_blocks, ops.len() as u64);
    assert_eq!(stats.nr_copied, ops.len() as u64);
    assert!(stats.read_errors.is_empty());
    assert!(stats.write_errors.is_empty());

    t.verify(&ops)
}

#[test]
fn skip_read_failed_blocks() -> Result<()> {
    const NR_BLOCKS: u64 = 1024;

    let mut t = CopierTest::new(BLOCK_SIZE, NR_BLOCKS as u32, NR_BLOCKS as u32);
    t.stamp_src_dev()?;
    t.stamp_dst_dev()?;
    t.invalidate_src_dev(32768..36864)?;

    let ops = mk_ops(0..NR_BLOCKS, 0..NR_BLOCKS);
    let stats = t.copy(&ops, BUFFER_SIZE).unwrap();
    assert_eq!(stats.nr_blocks, ops.len() as u64);
    assert_eq!(stats.nr_copied, ops.len() as u64 - 1);
    assert_eq!(stats.read_errors.len(), 1);
    assert!(stats.write_errors.is_empty());

    assert_eq!(stats.read_errors[0], CopyOp { src: 1, dst: 1 });

    t.verify(&ops)
}

#[test]
fn skip_write_failed_blocks() -> Result<()> {
    const NR_BLOCKS: u64 = 1024;

    let mut t = CopierTest::new(BLOCK_SIZE, NR_BLOCKS as u32, NR_BLOCKS as u32);
    t.stamp_src_dev()?;
    t.stamp_dst_dev()?;
    t.invalidate_dst_dev(32768..36864)?;

    let ops = mk_ops(0..NR_BLOCKS, 0..NR_BLOCKS);
    let stats = t.copy(&ops, BUFFER_SIZE).unwrap();
    assert_eq!(stats.nr_blocks, ops.len() as u64);
    assert_eq!(stats.nr_copied, ops.len() as u64 - 1);
    assert!(stats.read_errors.is_empty());
    assert_eq!(stats.write_errors.len(), 1);

    assert_eq!(stats.write_errors[0], CopyOp { src: 1, dst: 1 });

    t.verify(&ops)
}

#[test]
fn skip_copy_if_all_read_failed() -> Result<()> {
    const NR_BLOCKS: u64 = 1024;

    let mut t = CopierTest::new(BLOCK_SIZE, NR_BLOCKS as u32, NR_BLOCKS as u32);
    t.stamp_src_dev()?;
    t.stamp_dst_dev()?;
    t.invalidate_src_dev(0..NR_BLOCKS as u32 * BLOCK_SIZE)?;

    let mut ops = mk_ops(0..NR_BLOCKS, 0..NR_BLOCKS);
    let mut stats = t.copy(&ops, BUFFER_SIZE).unwrap();
    assert_eq!(stats.nr_blocks, ops.len() as u64);
    assert_eq!(stats.nr_copied, 0);
    assert_eq!(stats.read_errors.len(), ops.len());
    assert!(stats.write_errors.is_empty());

    ops.sort_unstable_by(|lhs, rhs| lhs.src.cmp(&rhs.src));
    stats
        .read_errors
        .sort_unstable_by(|lhs, rhs| lhs.src.cmp(&rhs.src));
    assert_eq!(ops, stats.read_errors);

    t.verify(&ops)
}

#[test]
fn skip_copy_if_all_write_failed() -> Result<()> {
    const NR_BLOCKS: u64 = 1024;

    let mut t = CopierTest::new(BLOCK_SIZE, NR_BLOCKS as u32, NR_BLOCKS as u32);
    t.stamp_src_dev()?;
    t.stamp_dst_dev()?;
    t.invalidate_dst_dev(0..NR_BLOCKS as u32 * BLOCK_SIZE)?;

    let mut ops = mk_ops(0..NR_BLOCKS, 0..NR_BLOCKS);
    let mut stats = t.copy(&ops, BUFFER_SIZE).unwrap();
    assert_eq!(stats.nr_blocks, ops.len() as u64);
    assert_eq!(stats.nr_copied, 0);
    assert!(stats.read_errors.is_empty());
    assert_eq!(stats.write_errors.len(), ops.len());

    ops.sort_unstable_by(|lhs, rhs| lhs.src.cmp(&rhs.src));
    stats
        .write_errors
        .sort_unstable_by(|lhs, rhs| lhs.src.cmp(&rhs.src));
    assert_eq!(ops, stats.write_errors);

    t.verify(&ops)
}

#[test]
fn copy_length_is_less_than_buffer_size_should_success() -> Result<()> {
    const NR_BLOCKS: u64 = 1024;

    let t = CopierTest::new(BLOCK_SIZE, NR_BLOCKS as u32, NR_BLOCKS as u32);
    t.stamp_src_dev()?;
    t.stamp_dst_dev()?;

    let ops = mk_random_ops(0..1024, 0..NR_BLOCKS, 16); // a small numbers of ops
    let stats = t.copy(&ops, BUFFER_SIZE).unwrap();
    assert_eq!(stats.nr_blocks, ops.len() as u64);
    assert_eq!(stats.nr_copied, ops.len() as u64);
    assert!(stats.read_errors.is_empty());
    assert!(stats.write_errors.is_empty());

    t.verify(&ops)
}

#[test]
fn copy_length_is_not_a_multiple_of_buffer_size_should_success() -> Result<()> {
    const NR_BLOCKS: u64 = 1024;

    let t = CopierTest::new(BLOCK_SIZE, NR_BLOCKS as u32, NR_BLOCKS as u32);
    t.stamp_src_dev()?;
    t.stamp_dst_dev()?;

    let nr_ops = (BUFFER_SIZE / BLOCK_SIZE as usize) + 1;
    let ops = mk_random_ops(0..1024, 0..NR_BLOCKS, nr_ops);
    let stats = t.copy(&ops, BUFFER_SIZE).unwrap();
    assert_eq!(stats.nr_blocks, ops.len() as u64);
    assert_eq!(stats.nr_copied, ops.len() as u64);
    assert!(stats.read_errors.is_empty());
    assert!(stats.write_errors.is_empty());

    t.verify(&ops)
}

//------------------------------------------
