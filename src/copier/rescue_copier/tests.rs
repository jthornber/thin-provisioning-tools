use super::*;
use rand::prelude::*;
use rand::Rng;
use roaring::RoaringBitmap;
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
const BLOCK_SIZE: u32 = 32768; // 32 KiB

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

trait SourcePageIndicator {
    fn source(&self, page: u64) -> Option<u64>;
}

struct CopySourceIndicator {
    rmap: BTreeMap<u64, u64>, // rmap of blocks
    pages_per_block: usize,
}

impl CopySourceIndicator {
    fn new(rmap: BTreeMap<u64, u64>, pages_per_block: usize) -> Self {
        Self {
            rmap,
            pages_per_block,
        }
    }
}

impl SourcePageIndicator for CopySourceIndicator {
    fn source(&self, page: u64) -> Option<u64> {
        let block = page / self.pages_per_block as u64;
        let page_off = page % self.pages_per_block as u64;
        self.rmap
            .get(&block)
            .map(|src| src * self.pages_per_block as u64 + page_off)
    }
}

//------------------------------------------

// page-based verifier
struct CopyVerifier {
    dev: Ramdisk,
    seed: u64,
    indicator: Box<dyn SourcePageIndicator>,
    buf: Buffer,
    faulty_src_pages: RoaringBitmap,
    faulty_dst_pages: RoaringBitmap,
}

impl CopyVerifier {
    fn new(dev: Ramdisk, seed: u64, indicator: Box<dyn SourcePageIndicator>) -> Self {
        let buf = Buffer::new(PAGE_SIZE, PAGE_SIZE);

        Self {
            dev,
            seed,
            indicator,
            buf,
            faulty_src_pages: RoaringBitmap::new(),
            faulty_dst_pages: RoaringBitmap::new(),
        }
    }
}

impl BlockVisitor for CopyVerifier {
    fn visit(&mut self, page: u64) -> Result<()> {
        if self.faulty_dst_pages.contains(page as u32) {
            return Ok(()); // skip unwritable blocks
        }

        let seed = match self.indicator.source(page) {
            Some(src) => {
                if self.faulty_src_pages.contains(src as u32) {
                    // pages with unreadable sources should not be touched
                    self.seed ^ page
                } else {
                    self.seed ^ src ^ SRC_SALT
                }
            }
            None => self.seed ^ page,
        };

        let offset = page << PAGE_SHIFT;
        self.dev.read_exact_at(self.buf.get_data(), offset)?;

        let mut gen = Generator::new();
        if !gen.verify_buffer(seed, self.buf.get_data())? {
            return Err(anyhow!("data verification failed for page {}", page));
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
    faulty_src_pages: RoaringBitmap,
    faulty_dst_pages: RoaringBitmap,
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
            faulty_src_pages: RoaringBitmap::new(),
            faulty_dst_pages: RoaringBitmap::new(),
        }
    }

    fn stamp_src_dev(&self) -> Result<()> {
        let src = self.src.try_clone()?;
        let pages_per_block = self.block_size >> PAGE_SHIFT;
        let mut stamper = Stamper::new(src, self.seed ^ SRC_SALT, PAGE_SIZE);
        visit_blocks(self.nr_src_blocks * pages_per_block as u64, &mut stamper)
    }

    fn stamp_dst_dev(&self) -> Result<()> {
        let dst = self.dst.try_clone()?;
        let pages_per_block = self.block_size >> PAGE_SHIFT;
        let mut stamper = Stamper::new(dst, self.seed, PAGE_SIZE);
        visit_blocks(self.nr_dst_blocks * pages_per_block as u64, &mut stamper)
    }

    fn invalidate_src_dev(&mut self, bytes: Range<u32>) -> Result<()> {
        let page_begin = bytes.start >> PAGE_SHIFT;
        let page_end = div_up(bytes.end, PAGE_SIZE as u32);

        self.src.invalidate(bytes);

        for p in page_begin..page_end {
            self.faulty_src_pages.push(p);
        }

        Ok(())
    }

    fn invalidate_dst_dev(&mut self, bytes: Range<u32>) -> Result<()> {
        let page_begin = bytes.start >> PAGE_SHIFT;
        let page_end = div_up(bytes.end, PAGE_SIZE as u32);

        self.dst.invalidate(bytes);

        for p in page_begin..page_end {
            self.faulty_dst_pages.push(p);
        }

        Ok(())
    }

    fn verify(&self, ops: &[CopyOp]) -> Result<()> {
        let rmap: BTreeMap<u64, u64> = ops.iter().map(|op| (op.dst, op.src)).collect();
        let pages_per_block = self.block_size >> PAGE_SHIFT;
        let indicator = Box::new(CopySourceIndicator::new(rmap, pages_per_block));

        let dst = self.dst.try_clone()?;
        let mut verifier = CopyVerifier::new(dst, self.seed, indicator);
        verifier.faulty_src_pages = self.faulty_src_pages.clone();
        verifier.faulty_dst_pages = self.faulty_dst_pages.clone();
        visit_blocks(self.nr_dst_blocks * pages_per_block as u64, &mut verifier)
    }

    fn copy(&self, ops: &[CopyOp]) -> Result<CopyStats> {
        let mut copier = RescueCopier::<Ramdisk>::new(
            self.block_size,
            self.src.try_clone().unwrap(),
            self.dst.try_clone().unwrap(),
        )
        .unwrap();

        let progress = Arc::new(IgnoreProgress {});
        copier.copy(ops, progress)
    }
}

//------------------------------------------

#[test]
fn copy_completed_blocks() -> Result<()> {
    const NR_BLOCKS: u64 = 1024;
    let t = CopierTest::new(BLOCK_SIZE, NR_BLOCKS as u32, NR_BLOCKS as u32);
    t.stamp_src_dev()?;
    t.stamp_dst_dev()?;

    let ops = mk_random_ops(0..NR_BLOCKS, 0..NR_BLOCKS, NR_BLOCKS as usize);
    let stats = t.copy(&ops).unwrap();
    assert_eq!(stats.nr_blocks, ops.len() as u64);
    assert_eq!(stats.nr_copied, ops.len() as u64);
    assert!(stats.read_errors.is_empty());
    assert!(stats.write_errors.is_empty());

    t.verify(&ops)
}

#[test]
fn partial_copy_with_unreadable_pages() -> Result<()> {
    const NR_BLOCKS: u64 = 1024;
    let mut t = CopierTest::new(BLOCK_SIZE, NR_BLOCKS as u32, NR_BLOCKS as u32);
    t.stamp_src_dev()?;
    t.stamp_dst_dev()?;
    t.invalidate_src_dev(65536..69632)?;

    let ops = mk_random_ops(0..NR_BLOCKS, 0..NR_BLOCKS, NR_BLOCKS as usize);
    let stats = t.copy(&ops).unwrap();
    assert_eq!(stats.nr_blocks, ops.len() as u64);
    assert_eq!(stats.nr_copied, ops.len() as u64 - 1);
    assert_eq!(stats.read_errors.len(), 1);
    assert!(stats.write_errors.is_empty());

    assert_eq!(stats.read_errors[0].src, 2);

    t.verify(&ops)
}

#[test]
fn partial_copy_with_unwritable_pages() -> Result<()> {
    const NR_BLOCKS: u64 = 1024;
    let mut t = CopierTest::new(BLOCK_SIZE, NR_BLOCKS as u32, NR_BLOCKS as u32);
    t.stamp_src_dev()?;
    t.stamp_dst_dev()?;
    t.invalidate_dst_dev(32768..36882)?;

    let ops = mk_random_ops(0..NR_BLOCKS, 0..NR_BLOCKS, NR_BLOCKS as usize);
    let stats = t.copy(&ops).unwrap();
    assert_eq!(stats.nr_blocks, ops.len() as u64);
    assert_eq!(stats.nr_copied, ops.len() as u64 - 1);
    assert!(stats.read_errors.is_empty());
    assert_eq!(stats.write_errors.len(), 1);

    assert_eq!(stats.write_errors[0].dst, 1);

    t.verify(&ops)
}

#[test]
fn partial_copy_with_unreadable_and_unwritable_pages() -> Result<()> {
    const NR_BLOCKS: u64 = 1024;
    let mut t = CopierTest::new(BLOCK_SIZE, NR_BLOCKS as u32, NR_BLOCKS as u32);
    t.stamp_src_dev()?;
    t.stamp_dst_dev()?;
    t.invalidate_src_dev(65536..69632)?;
    t.invalidate_dst_dev(32768..36882)?;

    let ops = mk_random_ops(0..NR_BLOCKS, 0..NR_BLOCKS, NR_BLOCKS as usize);
    let stats = t.copy(&ops).unwrap();
    assert_eq!(stats.nr_blocks, ops.len() as u64);
    assert_eq!(stats.nr_copied, ops.len() as u64 - 2);
    assert_eq!(stats.read_errors.len(), 1);
    assert_eq!(stats.write_errors.len(), 1);

    assert_eq!(stats.read_errors[0].src, 2);
    assert_eq!(stats.write_errors[0].dst, 1);

    t.verify(&ops)
}

//------------------------------------------
