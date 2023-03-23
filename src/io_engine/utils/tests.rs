use super::*;

use roaring::RoaringBitmap;
use std::ops::{Range, Sub};
use std::vec::Vec;

use crate::io_engine::buffer::Buffer;
use crate::io_engine::ramdisk::Ramdisk;
use crate::math::div_up;

//-------------------------------------

fn length<T: Copy + Sub<Output = T>>(r: &Range<T>) -> T {
    r.end - r.start
}

//-------------------------------------

struct TestContext {
    disk: Ramdisk,
    block_size: u32,
    offset: u32,
    faulty_blocks: RoaringBitmap,
}

impl TestContext {
    // Ramdisk is u32-based
    fn new(disk_size: u32, block_size: u32) -> Self {
        Self {
            disk: Ramdisk::new(disk_size),
            block_size,
            offset: 0,
            faulty_blocks: RoaringBitmap::new(),
        }
    }

    #[allow(dead_code)]
    fn offset(mut self, offset: u32) -> Self {
        self.offset = offset;
        self
    }

    fn get_block_size(&self) -> u32 {
        self.block_size
    }

    fn set_faulty(&mut self, bytes: std::ops::Range<u32>) {
        self.disk.invalidate(bytes.clone());

        let block_begin = (bytes.start - self.offset) / self.block_size;
        let block_end = div_up(bytes.end - self.offset, self.block_size);
        for b in block_begin..block_end {
            self.faulty_blocks.push(b);
        }
    }
}

//-------------------------------------

trait Validator {
    fn validate(&self, blocks: Range<u64>, results: &[anyhow::Result<()>]);
}

struct ReadWriteTest<T, V> {
    dev: T,
    block_size: usize,
    offset: u64,
    validator: V,
}

impl<T: ReadBlocks + WriteBlocks, V: Validator> ReadWriteTest<T, V> {
    fn new(dev: T, block_size: usize, validator: V) -> ReadWriteTest<T, V> {
        ReadWriteTest {
            dev,
            block_size,
            offset: 0,
            validator,
        }
    }

    #[allow(dead_code)]
    fn offset(mut self, offset: u64) -> Self {
        self.offset = offset;
        self
    }

    fn test_read(&self, blocks: Range<u64>) {
        let buf = Buffer::new(self.block_size * length(&blocks) as usize, 4096);
        let mut bufs: Vec<&mut [u8]> = buf.get_data().chunks_mut(self.block_size).collect();
        let pos = self.offset + blocks.start * self.block_size as u64;
        let ret = self.dev.read_blocks(&mut bufs, pos);
        assert!(ret.is_ok());
        self.validator.validate(blocks, &ret.unwrap());
    }

    fn test_write(&self, blocks: Range<u64>) {
        let buf = Buffer::new(self.block_size * length(&blocks) as usize, 4096);
        let bufs: Vec<&[u8]> = buf.get_data().chunks(self.block_size).collect();
        let pos = self.offset + blocks.start * self.block_size as u64;
        let ret = self.dev.write_blocks(&bufs, pos);
        assert!(ret.is_ok());
        self.validator.validate(blocks, &ret.unwrap());
    }
}

//-------------------------------------

const BLOCK_SIZE: u32 = 8192; // bytes
const RAMDISK_SIZE: u32 = 65536; // bytes

mod vectored_io {
    use super::*;

    struct VectoredIoValidator {
        faulty_blocks: RoaringBitmap,
    }

    impl VectoredIoValidator {
        fn new(faulty_blocks: RoaringBitmap) -> Self {
            Self { faulty_blocks }
        }
    }

    impl Validator for VectoredIoValidator {
        fn validate(&self, blocks: Range<u64>, results: &[anyhow::Result<()>]) {
            let nr_blocks = length(&blocks) as usize;
            assert_eq!(results.len(), nr_blocks);

            // trait ExactSizeIterator is not implemented for Range<u64> (rust-lang pr#22299)
            // so we cannot do reverse traversal like rev() or rposition().
            let mut err_len = 0;
            for (i, b) in blocks.enumerate() {
                if self.faulty_blocks.contains(b as u32) {
                    err_len = i + 1;
                }
            }

            // all the blocks before the last faulty one should fail
            for r in results.iter().take(err_len) {
                assert!(r.is_err());
            }
            for r in results.iter().skip(err_len) {
                assert!(r.is_ok());
            }
        }
    }

    impl From<RoaringBitmap> for VectoredIoValidator {
        fn from(faulty_blocks: RoaringBitmap) -> VectoredIoValidator {
            VectoredIoValidator::new(faulty_blocks)
        }
    }

    fn to_vectored_test(
        ctx: TestContext,
    ) -> ReadWriteTest<VectoredBlockIo<Ramdisk>, VectoredIoValidator> {
        let block_size = ctx.get_block_size() as usize;
        let validator = VectoredIoValidator::from(ctx.faulty_blocks);
        ReadWriteTest::new(VectoredBlockIo::from(ctx.disk), block_size, validator)
    }

    //-------------------------------------

    #[test]
    fn read_from_the_faulty_block_should_skip() {
        let mut ctx = TestContext::new(RAMDISK_SIZE, BLOCK_SIZE);
        ctx.set_faulty(0..512);

        let t = to_vectored_test(ctx);
        t.test_read(0..4);
    }

    #[test]
    fn read_overlap_the_faulty_block_should_skip() {
        let mut ctx = TestContext::new(RAMDISK_SIZE, BLOCK_SIZE);
        ctx.set_faulty(BLOCK_SIZE..BLOCK_SIZE + 512);

        let t = to_vectored_test(ctx);
        t.test_read(0..4);
    }

    #[test]
    fn read_until_the_faulty_block_should_fail() {
        let mut ctx = TestContext::new(RAMDISK_SIZE, BLOCK_SIZE);
        ctx.set_faulty((RAMDISK_SIZE - 512)..RAMDISK_SIZE);

        let t = to_vectored_test(ctx);
        let nr_blocks = RAMDISK_SIZE / BLOCK_SIZE;
        t.test_read(0..nr_blocks as u64);
    }

    #[test]
    fn read_before_the_faulty_block_should_success() {
        let mut ctx = TestContext::new(RAMDISK_SIZE, BLOCK_SIZE);
        ctx.set_faulty((RAMDISK_SIZE - 4096)..RAMDISK_SIZE);

        let t = to_vectored_test(ctx);
        t.test_read(0..4);
    }

    #[test]
    fn write_to_the_faulty_block_should_skip() {
        let mut ctx = TestContext::new(RAMDISK_SIZE, BLOCK_SIZE);
        ctx.set_faulty(0..512);

        let t = to_vectored_test(ctx);
        t.test_write(0..4);
    }

    #[test]
    fn write_overlap_the_faulty_block_should_skip() {
        let mut ctx = TestContext::new(RAMDISK_SIZE, BLOCK_SIZE);
        ctx.set_faulty(BLOCK_SIZE..BLOCK_SIZE + 512);

        let t = to_vectored_test(ctx);
        t.test_write(0..4);
    }

    #[test]
    fn write_until_the_faulty_block_should_fail() {
        let mut ctx = TestContext::new(RAMDISK_SIZE, BLOCK_SIZE);
        ctx.set_faulty((RAMDISK_SIZE - 512)..RAMDISK_SIZE);

        let t = to_vectored_test(ctx);
        let nr_blocks = RAMDISK_SIZE / BLOCK_SIZE;
        t.test_write(0..nr_blocks as u64);
    }

    #[test]
    fn write_before_the_faulty_block_should_success() {
        let mut ctx = TestContext::new(RAMDISK_SIZE, BLOCK_SIZE);
        ctx.set_faulty((RAMDISK_SIZE - 4096)..RAMDISK_SIZE);

        let t = to_vectored_test(ctx);
        t.test_write(0..4);
    }
}

//-------------------------------------

mod simple_io {
    use super::*;

    struct SimpleIoValidator {
        faulty_blocks: RoaringBitmap,
    }

    impl SimpleIoValidator {
        fn new(faulty_blocks: RoaringBitmap) -> Self {
            Self { faulty_blocks }
        }
    }

    impl Validator for SimpleIoValidator {
        fn validate(&self, blocks: Range<u64>, results: &[anyhow::Result<()>]) {
            let nr_blocks = length(&blocks) as usize;
            assert_eq!(results.len(), nr_blocks);

            for (b, r) in blocks.zip(results) {
                if self.faulty_blocks.contains(b as u32) {
                    assert!(r.is_err());
                } else {
                    assert!(r.is_ok());
                }
            }
        }
    }

    impl From<RoaringBitmap> for SimpleIoValidator {
        fn from(faulty_blocks: RoaringBitmap) -> SimpleIoValidator {
            SimpleIoValidator::new(faulty_blocks)
        }
    }

    fn to_simple_test(
        ctx: TestContext,
    ) -> ReadWriteTest<SimpleBlockIo<Ramdisk>, SimpleIoValidator> {
        let block_size = ctx.get_block_size() as usize;
        let validator = SimpleIoValidator::from(ctx.faulty_blocks);
        ReadWriteTest::new(SimpleBlockIo::from(ctx.disk), block_size, validator)
    }

    //-------------------------------------

    #[test]
    fn read_from_the_faulty_block() {
        let mut ctx = TestContext::new(RAMDISK_SIZE, BLOCK_SIZE);
        ctx.set_faulty(0..512);

        let t = to_simple_test(ctx);
        t.test_read(0..4);
    }

    #[test]
    fn read_overlap_the_faulty_block() {
        let mut ctx = TestContext::new(RAMDISK_SIZE, BLOCK_SIZE);
        ctx.set_faulty(BLOCK_SIZE..BLOCK_SIZE + 512);

        let t = to_simple_test(ctx);
        t.test_read(0..4);
    }

    #[test]
    fn read_until_the_faulty_block() {
        let mut ctx = TestContext::new(RAMDISK_SIZE, BLOCK_SIZE);
        ctx.set_faulty((RAMDISK_SIZE - 512)..RAMDISK_SIZE);

        let t = to_simple_test(ctx);
        let nr_blocks = RAMDISK_SIZE / BLOCK_SIZE;
        t.test_read(0..nr_blocks as u64);
    }

    #[test]
    fn read_before_the_faulty_block_should_success() {
        let mut ctx = TestContext::new(RAMDISK_SIZE, BLOCK_SIZE);
        ctx.set_faulty((RAMDISK_SIZE - 4096)..RAMDISK_SIZE);

        let t = to_simple_test(ctx);
        t.test_read(0..4);
    }

    #[test]
    fn write_to_the_faulty_block() {
        let mut ctx = TestContext::new(RAMDISK_SIZE, BLOCK_SIZE);
        ctx.set_faulty(0..512);

        let t = to_simple_test(ctx);
        t.test_write(0..4);
    }

    #[test]
    fn write_overlap_the_faulty_block() {
        let mut ctx = TestContext::new(RAMDISK_SIZE, BLOCK_SIZE);
        ctx.set_faulty(BLOCK_SIZE..BLOCK_SIZE + 512);

        let t = to_simple_test(ctx);
        t.test_write(0..4);
    }

    #[test]
    fn write_until_the_faulty_block() {
        let mut ctx = TestContext::new(RAMDISK_SIZE, BLOCK_SIZE);
        ctx.set_faulty((RAMDISK_SIZE - 512)..RAMDISK_SIZE);

        let t = to_simple_test(ctx);
        let nr_blocks = RAMDISK_SIZE / BLOCK_SIZE;
        t.test_write(0..nr_blocks as u64);
    }

    #[test]
    fn write_before_the_faulty_block_should_success() {
        let mut ctx = TestContext::new(RAMDISK_SIZE, BLOCK_SIZE);
        ctx.set_faulty((RAMDISK_SIZE - 4096)..RAMDISK_SIZE);

        let t = to_simple_test(ctx);
        t.test_write(0..4);
    }
}

//-------------------------------------
