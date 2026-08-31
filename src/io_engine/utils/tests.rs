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
            let _ = self.faulty_blocks.try_push(b);
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

mod vectored_io {
    use super::*;

    const BLOCK_SIZE: u32 = 8192; // bytes
    const RAMDISK_SIZE: u32 = 65536; // bytes

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

    const BLOCK_SIZE: u32 = 8192; // bytes
    const RAMDISK_SIZE: u32 = 65536; // bytes

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

// Three distinct boundaries govern a vectored transfer, and read_blocks() has a
// separate arm for each:
//
//   * end of file    -- the io is short, leaving a ragged block half filled
//   * beyond EOF     -- the io succeeds and transfers nothing
//   * off_t overflow -- the kernel refuses a transfer ending past i64::MAX
//
// A fourth is ours alone: a block so high that `pos + n * block_size` is not
// representable at all, which never reaches the kernel.
mod boundaries {
    use super::*;

    use crate::io_engine::BLOCK_SIZE;
    use std::fs::File;

    // Highest block whose byte offset fits in a u64.  Note this is only about
    // our own arithmetic -- the kernel refuses such offsets long before here.
    const MAX_REPRESENTABLE_BLOCK: u64 = u64::MAX / BLOCK_SIZE as u64;

    // Highest block a single-block read may start at without the transfer
    // ending past i64::MAX, which the kernel rejects with EINVAL.
    const MAX_OFFT_BLOCK: u64 = (i64::MAX as u64) / BLOCK_SIZE as u64 - 1;

    fn file_of(len: u64) -> File {
        let f = tempfile::tempfile().unwrap();
        f.set_len(len).unwrap();
        f
    }

    // Returns the per-block results along with the buffers, so callers can also
    // inspect what was left in a partially filled block.
    fn read(f: &File, nr: usize, pos: u64, partial: bool) -> (Vec<Result<()>>, Vec<Vec<u8>>) {
        let mut owned: Vec<Vec<u8>> = (0..nr).map(|_| vec![0xffu8; BLOCK_SIZE]).collect();
        let mut bufs: Vec<&mut [u8]> = owned.iter_mut().map(|b| &mut b[..]).collect();
        let vio = if partial {
            VectoredBlockIo::with_partial(f)
        } else {
            VectoredBlockIo::from(f)
        };
        let results = vio.read_blocks(&mut bufs, pos).unwrap();
        (results, owned)
    }

    //---------------------------------
    // end of file: the io comes back short

    #[test]
    fn read_across_eof_leaves_a_ragged_block() {
        let f = file_of(2 * BLOCK_SIZE as u64 + 2048); // two whole blocks and a stub
        let (results, bufs) = read(&f, 3, 0, false);

        assert_eq!(results.len(), 3);
        assert!(results[0].is_ok());
        assert!(results[1].is_ok());
        // without partial io the ragged block counts as a failure ...
        assert!(results[2].is_err());
        // ... but it is still zero-filled past the bytes that arrived, so no
        // stale contents are left behind for the caller to read
        assert!(bufs[2][..2048].iter().all(|b| *b == 0));
        assert!(bufs[2][2048..].iter().all(|b| *b == 0));
    }

    #[test]
    fn read_across_eof_is_accepted_when_partial_io_is_allowed() {
        let f = file_of(2 * BLOCK_SIZE as u64 + 2048);
        let (results, _) = read(&f, 3, 0, true);

        assert_eq!(results.len(), 3);
        assert!(results.iter().all(|r| r.is_ok()));
    }

    //---------------------------------
    // past end of file: the io succeeds and transfers nothing

    #[test]
    fn read_starting_past_eof_reports_eof() {
        let f = file_of(2 * BLOCK_SIZE as u64);
        let (results, _) = read(&f, 2, 3 * BLOCK_SIZE as u64, false);

        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|r| r.is_err()));
    }

    //---------------------------------
    // off_t overflow: the kernel refuses the whole transfer

    #[test]
    fn read_ending_at_the_offt_limit_is_accepted() {
        let f = file_of(BLOCK_SIZE as u64);
        // the transfer ends exactly on i64::MAX, so the kernel takes it and
        // reports end of file rather than EINVAL
        let (results, _) = read(&f, 1, MAX_OFFT_BLOCK * BLOCK_SIZE as u64, false);

        assert_eq!(results.len(), 1);
        assert!(results[0].is_err()); // EOF, not a rejection
    }

    #[test]
    fn read_spanning_the_offt_limit_fails_every_block() {
        let f = file_of(BLOCK_SIZE as u64);
        // the second block pushes the transfer past i64::MAX, so the kernel
        // rejects the call outright and the retry fares no better
        let (results, _) = read(&f, 2, MAX_OFFT_BLOCK * BLOCK_SIZE as u64, false);

        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|r| r.is_err()));
    }

    //---------------------------------
    // beyond our own arithmetic: never handed to the kernel at all

    #[test]
    fn read_past_the_representable_offset_is_trimmed() {
        let f = file_of(BLOCK_SIZE as u64);
        let (results, _) = read(&f, 2, MAX_REPRESENTABLE_BLOCK * BLOCK_SIZE as u64, false);

        assert_eq!(results.len(), 2);
        // the first block has an offset, so it reaches the kernel and is refused
        assert!(results[0].is_err());
        // the second has none, so it is reported without an io being attempted
        assert_eq!(
            results[1].as_ref().unwrap_err().to_string(),
            "block address out of range"
        );
    }

    //---------------------------------
    // the write path has the same boundaries

    fn write(f: &File, nr: usize, pos: u64) -> Vec<Result<()>> {
        let owned: Vec<Vec<u8>> = (0..nr).map(|_| vec![0u8; BLOCK_SIZE]).collect();
        let bufs: Vec<&[u8]> = owned.iter().map(|b| &b[..]).collect();
        VectoredBlockIo::from(f).write_blocks(&bufs, pos).unwrap()
    }

    #[test]
    fn write_spanning_the_offt_limit_fails_every_block() {
        let f = file_of(BLOCK_SIZE as u64);
        let results = write(&f, 2, MAX_OFFT_BLOCK * BLOCK_SIZE as u64);

        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|r| r.is_err()));
    }

    #[test]
    fn write_past_the_representable_offset_is_trimmed() {
        let f = file_of(BLOCK_SIZE as u64);
        let results = write(&f, 2, MAX_REPRESENTABLE_BLOCK * BLOCK_SIZE as u64);

        assert_eq!(results.len(), 2);
        assert!(results[0].is_err());
        assert_eq!(
            results[1].as_ref().unwrap_err().to_string(),
            "block address out of range"
        );
    }
}
