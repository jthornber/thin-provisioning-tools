use anyhow::{anyhow, Result};
use fixedbitset::FixedBitSet;
use rand::prelude::*;
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use thinp::cache::mapping::*;
use thinp::cache::superblock::*;
use thinp::copier::test_utils::*;
use thinp::file_utils::create_sized_file;
use thinp::io_engine::buffer::Buffer;
use thinp::io_engine::{self, SyncIoEngine, PAGE_SIZE, SECTOR_SHIFT};
use thinp::pdata::array::{self, *};
use thinp::pdata::array_walker::*;
use thinp::pdata::bitset::read_bitset;
use thinp::random::Generator;

mod common;

use common::process::*;
use common::target::*;
use common::test_dir::*;

//------------------------------------------

const CACHE_SALT: u64 = 0xA825C613B704D0E1;

//------------------------------------------

trait SourceBlockIndicator {
    fn source(&self, oblock: u64) -> Option<u32>;
}

struct DirtySourceIndicator {
    rmap: BTreeMap<u64, u32>,
    dirty_oblocks: FixedBitSet, // dirty bitset for oblocks
}

impl DirtySourceIndicator {
    fn new(rmap: BTreeMap<u64, u32>, dirty_bits: FixedBitSet, nr_origin_blocks: u64) -> Self {
        let mut dirty_oblocks = FixedBitSet::with_capacity(nr_origin_blocks as usize);

        for (oblock, cblock) in rmap.iter() {
            if dirty_bits.contains(*cblock as usize) {
                dirty_oblocks.insert(*oblock as usize);
            }
        }

        Self {
            rmap,
            dirty_oblocks,
        }
    }
}

impl SourceBlockIndicator for DirtySourceIndicator {
    fn source(&self, oblock: u64) -> Option<u32> {
        if self.dirty_oblocks.contains(oblock as usize) {
            Some(self.rmap[&oblock])
        } else {
            None
        }
    }
}

struct WritebackAllIndicator {
    rmap: BTreeMap<u64, u32>,
}

impl WritebackAllIndicator {
    fn new(rmap: BTreeMap<u64, u32>) -> Self {
        Self { rmap }
    }
}

impl SourceBlockIndicator for WritebackAllIndicator {
    fn source(&self, oblock: u64) -> Option<u32> {
        self.rmap.get(&oblock).copied()
    }
}

struct NoopIndicator;

impl SourceBlockIndicator for NoopIndicator {
    fn source(&self, _oblock: u64) -> Option<u32> {
        None
    }
}

//------------------------------------------

struct WritebackVerifier {
    origin_dev: File,
    block_size: usize, // bytes
    seed: u64,
    indicator: Box<dyn SourceBlockIndicator>,
    buf: Buffer,
    offset: u64, // bytes
}

impl WritebackVerifier {
    fn new(
        origin_dev: File,
        block_size: usize,
        seed: u64,
        indicator: Box<dyn SourceBlockIndicator>,
    ) -> Result<Self> {
        let buf = Buffer::new(block_size, PAGE_SIZE);

        Ok(Self {
            origin_dev,
            block_size,
            seed,
            buf,
            indicator,
            offset: 0,
        })
    }

    fn offset(mut self, offset: u64) -> Self {
        self.offset = offset;
        self
    }
}

impl BlockVisitor for WritebackVerifier {
    fn visit(&mut self, oblock: u64) -> Result<()> {
        let seed = match self.indicator.source(oblock) {
            Some(cblock) => self.seed ^ cblock as u64 ^ CACHE_SALT,
            None => self.seed ^ oblock,
        };

        let offset = oblock * self.block_size as u64 + self.offset;
        self.origin_dev.read_exact_at(self.buf.get_data(), offset)?;

        let mut gen = Generator::new();
        if !gen.verify_buffer(seed, self.buf.get_data())? {
            return Err(anyhow!("data verification failed for block {}", oblock));
        }

        Ok(())
    }
}

//------------------------------------------

struct WritebackTest {
    #[allow(dead_code)]
    td: TestDir,
    cache_block_size: usize, // bytes
    nr_cache_blocks: u32,
    nr_origin_blocks: u64,
    metadata_dev: PathBuf,
    fast_dev: PathBuf,
    origin_dev: PathBuf,
    fast_dev_offset: u64,   // bytes
    origin_dev_offset: u64, // bytes
    seed: u64,
}

impl WritebackTest {
    fn new() -> Result<Self> {
        let mut td = TestDir::new()?;
        let metadata_dev = td.mk_path("cmeta.bin");
        let fast_dev = td.mk_path("cache.bin");
        let origin_dev = td.mk_path("corig.bin");

        Ok(Self {
            td,
            cache_block_size: 0,
            nr_cache_blocks: 0,
            nr_origin_blocks: 0,
            metadata_dev,
            fast_dev,
            origin_dev,
            fast_dev_offset: 0,
            origin_dev_offset: 0,
            seed: rand::thread_rng().gen::<u64>(),
        })
    }

    fn format_metadata(
        &mut self,
        cache_block_size: usize,
        nr_cache_blocks: u32,
        nr_origin_blocks: u64,
        metadata_version: u32,
    ) -> Result<()> {
        self.format_metadata_with(
            cache_block_size,
            nr_cache_blocks,
            nr_origin_blocks,
            metadata_version,
            80,
            50,
        )
    }

    fn format_metadata_with(
        &mut self,
        cache_block_size: usize,
        nr_cache_blocks: u32,
        nr_origin_blocks: u64,
        metadata_version: u32,
        percent_resident: u32,
        percent_dirty: u32,
    ) -> Result<()> {
        self.cache_block_size = cache_block_size;
        self.nr_cache_blocks = nr_cache_blocks;
        self.nr_origin_blocks = nr_origin_blocks;

        let metadata_size: u64 = 4 << 20; // 4 MiB
        create_sized_file(&self.metadata_dev, metadata_size)?;

        let bs = (cache_block_size >> SECTOR_SHIFT).to_string();
        let nr_cblocks = nr_cache_blocks.to_string();
        let nr_oblocks = nr_origin_blocks.to_string();
        let version = metadata_version.to_string();
        let percent_resident = percent_resident.to_string();
        let percent_dirty = percent_dirty.to_string();

        run_ok(cache_generate_metadata_cmd(args![
            "--format",
            "-o",
            &self.metadata_dev,
            "--cache-block-size",
            &bs,
            "--nr-cache-blocks",
            &nr_cblocks,
            "--nr-origin-blocks",
            &nr_oblocks,
            "--metadata-version",
            &version,
            "--percent-resident",
            &percent_resident,
            "--percent-dirty",
            &percent_dirty
        ]))?;

        Ok(())
    }

    fn set_unclean_shutdown(&self) -> Result<()> {
        run_ok(cache_generate_metadata_cmd(args![
            "--set-clean-shutdown=false",
            "-o",
            &self.metadata_dev
        ]))?;
        Ok(())
    }

    fn damage_metadata(&mut self) -> Result<()> {
        // TODO: build a program like cache_generate_damage to create damage
        let file = OpenOptions::new()
            .read(false)
            .write(true)
            .custom_flags(libc::O_EXCL | libc::O_DIRECT)
            .open(&self.metadata_dev)?;

        let b = io_engine::Block::zeroed(1);
        file.write_all_at(b.get_data(), 4096)?; // break the second block

        Ok(())
    }

    fn set_fast_dev_offset(&mut self, offset: u64) -> Result<()> {
        if offset & (1 << SECTOR_SHIFT) != 0 {
            return Err(anyhow!("offset must be align to sectors"));
        }
        self.fast_dev_offset = offset;
        Ok(())
    }

    fn set_origin_dev_offset(&mut self, offset: u64) -> Result<()> {
        if offset & (1 << SECTOR_SHIFT) != 0 {
            return Err(anyhow!("offset must be align to sectors"));
        }
        self.origin_dev_offset = offset;
        Ok(())
    }

    fn stamp_cache_blocks(&self) -> Result<()> {
        let cache_size =
            self.nr_cache_blocks as u64 * self.cache_block_size as u64 + self.fast_dev_offset;
        let file = create_sized_file(&self.fast_dev, cache_size)?;

        let mut stamper = Stamper::new(file, self.seed ^ CACHE_SALT, self.cache_block_size)
            .offset(self.fast_dev_offset);
        visit_blocks(self.nr_cache_blocks as u64, &mut stamper)?;

        Ok(())
    }

    fn stamp_origin_blocks(&self) -> Result<()> {
        let origin_dev_size =
            self.nr_origin_blocks * self.cache_block_size as u64 + self.origin_dev_offset;
        let file = create_sized_file(&self.origin_dev, origin_dev_size)?;

        let mut stamper =
            Stamper::new(file, self.seed, self.cache_block_size).offset(self.origin_dev_offset);
        visit_blocks(self.nr_origin_blocks, &mut stamper)?;

        Ok(())
    }

    fn writeback(&self, update_metadata: bool) -> Result<()> {
        self.writeback_(update_metadata, true)
    }

    fn writeback_fail(&self, update_metadata: bool) -> Result<()> {
        self.writeback_(update_metadata, false)
    }

    fn writeback_(&self, update_metadata: bool, expect_ok: bool) -> Result<()> {
        use std::ffi::OsStr;

        let mut args = args![
            "--metadata-device",
            &self.metadata_dev,
            "--fast-device",
            &self.fast_dev,
            "--origin-device",
            &self.origin_dev
        ]
        .to_vec();

        if !update_metadata {
            args.push(OsStr::new("--no-metadata-update"));
        }

        let fast_dev_offset = (self.fast_dev_offset >> SECTOR_SHIFT).to_string();
        if self.fast_dev_offset > 0 {
            args.push(OsStr::new("--fast-device-offset"));
            args.push(OsStr::new(&fast_dev_offset));
        }

        let origin_dev_offset = (self.origin_dev_offset >> SECTOR_SHIFT).to_string();
        if self.origin_dev_offset > 0 {
            args.push(OsStr::new("--origin-device-offset"));
            args.push(OsStr::new(&origin_dev_offset));
        }

        if expect_ok {
            run_ok(cache_writeback_cmd(args))?;
        } else {
            run_fail(cache_writeback_cmd(args))?;
        }

        Ok(())
    }

    // verify the origin device
    fn verify(&self, indicator: Box<dyn SourceBlockIndicator>) -> Result<()> {
        let file = OpenOptions::new()
            .read(true)
            .write(false)
            .custom_flags(libc::O_EXCL)
            .open(&self.origin_dev)?;

        let mut verifier =
            WritebackVerifier::new(file, self.cache_block_size, self.seed, indicator)?
                .offset(self.origin_dev_offset);
        visit_blocks(self.nr_origin_blocks, &mut verifier)?;

        Ok(())
    }
}

//------------------------------------------

mod format1 {
    use super::*;

    struct RmapInner {
        rmap: BTreeMap<u64, u32>,
        dirty_bits: FixedBitSet,
    }

    struct RmapCollector {
        inner: Mutex<RmapInner>,
    }

    impl RmapCollector {
        fn new(nr_cache_blocks: u32) -> Self {
            let inner = RmapInner {
                rmap: BTreeMap::new(),
                dirty_bits: FixedBitSet::with_capacity(nr_cache_blocks as usize),
            };

            Self {
                inner: Mutex::new(inner),
            }
        }

        fn complete(self) -> Result<(BTreeMap<u64, u32>, FixedBitSet)> {
            let inner = self.inner.into_inner()?;
            Ok((inner.rmap, inner.dirty_bits))
        }
    }

    impl ArrayVisitor<Mapping> for RmapCollector {
        fn visit(&self, index: u64, b: ArrayBlock<Mapping>) -> array::Result<()> {
            let mut inner = self.inner.lock().unwrap();

            let cbegin = index as u32 * b.header.max_entries;
            let cend = cbegin + b.header.nr_entries;
            for (map, cblock) in b.values.iter().zip(cbegin..cend) {
                if !map.is_valid() {
                    continue;
                }

                if inner.rmap.insert(map.oblock, cblock).is_some() {
                    return Err(array::value_err("duplicate mappings".to_string()));
                }

                if map.is_dirty() {
                    inner.dirty_bits.insert(cblock as usize);
                }
            }

            Ok(())
        }
    }

    fn read_rmap_and_dirty_bits(
        metadata_dev: &Path,
        nr_cache_blocks: u32,
    ) -> Result<(BTreeMap<u64, u32>, FixedBitSet)> {
        let engine = Arc::new(SyncIoEngine::new(metadata_dev, true)?);
        let sb = read_superblock(engine.as_ref(), SUPERBLOCK_LOCATION)?;

        let v = RmapCollector::new(nr_cache_blocks);
        let w = ArrayWalker::new(engine, false);
        w.walk(&v, sb.mapping_root)?;

        v.complete()
    }

    #[test]
    fn writeback_only_dirty() -> Result<()> {
        let cache_block_size: usize = 32768; // 32 KiB
        let nr_cache_blocks: u32 = 1024;
        let nr_origin_blocks: u64 = 4096;

        let mut t = WritebackTest::new()?;
        t.format_metadata(cache_block_size, nr_cache_blocks, nr_origin_blocks, 1)?;
        t.stamp_cache_blocks()?;
        t.stamp_origin_blocks()?;
        t.writeback(false)?;

        let (rmap, dirty_bits) = read_rmap_and_dirty_bits(&t.metadata_dev, t.nr_cache_blocks)?;
        let indicator = Box::new(DirtySourceIndicator::new(
            rmap,
            dirty_bits,
            t.nr_origin_blocks,
        ));
        t.verify(indicator)?;

        Ok(())
    }

    #[test]
    fn writeback_unclean_shutdown() -> Result<()> {
        let cache_block_size: usize = 32768; // 32 KiB
        let nr_cache_blocks: u32 = 1024;
        let nr_origin_blocks: u64 = 4096;

        let mut t = WritebackTest::new()?;
        t.format_metadata(cache_block_size, nr_cache_blocks, nr_origin_blocks, 1)?;
        t.set_unclean_shutdown()?;
        t.stamp_cache_blocks()?;
        t.stamp_origin_blocks()?;
        t.writeback(false)?;

        let (rmap, _dirty_bits) =
            format1::read_rmap_and_dirty_bits(&t.metadata_dev, t.nr_cache_blocks)?;
        let indicator = Box::new(WritebackAllIndicator::new(rmap));
        t.verify(indicator)?;

        Ok(())
    }

    #[test]
    fn writeback_with_offsets() -> Result<()> {
        let cache_block_size: usize = 32768; // 32 KiB
        let nr_cache_blocks: u32 = 1024;
        let nr_origin_blocks: u64 = 4096;

        let mut t = WritebackTest::new()?;
        t.format_metadata(cache_block_size, nr_cache_blocks, nr_origin_blocks, 1)?;
        t.set_fast_dev_offset(1048576)?;
        t.set_origin_dev_offset(4194304)?;
        t.stamp_cache_blocks()?;
        t.stamp_origin_blocks()?;
        t.writeback(false)?;

        let (rmap, dirty_bits) =
            format1::read_rmap_and_dirty_bits(&t.metadata_dev, t.nr_cache_blocks)?;
        let indicator = Box::new(DirtySourceIndicator::new(
            rmap,
            dirty_bits,
            t.nr_origin_blocks,
        ));
        t.verify(indicator)?;

        Ok(())
    }

    #[test]
    fn update_metadata() -> Result<()> {
        let cache_block_size: usize = 32768; // 32 KiB
        let nr_cache_blocks: u32 = 1024;
        let nr_origin_blocks: u64 = 4096;

        let mut t = WritebackTest::new()?;
        t.format_metadata(cache_block_size, nr_cache_blocks, nr_origin_blocks, 1)?;
        let (_, dirty_bits) =
            format1::read_rmap_and_dirty_bits(&t.metadata_dev, t.nr_cache_blocks)?;

        t.stamp_cache_blocks()?;
        t.stamp_origin_blocks()?;
        t.writeback(true)?;

        // ensure the mappings are not affected by metadata update
        let (rmap, dirty_bits_upd) =
            format1::read_rmap_and_dirty_bits(&t.metadata_dev, t.nr_cache_blocks)?;
        let indicator = Box::new(DirtySourceIndicator::new(
            rmap,
            dirty_bits,
            t.nr_origin_blocks,
        ));
        t.verify(indicator)?;

        // ensure dirty bits are cleared
        for v in dirty_bits_upd.as_slice() {
            if *v != 0 {
                return Err(anyhow!("dirty bitset is not cleared"));
            }
        }

        Ok(())
    }

    #[test]
    fn no_dirty_blocks() -> Result<()> {
        let cache_block_size: usize = 32768; // 32 KiB
        let nr_cache_blocks: u32 = 1024;
        let nr_origin_blocks: u64 = 4096;

        let mut t = WritebackTest::new()?;
        t.format_metadata_with(
            cache_block_size,
            nr_cache_blocks,
            nr_origin_blocks,
            1,
            80,
            0,
        )?;
        t.stamp_cache_blocks()?;
        t.stamp_origin_blocks()?;
        t.writeback(false)?;

        let (rmap, dirty_bits) =
            format1::read_rmap_and_dirty_bits(&t.metadata_dev, t.nr_cache_blocks)?;
        let indicator = Box::new(DirtySourceIndicator::new(
            rmap,
            dirty_bits,
            t.nr_origin_blocks,
        ));
        t.verify(indicator)?;

        Ok(())
    }

    #[test]
    fn disallow_corrupted_metadata() -> Result<()> {
        let cache_block_size: usize = 32768; // 32 KiB
        let nr_cache_blocks: u32 = 1024;
        let nr_origin_blocks: u64 = 4096;

        let mut t = WritebackTest::new()?;
        t.format_metadata(cache_block_size, nr_cache_blocks, nr_origin_blocks, 1)?;
        t.stamp_cache_blocks()?;
        t.stamp_origin_blocks()?;
        t.damage_metadata()?;
        t.writeback_fail(true)?;
        t.verify(Box::new(NoopIndicator))?;

        Ok(())
    }
}

//------------------------------------------

mod format2 {
    use super::*;

    struct RmapInner {
        rmap: BTreeMap<u64, u32>,
    }

    struct RmapCollector {
        inner: Mutex<RmapInner>,
    }

    impl RmapCollector {
        fn new() -> Self {
            let inner = RmapInner {
                rmap: BTreeMap::new(),
            };

            Self {
                inner: Mutex::new(inner),
            }
        }

        fn complete(self) -> Result<BTreeMap<u64, u32>> {
            let inner = self.inner.into_inner()?;
            Ok(inner.rmap)
        }
    }

    impl ArrayVisitor<Mapping> for RmapCollector {
        fn visit(&self, index: u64, b: ArrayBlock<Mapping>) -> array::Result<()> {
            let mut inner = self.inner.lock().unwrap();

            let cbegin = index as u32 * b.header.max_entries;
            let cend = cbegin + b.header.nr_entries;
            for (map, cblock) in b.values.iter().zip(cbegin..cend) {
                if !map.is_valid() {
                    continue;
                }

                if inner.rmap.insert(map.oblock, cblock).is_some() {
                    return Err(array::value_err("duplicate mappings".to_string()));
                }
            }

            Ok(())
        }
    }

    fn read_rmap(metadata_dev: &Path) -> Result<BTreeMap<u64, u32>> {
        let engine = Arc::new(SyncIoEngine::new(metadata_dev, true)?);
        let sb = read_superblock(engine.as_ref(), SUPERBLOCK_LOCATION)?;

        let v = RmapCollector::new();
        let w = ArrayWalker::new(engine, false);
        w.walk(&v, sb.mapping_root)?;

        v.complete()
    }

    fn read_dirty_bits(metadata_dev: &Path, nr_cache_blocks: u32) -> Result<FixedBitSet> {
        let engine = Arc::new(SyncIoEngine::new(metadata_dev, true)?);
        let sb = read_superblock(engine.as_ref(), SUPERBLOCK_LOCATION)?;

        let dirty_bits = read_bitset(
            engine,
            sb.dirty_root.unwrap(),
            nr_cache_blocks as usize,
            false,
        )?;
        Ok(dirty_bits)
    }

    #[test]
    fn writeback_only_dirty() -> Result<()> {
        let cache_block_size: usize = 32768; // 32 KiB
        let nr_cache_blocks: u32 = 1024;
        let nr_origin_blocks: u64 = 4096;

        let mut t = WritebackTest::new()?;
        t.format_metadata(cache_block_size, nr_cache_blocks, nr_origin_blocks, 2)?;
        t.stamp_cache_blocks()?;
        t.stamp_origin_blocks()?;
        t.writeback(false)?;

        let rmap = format2::read_rmap(&t.metadata_dev)?;
        let dirty_bits = format2::read_dirty_bits(&t.metadata_dev, t.nr_cache_blocks)?;
        let indicator = Box::new(DirtySourceIndicator::new(
            rmap,
            dirty_bits,
            t.nr_origin_blocks,
        ));
        t.verify(indicator)?;

        Ok(())
    }

    #[test]
    fn writeback_unclean_shutdown() -> Result<()> {
        let cache_block_size: usize = 32768; // 32 KiB
        let nr_cache_blocks: u32 = 1024;
        let nr_origin_blocks: u64 = 4096;

        let mut t = WritebackTest::new()?;
        t.format_metadata(cache_block_size, nr_cache_blocks, nr_origin_blocks, 2)?;
        t.set_unclean_shutdown()?;
        t.stamp_cache_blocks()?;
        t.stamp_origin_blocks()?;
        t.writeback(false)?;

        let rmap = format2::read_rmap(&t.metadata_dev)?;
        let indicator = Box::new(WritebackAllIndicator::new(rmap));
        t.verify(indicator)?;

        Ok(())
    }

    #[test]
    fn writeback_with_offsets() -> Result<()> {
        let cache_block_size: usize = 32768; // 32 KiB
        let nr_cache_blocks: u32 = 1024;
        let nr_origin_blocks: u64 = 4096;

        let mut t = WritebackTest::new()?;
        t.format_metadata(cache_block_size, nr_cache_blocks, nr_origin_blocks, 2)?;
        t.set_fast_dev_offset(1048576)?;
        t.set_origin_dev_offset(4194304)?;
        t.stamp_cache_blocks()?;
        t.stamp_origin_blocks()?;
        t.writeback(false)?;

        let rmap = format2::read_rmap(&t.metadata_dev)?;
        let dirty_bits = format2::read_dirty_bits(&t.metadata_dev, t.nr_cache_blocks)?;
        let indicator = Box::new(DirtySourceIndicator::new(
            rmap,
            dirty_bits,
            t.nr_origin_blocks,
        ));
        t.verify(indicator)?;

        Ok(())
    }

    #[test]
    fn update_metadata() -> Result<()> {
        let cache_block_size: usize = 32768; // 32 KiB
        let nr_cache_blocks: u32 = 1024;
        let nr_origin_blocks: u64 = 4096;

        let mut t = WritebackTest::new()?;
        t.format_metadata(cache_block_size, nr_cache_blocks, nr_origin_blocks, 2)?;
        let dirty_bits = format2::read_dirty_bits(&t.metadata_dev, t.nr_cache_blocks)?;

        t.stamp_cache_blocks()?;
        t.stamp_origin_blocks()?;
        t.writeback(true)?;

        // Do verification using the updated metadata, to ensure that
        // the mappings are not affected by metadata update.
        let rmap = format2::read_rmap(&t.metadata_dev)?;
        let indicator = Box::new(DirtySourceIndicator::new(
            rmap,
            dirty_bits,
            t.nr_origin_blocks,
        ));
        t.verify(indicator)?;

        // Do verification using the updated metadata, to ensure that
        // the mappings are not affected by metadata update.
        let dirty_bits_upd = format2::read_dirty_bits(&t.metadata_dev, t.nr_cache_blocks)?;
        for v in dirty_bits_upd.as_slice() {
            if *v != 0 {
                return Err(anyhow!("dirty bitset is not cleared"));
            }
        }

        Ok(())
    }

    #[test]
    fn no_dirty_blocks() -> Result<()> {
        let cache_block_size: usize = 32768; // 32 KiB
        let nr_cache_blocks: u32 = 1024;
        let nr_origin_blocks: u64 = 4096;

        let mut t = WritebackTest::new()?;
        t.format_metadata_with(
            cache_block_size,
            nr_cache_blocks,
            nr_origin_blocks,
            2,
            80,
            0,
        )?;
        t.stamp_cache_blocks()?;
        t.stamp_origin_blocks()?;
        t.writeback(false)?;

        let rmap = format2::read_rmap(&t.metadata_dev)?;
        let dirty_bits = format2::read_dirty_bits(&t.metadata_dev, t.nr_cache_blocks)?;
        let indicator = Box::new(DirtySourceIndicator::new(
            rmap,
            dirty_bits,
            t.nr_origin_blocks,
        ));
        t.verify(indicator)?;

        Ok(())
    }

    #[test]
    fn disallow_corrupted_metadata() -> Result<()> {
        let cache_block_size: usize = 32768; // 32 KiB
        let nr_cache_blocks: u32 = 1024;
        let nr_origin_blocks: u64 = 4096;

        let mut t = WritebackTest::new()?;
        t.format_metadata(cache_block_size, nr_cache_blocks, nr_origin_blocks, 2)?;
        t.stamp_cache_blocks()?;
        t.stamp_origin_blocks()?;
        t.damage_metadata()?;
        t.writeback_fail(true)?;
        t.verify(Box::new(NoopIndicator))?;

        Ok(())
    }
}

//------------------------------------------
