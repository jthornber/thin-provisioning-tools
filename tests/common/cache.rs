use anyhow::Result;
use std::path::{Path, PathBuf};
use thinp::cache::metadata_generator::CacheGenerator;
use thinp::file_utils;

use thinp::io_engine::SyncIoEngine;

use crate::args;
use crate::common::cache_xml_generator::write_xml;
use crate::common::process::*;
use crate::common::target::*;
use crate::common::test_dir::TestDir;

//-----------------------------------------------

pub fn mk_valid_xml(td: &mut TestDir) -> Result<PathBuf> {
    let xml = td.mk_path("meta.xml");
    // bs, cblocks, oblocks, res, dirty, version, hotspot_size
    let mut gen = CacheGenerator::new(512, 128, 1024, 80, 50, 2, 16);
    write_xml(&xml, &mut gen)?;
    Ok(xml)
}

pub fn mk_valid_md(td: &mut TestDir) -> Result<PathBuf> {
    let xml = td.mk_path("meta.xml");
    let md = td.mk_path("meta.bin");

    let mut gen = CacheGenerator::new(512, 4096, 32768, 80, 50, 2, 16);
    write_xml(&xml, &mut gen)?;

    let _file = file_utils::create_sized_file(&md, 4096 * 4096);
    run_ok(cache_restore_cmd(args!["-i", &xml, "-o", &md]))?;

    Ok(md)
}

//-----------------------------------------------

pub fn generate_metadata_leaks(
    md: &Path,
    nr_blocks: u64,
    expected: u32,
    actual: u32,
) -> Result<()> {
    let nr_blocks_str = nr_blocks.to_string();
    let expected_str = expected.to_string();
    let actual_str = actual.to_string();
    let args = args![
        "-o",
        &md,
        "--create-metadata-leaks",
        "--nr-blocks",
        &nr_blocks_str,
        "--expected",
        &expected_str,
        "--actual",
        &actual_str
    ];
    run_ok(cache_generate_damage_cmd(args))?;

    Ok(())
}

//-----------------------------------------------

pub fn get_clean_shutdown(md: &Path) -> Result<bool> {
    use thinp::cache::superblock::*;

    let engine = SyncIoEngine::new(md, false)?;
    let sb = read_superblock(&engine, SUPERBLOCK_LOCATION)?;
    Ok(sb.flags.clean_shutdown)
}

pub fn get_needs_check(md: &Path) -> Result<bool> {
    use thinp::cache::superblock::*;

    let engine = SyncIoEngine::new(md, false)?;
    let sb = read_superblock(&engine, SUPERBLOCK_LOCATION)?;
    Ok(sb.flags.needs_check)
}

pub fn unset_clean_shutdown(md: &Path) -> Result<()> {
    let args = args!["-o", &md, "--set-clean-shutdown=false"];
    run_ok(cache_generate_metadata_cmd(args))?;
    Ok(())
}

pub fn set_needs_check(md: &Path) -> Result<()> {
    let args = args!["-o", &md, "--set-needs-check"];
    run_ok(cache_generate_metadata_cmd(args))?;
    Ok(())
}

//-----------------------------------------------

// Invalidates up to `nr` mappings whose dirty bit is set, leaving the dirty
// bitset untouched. This mimics the kernel demoting blocks after the dirty
// bitset was last written: a set dirty bit on an unmapped cache block.
// Returns the number of mappings invalidated. Format 2 metadata only.
pub fn unmap_dirty_blocks(md: &Path, nr: usize) -> Result<usize> {
    use std::io::Cursor;
    use std::sync::Mutex;
    use thinp::cache::mapping::Mapping;
    use thinp::cache::superblock::*;
    use thinp::checksum::{write_checksum, BT};
    use thinp::io_engine::{Block, IoEngine};
    use thinp::pdata::array::{self, pack_array_block, ArrayBlock};
    use thinp::pdata::array_walker::{ArrayVisitor, ArrayWalker};
    use thinp::pdata::bitset::read_bitset_checked;

    struct Collector {
        blocks: Mutex<Vec<(u64, ArrayBlock<Mapping>)>>,
    }

    impl ArrayVisitor<Mapping> for Collector {
        fn visit(&self, index: u64, b: ArrayBlock<Mapping>) -> array::Result<()> {
            self.blocks.lock().unwrap().push((index, b));
            Ok(())
        }
    }

    let engine = SyncIoEngine::new(md, true)?;
    let sb = read_superblock(&engine, SUPERBLOCK_LOCATION)?;
    let dirty_root = sb
        .dirty_root
        .ok_or_else(|| anyhow::anyhow!("no dirty bitset"))?;
    let (dirty_bits, err) =
        read_bitset_checked(&engine, dirty_root, sb.cache_blocks as usize, false);
    if let Some(e) = err {
        return Err(anyhow::anyhow!("failed to read dirty bitset: {}", e));
    }

    let collector = Collector {
        blocks: Mutex::new(Vec::new()),
    };
    ArrayWalker::new(&engine, false).walk(&collector, sb.mapping_root)?;

    let mut count = 0;
    for (index, mut ablock) in collector.blocks.into_inner().unwrap() {
        let cbegin = index * ablock.header.max_entries as u64;
        let mut modified = false;
        for (i, m) in ablock.values.iter_mut().enumerate() {
            if count == nr {
                break;
            }
            let cblock = (cbegin + i as u64) as usize;
            if m.is_valid() && dirty_bits.contains(cblock) == Some(true) {
                *m = Mapping {
                    oblock: 0,
                    flags: 0,
                };
                count += 1;
                modified = true;
            }
        }
        if modified {
            let b = Block::zeroed(ablock.header.blocknr);
            pack_array_block(&ablock, &mut Cursor::new(b.get_data()))?;
            write_checksum(b.get_data(), BT::ARRAY)?;
            engine.write(&b)?;
        }
    }

    Ok(count)
}

//-----------------------------------------------
