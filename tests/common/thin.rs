use anyhow::Result;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use thinp::file_utils;
use thinp::io_engine::*;
use thinp::pdata::btree_walker::btree_to_map;
use thinp::thin::device_detail::DeviceDetail;

use crate::args;
use crate::common::process::*;
use crate::common::target::*;
use crate::common::test_dir::TestDir;
use crate::common::thin_xml_generator::{write_xml, SingleThinS};

//-----------------------------------------------

pub fn mk_valid_xml(td: &mut TestDir) -> Result<PathBuf> {
    let xml = td.mk_path("meta.xml");
    let mut gen = SingleThinS::new(0, 1024, 2048, 2048);
    write_xml(&xml, &mut gen)?;
    Ok(xml)
}

pub fn mk_valid_md(td: &mut TestDir) -> Result<PathBuf> {
    let xml = td.mk_path("meta.xml");
    let md = td.mk_path("meta.bin");

    let mut gen = SingleThinS::new(0, 1024, 20480, 20480);
    write_xml(&xml, &mut gen)?;

    let _file = file_utils::create_sized_file(&md, 4096 * 4096);
    run_ok(thin_restore_cmd(args!["-i", &xml, "-o", &md]))?;

    Ok(md)
}

//-----------------------------------------------

pub enum TestData {
    PackedMetadata,
    PackedMetadataWithMetadataSnap,
}

pub fn path_to(t: TestData) -> Result<PathBuf> {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("tests/testdata");
    match t {
        TestData::PackedMetadata => path.push("tmeta.pack"),
        TestData::PackedMetadataWithMetadataSnap => path.push("tmeta_with_metadata_snap.pack"),
    }
    Ok(path)
}

fn unpack_metadata(input: &Path, output: &Path) -> Result<()> {
    let args = args!["-i", input, "-o", output];
    run_ok(thin_metadata_unpack_cmd(args))?;
    Ok(())
}

// FIXME: replace mk_valid_md with this?
pub fn prep_metadata(td: &mut TestDir) -> Result<PathBuf> {
    let input = path_to(TestData::PackedMetadata)?;
    let output = td.mk_path("tmeta.bin");
    unpack_metadata(&input, &output)?;
    Ok(output)
}

pub fn prep_metadata_with_metadata_snap(td: &mut TestDir) -> Result<PathBuf> {
    let input = path_to(TestData::PackedMetadataWithMetadataSnap)?;
    let output = td.mk_path("tmeta.bin");
    unpack_metadata(&input, &output)?;
    Ok(output)
}

pub fn prep_metadata_from_file(td: &mut TestDir, filename: &str) -> Result<PathBuf> {
    let mut input = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    input.push("tests/testdata");
    input.push(filename);
    let output = td.mk_path("tmeta.bin");
    unpack_metadata(&input, &output)?;
    Ok(output)
}

// Sometimes we need a rebuilt metadata in order to produce binary identical metadata
// between dump-restore cycles.
pub fn prep_rebuilt_metadata(td: &mut TestDir) -> Result<PathBuf> {
    let input = path_to(TestData::PackedMetadata)?;
    let unpacked = td.mk_path("unpacked.bin");
    unpack_metadata(&input, &unpacked)?;

    let rebuilt = td.mk_path("rebuilt.bin");
    let _file = file_utils::create_sized_file(&rebuilt, file_utils::file_size(&unpacked)?)?;
    let args = args!["-i", &unpacked, "-o", &rebuilt];
    run_ok(thin_repair_cmd(args))?;

    Ok(rebuilt)
}

pub fn set_needs_check(md: &Path) -> Result<()> {
    let args = args!["-o", &md, "--set-needs-check"];
    run_ok(thin_generate_metadata_cmd(args))?;
    Ok(())
}

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
    run_ok(thin_generate_damage_cmd(args))?;

    Ok(())
}

pub fn get_superblock(md: &Path) -> Result<thinp::thin::superblock::Superblock> {
    use thinp::thin::superblock::*;

    let engine = SyncIoEngine::new(md, false)?;
    read_superblock(&engine, SUPERBLOCK_LOCATION)
}

pub fn get_needs_check(md: &Path) -> Result<bool> {
    use thinp::thin::superblock::*;

    let engine = SyncIoEngine::new(md, false)?;
    let sb = read_superblock(&engine, SUPERBLOCK_LOCATION)?;
    Ok(sb.flags.needs_check)
}

pub fn get_metadata_usage(md: &Path) -> Result<(u64, u64)> {
    use thinp::pdata::space_map::common::SMRoot;
    use thinp::pdata::unpack::unpack;
    use thinp::thin::superblock::*;

    let engine = SyncIoEngine::new(md, false)?;
    let sb = read_superblock(&engine, SUPERBLOCK_LOCATION)?;
    let root = unpack::<SMRoot>(&sb.metadata_sm_root)?;
    Ok((root.nr_blocks, root.nr_allocated))
}

pub fn get_data_usage(md: &Path) -> Result<(u64, u64)> {
    use thinp::pdata::space_map::common::SMRoot;
    use thinp::pdata::unpack::unpack;
    use thinp::thin::superblock::*;

    let engine = SyncIoEngine::new(md, false)?;
    let sb = read_superblock(&engine, SUPERBLOCK_LOCATION)?;
    let root = unpack::<SMRoot>(&sb.data_sm_root)?;
    Ok((root.nr_blocks, root.nr_allocated))
}

// FIXME: duplicates of thin::check::get_thins_from_superblock()
pub fn get_thins(md: &Path) -> Result<BTreeMap<u64, (u64, DeviceDetail)>> {
    use thinp::thin::superblock::*;

    let engine: Arc<dyn IoEngine + Send + Sync> = Arc::new(SyncIoEngine::new(md, false)?);
    let sb = read_superblock(engine.as_ref(), SUPERBLOCK_LOCATION)?;

    let devs =
        btree_to_map::<DeviceDetail>(&mut Vec::new(), engine.clone(), false, sb.details_root)?;

    let roots = btree_to_map::<u64>(&mut Vec::new(), engine, false, sb.mapping_root)?;

    let thins = roots
        .into_iter()
        .zip(devs.into_values())
        .map(|((id, root), details)| (id, (root, details)))
        .collect();
    Ok(thins)
}
//-----------------------------------------------
