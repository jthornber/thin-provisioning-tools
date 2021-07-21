use anyhow::Result;
use std::path::PathBuf;

use thinp::file_utils;
use thinp::io_engine::*;

use crate::args;
use crate::common::fixture::*;
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
    let args = args!["-i", &xml, "-o", &md];
    run_ok(THIN_RESTORE, &args)?;

    Ok(md)
}

//-----------------------------------------------

// FIXME: replace mk_valid_md with this?
pub fn prep_metadata(td: &mut TestDir) -> Result<PathBuf> {
    let md = mk_zeroed_md(td)?;
    let args = args!["-o", &md, "--format", "--nr-data-blocks", "102400"];
    run_ok(THIN_GENERATE_METADATA, &args)?;

    // Create a 2GB device
    let args = args!["-o", &md, "--create-thin", "1"];
    run_ok(THIN_GENERATE_METADATA, &args)?;
    let args = args![
        "-o",
        &md,
        "--dev-id",
        "1",
        "--size",
        "2097152",
        "--rw=randwrite",
        "--seq-nr=16"
    ];
    run_ok(THIN_GENERATE_MAPPINGS, &args)?;

    // Take a few snapshots.
    let mut snap_id = 2;
    for _i in 0..10 {
        // take a snapshot
        let snap_id_str = snap_id.to_string();
        let args = args!["-o", &md, "--create-snap", &snap_id_str, "--origin", "1"];
        run_ok(THIN_GENERATE_METADATA, &args)?;

        // partially overwrite the origin (64MB)
        let args = args![
            "-o",
            &md,
            "--dev-id",
            "1",
            "--size",
            "2097152",
            "--io-size",
            "131072",
            "--rw=randwrite",
            "--seq-nr=16"
        ];
        run_ok(THIN_GENERATE_MAPPINGS, &args)?;
        snap_id += 1;
    }

    Ok(md)
}

pub fn set_needs_check(md: &PathBuf) -> Result<()> {
    let args = args!["-o", &md, "--set-needs-check"];
    run_ok(THIN_GENERATE_METADATA, &args)?;
    Ok(())
}

pub fn generate_metadata_leaks(
    md: &PathBuf,
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
    run_ok(THIN_GENERATE_DAMAGE, &args)?;

    Ok(())
}

pub fn get_needs_check(md: &PathBuf) -> Result<bool> {
    use thinp::thin::superblock::*;

    let engine = SyncIoEngine::new(&md, 1, false)?;
    let sb = read_superblock(&engine, SUPERBLOCK_LOCATION)?;
    Ok(sb.flags.needs_check)
}

//-----------------------------------------------
