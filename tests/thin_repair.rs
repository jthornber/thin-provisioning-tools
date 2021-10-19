use anyhow::Result;

mod common;

use common::common_args::*;
use common::fixture::*;
use common::input_arg::*;
use common::output_option::*;
use common::process::*;
use common::program::*;
use common::target::*;
use common::test_dir::*;
use common::thin::*;

//------------------------------------------

const USAGE: &str = "Usage: thin_repair [options] {device|file}\n\
                     Options:\n  \
                       {-h|--help}\n  \
                       {-i|--input} <input metadata (binary format)>\n  \
                       {-o|--output} <output metadata (binary format)>\n  \
                       {--transaction-id} <natural>\n  \
                       {--data-block-size} <natural>\n  \
                       {--nr-data-blocks} <natural>\n  \
                       {-V|--version}";

//-----------------------------------------

struct ThinRepair;

impl<'a> Program<'a> for ThinRepair {
    fn name() -> &'a str {
        "thin_repair"
    }

    fn cmd<I>(args: I) -> Command
    where
        I: IntoIterator,
        I::Item: Into<std::ffi::OsString>,
    {
        thin_repair_cmd(args)
    }

    fn usage() -> &'a str {
        USAGE
    }

    fn arg_type() -> ArgType {
        ArgType::IoOptions
    }

    fn bad_option_hint(option: &str) -> String {
        msg::bad_option_hint(option)
    }
}

impl<'a> InputProgram<'a> for ThinRepair {
    fn mk_valid_input(td: &mut TestDir) -> Result<std::path::PathBuf> {
        mk_valid_md(td)
    }

    fn file_not_found() -> &'a str {
        msg::FILE_NOT_FOUND
    }

    fn missing_input_arg() -> &'a str {
        msg::MISSING_INPUT_ARG
    }

    #[cfg(not(feature = "rust_tests"))]
    fn corrupted_input() -> &'a str {
        "The following field needs to be provided on the command line due to corruption in the superblock"
    }

    #[cfg(feature = "rust_tests")]
    fn corrupted_input() -> &'a str {
        "data block size needs to be provided due to corruption in the superblock"
    }
}

impl<'a> OutputProgram<'a> for ThinRepair {
    fn missing_output_arg() -> &'a str {
        msg::MISSING_OUTPUT_ARG
    }
}

impl<'a> MetadataWriter<'a> for ThinRepair {
    fn file_not_found() -> &'a str {
        msg::FILE_NOT_FOUND
    }
}

//-----------------------------------------

test_accepts_help!(ThinRepair);
test_accepts_version!(ThinRepair);
test_rejects_bad_option!(ThinRepair);

test_input_file_not_found!(ThinRepair);
test_input_cannot_be_a_directory!(ThinRepair);
test_corrupted_input_data!(ThinRepair);

test_missing_output_option!(ThinRepair);

//-----------------------------------------
// test output to a small file

// TODO: share with thin_restore

#[test]
fn dont_repair_xml() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_zeroed_md(&mut td)?;
    let xml = mk_valid_xml(&mut td)?;
    run_fail(thin_repair_cmd(args!["-i", &xml, "-o", &md]))?;
    Ok(())
}

//-----------------------------------------

// TODO: share with thin_dump

#[cfg(not(feature = "rust_tests"))]
fn override_thing(flag: &str, val: &str, pattern: &str) -> Result<()> {
    let mut td = TestDir::new()?;
    let md1 = mk_valid_md(&mut td)?;
    let md2 = mk_zeroed_md(&mut td)?;
    let output = run_ok_raw(thin_repair_cmd(args![flag, val, "-i", &md1, "-o", &md2]))?;
    assert_eq!(output.stderr.len(), 0);
    let output = run_ok(thin_dump_cmd(args![&md2]))?;
    assert!(output.contains(pattern));
    Ok(())
}

#[test]
#[cfg(not(feature = "rust_tests"))]
fn override_transaction_id() -> Result<()> {
    override_thing("--transaction-id", "2345", "transaction=\"2345\"")
}

#[test]
#[cfg(not(feature = "rust_tests"))]
fn override_data_block_size() -> Result<()> {
    override_thing("--data-block-size", "8192", "data_block_size=\"8192\"")
}

#[test]
#[cfg(not(feature = "rust_tests"))]
fn override_nr_data_blocks() -> Result<()> {
    override_thing("--nr-data-blocks", "234500", "nr_data_blocks=\"234500\"")
}

// FIXME: that's repair_superblock in thin_dump.rs
#[test]
fn superblock_succeeds() -> Result<()> {
    let mut td = TestDir::new()?;
    let md1 = mk_valid_md(&mut td)?;
    let original = run_ok_raw(thin_dump_cmd(args![&md1]))?;
    if !cfg!(feature = "rust_tests") {
        assert_eq!(original.stderr.len(), 0);
    }
    damage_superblock(&md1)?;
    let md2 = mk_zeroed_md(&mut td)?;
    run_ok(thin_repair_cmd(args![
        "--transaction-id=1",
        "--data-block-size=128",
        "--nr-data-blocks=20480",
        "-i",
        &md1,
        "-o",
        &md2
    ]))?;
    let repaired = run_ok_raw(thin_dump_cmd(args![&md2]))?;
    if !cfg!(feature = "rust_tests") {
        assert_eq!(repaired.stderr.len(), 0);
    }
    assert_eq!(original.stdout, repaired.stdout);
    Ok(())
}

//-----------------------------------------

// TODO: share with thin_dump

fn missing_thing(flag1: &str, flag2: &str, pattern: &str) -> Result<()> {
    let mut td = TestDir::new()?;
    let md1 = mk_valid_md(&mut td)?;
    damage_superblock(&md1)?;
    let md2 = mk_zeroed_md(&mut td)?;
    let stderr = run_fail(thin_repair_cmd(args![flag1, flag2, "-i", &md1, "-o", &md2]))?;
    assert!(stderr.contains(pattern));
    Ok(())
}

#[test]
#[cfg(not(feature = "rust_tests"))]
fn missing_transaction_id() -> Result<()> {
    missing_thing(
        "--data-block-size=128",
        "--nr-data-blocks=20480",
        "transaction id",
    )
}

#[test]
fn missing_data_block_size() -> Result<()> {
    missing_thing(
        "--transaction-id=1",
        "--nr-data-blocks=20480",
        "data block size",
    )
}

#[test]
#[cfg(not(feature = "rust_tests"))]
fn missing_nr_data_blocks() -> Result<()> {
    missing_thing(
        "--transaction-id=1",
        "--data-block-size=128",
        "nr data blocks",
    )
}

//-----------------------------------------
