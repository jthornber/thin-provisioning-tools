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

use thinp::tools_version;

//------------------------------------------

const USAGE: &str = concat!(
    "thin_repair ",
    tools_version!(),
    "
Repair thin-provisioning metadata, and write it to different device or file

USAGE:
    thin_repair [OPTIONS] --input <FILE> --output <FILE>

OPTIONS:
        --data-block-size <SECTORS>    Provide the data block size for repairing
    -h, --help                         Print help information
    -i, --input <FILE>                 Specify the input device
        --nr-data-blocks <NUM>         Override the number of data blocks if needed
    -o, --output <FILE>                Specify the output device
    -q, --quiet                        Suppress output messages, return only exit code.
        --transaction-id <NUM>         Override the transaction id if needed
    -V, --version                      Print version information"
);

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

    fn corrupted_input() -> &'a str {
        "no compatible roots found"
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

test_readonly_input_file!(ThinRepair);

test_missing_output_option!(ThinRepair);

//-----------------------------------------
// accepts empty argument

#[test]
fn accepts_empty_argument() -> Result<()> {
    let mut td = TestDir::new()?;
    let input = mk_valid_md(&mut td)?;
    let output = mk_zeroed_md(&mut td)?;
    run_ok(thin_repair_cmd(args!["-i", &input, "-o", &output, ""]))?;
    Ok(())
}

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

fn override_thing(flag: &str, val: &str, pattern: &str) -> Result<()> {
    let mut td = TestDir::new()?;
    let md1 = mk_valid_md(&mut td)?;
    let md2 = mk_zeroed_md(&mut td)?;
    run_ok(thin_repair_cmd(args![flag, val, "-i", &md1, "-o", &md2]))?;
    let output = run_ok(thin_dump_cmd(args![&md2]))?;
    assert!(output.contains(pattern));
    Ok(())
}

#[test]
fn override_transaction_id() -> Result<()> {
    override_thing("--transaction-id", "2345", "transaction=\"2345\"")
}

#[test]
fn override_data_block_size() -> Result<()> {
    override_thing("--data-block-size", "8192", "data_block_size=\"8192\"")
}

#[test]
fn override_nr_data_blocks() -> Result<()> {
    override_thing("--nr-data-blocks", "234500", "nr_data_blocks=\"234500\"")
}

// FIXME: that's repair_superblock in thin_dump.rs
#[test]
fn superblock_succeeds() -> Result<()> {
    let mut td = TestDir::new()?;
    let md1 = mk_valid_md(&mut td)?;
    let original = run_ok_raw(thin_dump_cmd(args![&md1]))?;
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
    assert_eq!(original.stdout, repaired.stdout);
    Ok(())
}

//-----------------------------------------

// TODO: share with thin_dump

#[test]
fn missing_data_block_size() -> Result<()> {
    let mut td = TestDir::new()?;
    let src = mk_valid_md(&mut td)?;
    damage_superblock(&src)?;
    let dest = mk_zeroed_md(&mut td)?;
    let stderr = run_fail(thin_repair_cmd(args![
        "--transaction-id=1",
        "--nr-data-blocks=20480",
        "-i",
        &src,
        "-o",
        &dest
    ]))?;
    assert!(stderr.contains("data block size"));
    Ok(())
}

#[test]
fn recovers_transaction_id_from_damaged_superblock() -> Result<()> {
    let mut td = TestDir::new()?;
    let src = mk_valid_md(&mut td)?;
    damage_superblock(&src)?;
    let dest = mk_zeroed_md(&mut td)?;
    run_ok(thin_repair_cmd(args![
        "--data-block-size=128",
        "--nr-data-blocks=20480",
        "-i",
        &src,
        "-o",
        &dest
    ]))?;
    let repaired = run_ok(thin_dump_cmd(args![&dest]))?;
    assert!(repaired.contains("transaction=\"1\""));
    Ok(())
}

#[test]
fn recovers_nr_data_blocks_from_damaged_superblock() -> Result<()> {
    let mut td = TestDir::new()?;
    let src = mk_valid_md(&mut td)?;
    damage_superblock(&src)?;
    let dest = mk_zeroed_md(&mut td)?;
    run_ok(thin_repair_cmd(args![
        "--transaction-id=1",
        "--data-block-size=128",
        "-i",
        &src,
        "-o",
        &dest
    ]))?;
    let repaired = run_ok(thin_dump_cmd(args![&dest]))?;
    assert!(repaired.contains("nr_data_blocks=\"1024\""));
    Ok(())
}

//-----------------------------------------
