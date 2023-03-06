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
    "thin_restore ",
    tools_version!(),
    "
Convert XML format metadata to binary.

USAGE:
    thin_restore [OPTIONS] --input <FILE> --output <FILE>

OPTIONS:
        --data-block-size <SECTORS>    Override the data block size if needed
    -h, --help                         Print help information
    -i, --input <FILE>                 Specify the input xml
        --nr-data-blocks <NUM>         Override the number of data blocks if needed
    -o, --output <FILE>                Specify the output device
    -q, --quiet                        Suppress output messages, return only exit code.
        --transaction-id <NUM>         Override the transaction id if needed
    -V, --version                      Print version information"
);

//------------------------------------------

struct ThinRestore;

impl<'a> Program<'a> for ThinRestore {
    fn name() -> &'a str {
        "thin_restore"
    }

    fn cmd<I>(args: I) -> Command
    where
        I: IntoIterator,
        I::Item: Into<std::ffi::OsString>,
    {
        thin_restore_cmd(args)
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

impl<'a> InputProgram<'a> for ThinRestore {
    fn mk_valid_input(td: &mut TestDir) -> Result<std::path::PathBuf> {
        mk_valid_xml(td)
    }

    fn file_not_found() -> &'a str {
        msg::FILE_NOT_FOUND
    }

    fn missing_input_arg() -> &'a str {
        msg::MISSING_INPUT_ARG
    }

    fn corrupted_input() -> &'a str {
        "" // we don't intent to verify error messages of XML parsing
    }
}

impl<'a> OutputProgram<'a> for ThinRestore {
    fn missing_output_arg() -> &'a str {
        msg::MISSING_OUTPUT_ARG
    }
}

impl<'a> MetadataWriter<'a> for ThinRestore {
    fn file_not_found() -> &'a str {
        msg::FILE_NOT_FOUND
    }
}

//-----------------------------------------

test_accepts_help!(ThinRestore);
test_accepts_version!(ThinRestore);

test_missing_input_option!(ThinRestore);
test_input_file_not_found!(ThinRestore);
test_corrupted_input_data!(ThinRestore);

test_missing_output_option!(ThinRestore);
test_tiny_output_file!(ThinRestore);

test_unwritable_output_file!(ThinRestore);

//-----------------------------------------

// TODO: share with cache_restore, era_restore

fn quiet_flag(flag: &str) -> Result<()> {
    let mut td = TestDir::new()?;
    let xml = mk_valid_xml(&mut td)?;
    let md = mk_zeroed_md(&mut td)?;

    let output = run_ok_raw(thin_restore_cmd(args!["-i", &xml, "-o", &md, flag]))?;

    assert_eq!(output.stdout.len(), 0);
    assert_eq!(output.stderr.len(), 0);
    Ok(())
}

#[test]
fn accepts_q() -> Result<()> {
    quiet_flag("-q")
}

#[test]
fn accepts_quiet() -> Result<()> {
    quiet_flag("--quiet")
}

//-----------------------------------------

// TODO: share with thin_dump
fn override_something(flag: &str, value: &str, pattern: &str) -> Result<()> {
    let mut td = TestDir::new()?;
    let xml = mk_valid_xml(&mut td)?;
    let md = mk_zeroed_md(&mut td)?;

    run_ok(thin_restore_cmd(args!["-i", &xml, "-o", &md, flag, value]))?;

    let output = run_ok(thin_dump_cmd(args![&md]))?;
    assert!(output.contains(pattern));
    Ok(())
}

#[test]
fn override_transaction_id() -> Result<()> {
    override_something("--transaction-id", "2345", "transaction=\"2345\"")
}

#[test]
fn override_data_block_size() -> Result<()> {
    override_something("--data-block-size", "8192", "data_block_size=\"8192\"")
}

#[test]
fn override_nr_data_blocks() -> Result<()> {
    override_something("--nr-data-blocks", "234500", "nr_data_blocks=\"234500\"")
}

//-----------------------------------------
