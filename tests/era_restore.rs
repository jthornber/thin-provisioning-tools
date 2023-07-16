use anyhow::Result;

mod common;

use common::common_args::*;
use common::era::*;
use common::fixture::*;
use common::input_arg::*;
use common::output_option::*;
use common::process::*;
use common::program::*;
use common::target::*;
use common::test_dir::*;

//------------------------------------------

const USAGE: &str = "Convert XML format metadata to binary.

Usage: era_restore [OPTIONS] --input <FILE> --output <FILE>

Options:
  -h, --help           Print help
  -i, --input <FILE>   Specify the input xml
  -o, --output <FILE>  Specify the output device
  -q, --quiet          Suppress output messages, return only exit code.
  -V, --version        Print version";

//------------------------------------------

struct EraRestore;

impl<'a> Program<'a> for EraRestore {
    fn name() -> &'a str {
        "era_restore"
    }

    fn cmd<I>(args: I) -> Command
    where
        I: IntoIterator,
        I::Item: Into<std::ffi::OsString>,
    {
        era_restore_cmd(args)
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

impl<'a> InputProgram<'a> for EraRestore {
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

impl<'a> OutputProgram<'a> for EraRestore {
    fn missing_output_arg() -> &'a str {
        msg::MISSING_OUTPUT_ARG
    }
}

impl<'a> MetadataWriter<'a> for EraRestore {
    fn file_not_found() -> &'a str {
        msg::FILE_NOT_FOUND
    }
}

//-----------------------------------------

test_accepts_help!(EraRestore);
test_accepts_version!(EraRestore);

test_missing_input_option!(EraRestore);
test_input_file_not_found!(EraRestore);
test_corrupted_input_data!(EraRestore);

test_missing_output_option!(EraRestore);
test_tiny_output_file!(EraRestore);

test_unwritable_output_file!(EraRestore);

//-----------------------------------------

// TODO: share with thin_restore, era_restore

fn quiet_flag(flag: &str) -> Result<()> {
    let mut td = TestDir::new()?;
    let xml = mk_valid_xml(&mut td)?;
    let md = mk_zeroed_md(&mut td)?;

    let output = run_ok_raw(era_restore_cmd(args!["-i", &xml, "-o", &md, flag]))?;

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

#[test]
fn successfully_restores() -> Result<()> {
    let mut td = TestDir::new()?;
    let xml = mk_valid_xml(&mut td)?;
    let md = mk_zeroed_md(&mut td)?;
    run_ok(era_restore_cmd(args!["-i", &xml, "-o", &md]))?;
    Ok(())
}

//-----------------------------------------
