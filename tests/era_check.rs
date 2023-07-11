use anyhow::Result;

mod common;

use common::common_args::*;
use common::era::*;
use common::fixture::*;
use common::input_arg::*;
use common::process::*;
use common::program::*;
use common::target::*;
use common::test_dir::*;

//------------------------------------------

const USAGE: &str = "Validate era metadata on device or file.

Usage: era_check [OPTIONS] <INPUT>

Arguments:
  <INPUT>  Specify the input device to check

Options:
  -h, --help                     Print help
      --ignore-non-fatal-errors  Only return a non-zero exit code if a fatal error is found.
  -q, --quiet                    Suppress output messages, return only exit code.
      --super-block-only         Only check the superblock.
  -V, --version                  Print version";

//------------------------------------------

struct EraCheck;

impl<'a> Program<'a> for EraCheck {
    fn name() -> &'a str {
        "era_check"
    }

    fn cmd<I>(args: I) -> Command
    where
        I: IntoIterator,
        I::Item: Into<std::ffi::OsString>,
    {
        era_check_cmd(args)
    }

    fn usage() -> &'a str {
        USAGE
    }

    fn arg_type() -> ArgType {
        ArgType::InputArg
    }

    fn bad_option_hint(option: &str) -> String {
        msg::bad_option_hint(option)
    }
}

impl<'a> InputProgram<'a> for EraCheck {
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
        msg::BAD_SUPERBLOCK
    }
}

impl<'a> MetadataReader<'a> for EraCheck {}

//------------------------------------------

test_accepts_help!(EraCheck);
test_accepts_version!(EraCheck);
test_rejects_bad_option!(EraCheck);

test_missing_input_arg!(EraCheck);
test_input_file_not_found!(EraCheck);
test_input_cannot_be_a_directory!(EraCheck);
test_unreadable_input_file!(EraCheck);

test_help_message_for_tiny_input_file!(EraCheck);
test_spot_xml_data!(EraCheck);
test_corrupted_input_data!(EraCheck);

test_readonly_input_file!(EraCheck);

//------------------------------------------
// test exclusive flags

fn accepts_flag(flag: &str) -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;
    run_ok(era_check_cmd(args![flag, &md]))?;
    Ok(())
}

#[test]
fn accepts_superblock_only() -> Result<()> {
    accepts_flag("--super-block-only")
}

#[test]
fn accepts_ignore_non_fatal_errors() -> Result<()> {
    accepts_flag("--ignore-non-fatal-errors")
}

//------------------------------------------

#[test]
fn failing_q() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_zeroed_md(&mut td)?;
    let output = run_fail_raw(era_check_cmd(args!["-q", &md]))?;
    assert_eq!(output.stdout.len(), 0);
    assert_eq!(output.stderr.len(), 0);
    Ok(())
}

#[test]
fn failing_quiet() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_zeroed_md(&mut td)?;
    let output = run_fail_raw(era_check_cmd(args!["--quiet", &md]))?;
    assert_eq!(output.stdout.len(), 0);
    assert_eq!(output.stderr.len(), 0);
    Ok(())
}

//------------------------------------------
