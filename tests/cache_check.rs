use anyhow::Result;

mod common;

use common::cache::*;
use common::common_args::*;
use common::fixture::*;
use common::input_arg::*;
use common::process::*;
use common::program::*;
use common::target::*;
use common::test_dir::*;

//------------------------------------------

const USAGE: &str = concat!(
    "cache_check ",
    include_str!("../VERSION"),
    "Validates cache metadata on a device or file.

USAGE:
    cache_check [OPTIONS] <INPUT>

ARGS:
    <INPUT>    Specify the input device to check

OPTIONS:
        --auto-repair                Auto repair trivial issues.
    -h, --help                       Print help information
        --ignore-non-fatal-errors    Only return a non-zero exit code if a fatal error is found.
    -q, --quiet                      Suppress output messages, return only exit code.
        --skip-discards              Don't check the discard bitset
        --skip-hints                 Don't check the hint array
        --skip-mappings              Don't check the mapping tree
        --super-block-only           Only check the superblock.
    -V, --version                    Print version information"
);

//------------------------------------------

struct CacheCheck;

impl<'a> Program<'a> for CacheCheck {
    fn name() -> &'a str {
        "cache_check"
    }

    fn cmd<I>(args: I) -> Command
    where
        I: IntoIterator,
        I::Item: Into<std::ffi::OsString>,
    {
        cache_check_cmd(args)
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

impl<'a> InputProgram<'a> for CacheCheck {
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

impl<'a> MetadataReader<'a> for CacheCheck {}

//------------------------------------------

test_accepts_help!(CacheCheck);
test_accepts_version!(CacheCheck);
test_rejects_bad_option!(CacheCheck);

test_missing_input_arg!(CacheCheck);
test_input_file_not_found!(CacheCheck);
test_input_cannot_be_a_directory!(CacheCheck);
test_unreadable_input_file!(CacheCheck);

test_help_message_for_tiny_input_file!(CacheCheck);
test_spot_xml_data!(CacheCheck);
test_corrupted_input_data!(CacheCheck);

//------------------------------------------

#[test]
fn failing_q() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_zeroed_md(&mut td)?;
    let output = run_fail_raw(cache_check_cmd(args!["-q", &md]))?;
    assert_eq!(output.stdout.len(), 0);
    eprintln!(
        "stderr = '{}'",
        std::str::from_utf8(&output.stderr).unwrap()
    );
    assert_eq!(output.stderr.len(), 0);
    Ok(())
}

#[test]
fn failing_quiet() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_zeroed_md(&mut td)?;
    let output = run_fail_raw(cache_check_cmd(args!["--quiet", &md]))?;
    assert_eq!(output.stdout.len(), 0);
    assert_eq!(output.stderr.len(), 0);
    Ok(())
}

#[test]
fn valid_metadata_passes() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;
    run_ok(cache_check_cmd(args![&md]))?;
    Ok(())
}

#[test]
fn bad_metadata_version() -> Result<()> {
    let mut td = TestDir::new()?;
    let xml = mk_valid_xml(&mut td)?;
    let md = mk_zeroed_md(&mut td)?;
    run_ok(cache_restore_cmd(args!["-i", &xml, "-o", &md]))?;
    run_ok(cache_generate_metadata_cmd(args![
        "-o",
        &md,
        "--set-superblock-version",
        "3"
    ]))?;
    run_fail(cache_check_cmd(args![&md]))?;
    Ok(())
}

#[test]
fn incompat_metadata_version_v1() -> Result<()> {
    let mut td = TestDir::new()?;
    let xml = mk_valid_xml(&mut td)?;
    let md = mk_zeroed_md(&mut td)?;
    run_ok(cache_restore_cmd(args!["-i", &xml, "-o", &md]))?;
    run_ok(cache_generate_metadata_cmd(args![
        "-o",
        &md,
        "--set-superblock-version",
        "1"
    ]))?;
    run_fail(cache_check_cmd(args![&md]))?;
    Ok(())
}

#[test]
fn incompat_metadata_version_v2() -> Result<()> {
    let mut td = TestDir::new()?;
    let xml = mk_valid_xml(&mut td)?;
    let md = mk_zeroed_md(&mut td)?;
    run_ok(cache_restore_cmd(args![
        "-i",
        &xml,
        "-o",
        &md,
        "--metadata-version",
        "1"
    ]))?;
    run_ok(cache_generate_metadata_cmd(args![
        "-o",
        &md,
        "--set-superblock-version",
        "2"
    ]))?;
    run_fail(cache_check_cmd(args![&md]))?;
    Ok(())
}
