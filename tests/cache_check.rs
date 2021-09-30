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

const USAGE: &str = "Usage: cache_check [options] {device|file}\n\
                     Options:\n  \
                       {-q|--quiet}\n  \
                       {-h|--help}\n  \
                       {-V|--version}\n  \
                       {--clear-needs-check-flag}\n  \
                       {--super-block-only}\n  \
                       {--skip-mappings}\n  \
                       {--skip-hints}\n  \
                       {--skip-discards}";

//------------------------------------------

struct CacheCheck;

impl<'a> Program<'a> for CacheCheck {
    fn name() -> &'a str {
        "cache_check"
    }

    fn path() -> &'a std::ffi::OsStr {
        CACHE_CHECK.as_ref()
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
    let output = run_fail_raw(CACHE_CHECK, args!["-q", &md])?;
    assert_eq!(output.stdout.len(), 0);
    assert_eq!(output.stderr.len(), 0);
    Ok(())
}

#[test]
fn failing_quiet() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_zeroed_md(&mut td)?;
    let output = run_fail_raw(CACHE_CHECK, args!["--quiet", &md])?;
    assert_eq!(output.stdout.len(), 0);
    assert_eq!(output.stderr.len(), 0);
    Ok(())
}

#[test]
fn valid_metadata_passes() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;
    run_ok(CACHE_CHECK, args![&md])?;
    Ok(())
}

#[test]
fn bad_metadata_version() -> Result<()> {
    let mut td = TestDir::new()?;
    let xml = mk_valid_xml(&mut td)?;
    let md = mk_zeroed_md(&mut td)?;
    run_ok(
        CACHE_RESTORE,
        args![
            "-i",
            &xml,
            "-o",
            &md,
            "--debug-override-metadata-version",
            "12345"
        ],
    )?;
    run_fail(CACHE_CHECK, args![&md])?;
    Ok(())
}
