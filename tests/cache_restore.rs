use anyhow::Result;

mod common;

use common::cache::*;
use common::common_args::*;
use common::fixture::*;
use common::input_arg::*;
use common::output_option::*;
use common::process::*;
use common::program::*;
use common::target::*;
use common::test_dir::*;

use thinp::tools_version;

//------------------------------------------

const USAGE: &str = concat!(
    "cache_restore ",
    tools_version!(),
    "
Convert XML format metadata to binary.

USAGE:
    cache_restore [OPTIONS] --input <FILE> --output <FILE>

OPTIONS:
    -h, --help                      Print help information
    -i, --input <FILE>              Specify the input xml
        --metadata-version <NUM>    Specify the output metadata version [default: 2] [possible
                                    values: 1, 2]
    -o, --output <FILE>             Specify the output device
        --omit-clean-shutdown       Don't set the clean shutdown flag
    -q, --quiet                     Suppress output messages, return only exit code
    -V, --version                   Print version information"
);

//------------------------------------------

struct CacheRestore;

impl<'a> Program<'a> for CacheRestore {
    fn name() -> &'a str {
        "thin_restore"
    }

    fn cmd<I>(args: I) -> Command
    where
        I: IntoIterator,
        I::Item: Into<std::ffi::OsString>,
    {
        cache_restore_cmd(args)
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

impl<'a> InputProgram<'a> for CacheRestore {
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

impl<'a> OutputProgram<'a> for CacheRestore {
    fn missing_output_arg() -> &'a str {
        msg::MISSING_OUTPUT_ARG
    }
}

impl<'a> MetadataWriter<'a> for CacheRestore {
    fn file_not_found() -> &'a str {
        msg::FILE_NOT_FOUND
    }
}

//-----------------------------------------

test_accepts_help!(CacheRestore);
test_accepts_version!(CacheRestore);

test_missing_input_option!(CacheRestore);
test_input_file_not_found!(CacheRestore);
test_corrupted_input_data!(CacheRestore);

test_missing_output_option!(CacheRestore);
test_tiny_output_file!(CacheRestore);

test_unwritable_output_file!(CacheRestore);

//-----------------------------------------

// TODO: share with thin_restore, era_restore

fn quiet_flag(flag: &str) -> Result<()> {
    let mut td = TestDir::new()?;
    let xml = mk_valid_xml(&mut td)?;
    let md = mk_zeroed_md(&mut td)?;

    let output = run_ok_raw(cache_restore_cmd(args!["-i", &xml, "-o", &md, flag]))?;

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
    run_ok(cache_restore_cmd(args!["-i", &xml, "-o", &md]))?;
    Ok(())
}

#[test]
fn omit_clean_shutdown() -> Result<()> {
    let mut td = TestDir::new()?;
    let xml = mk_valid_xml(&mut td)?;
    let md = mk_zeroed_md(&mut td)?;
    run_ok(cache_restore_cmd(args![
        "-i",
        &xml,
        "-o",
        &md,
        "--omit-clean-shutdown"
    ]))?;
    assert!(!get_clean_shutdown(&md)?);
    Ok(())
}

//-----------------------------------------
