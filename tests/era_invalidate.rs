use anyhow::Result;

mod common;

use common::common_args::*;
use common::era::*;
use common::input_arg::*;
use common::program::*;
use common::target::*;
use common::test_dir::*;

//------------------------------------------

const USAGE: &str = "List blocks that may have changed since a given era

Usage: era_invalidate [OPTIONS] --written-since <ERA> <INPUT>

Arguments:
  <INPUT>  Specify the input device

Options:
  -h, --help                 Print help
      --metadata-snapshot    Use the metadata snapshot rather than the current superblock
  -o, --output <FILE>        Specify the output file rather than stdout
  -V, --version              Print version
      --written-since <ERA>  Blocks written since the given era will be listed";

//------------------------------------------

struct EraInvalidate;

impl<'a> Program<'a> for EraInvalidate {
    fn name() -> &'a str {
        "era_invalidate"
    }

    fn cmd<I>(args: I) -> Command
    where
        I: IntoIterator,
        I::Item: Into<std::ffi::OsString>,
    {
        era_invalidate_cmd(args)
    }

    fn usage() -> &'a str {
        USAGE
    }

    fn arg_type() -> ArgType {
        ArgType::InputArg
    }

    fn required_args() -> &'a [&'a str] {
        &["--written-since", "0"]
    }

    fn bad_option_hint(option: &str) -> String {
        msg::bad_option_hint(option)
    }
}

impl<'a> InputProgram<'a> for EraInvalidate {
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

impl MetadataReader<'_> for EraInvalidate {}

//------------------------------------------

test_accepts_help!(EraInvalidate);
test_accepts_version!(EraInvalidate);
test_rejects_bad_option!(EraInvalidate);

test_missing_input_arg!(EraInvalidate);
test_input_file_not_found!(EraInvalidate);
test_input_cannot_be_a_directory!(EraInvalidate);
test_unreadable_input_file!(EraInvalidate);
test_tiny_input_file!(EraInvalidate);

test_readonly_input_file!(EraInvalidate);

//------------------------------------------

#[test]
fn written_since_unspecified() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;
    let stderr = run_fail(era_invalidate_cmd(args![&md]))?;
    assert!(stderr.contains(
        "the following required arguments were not provided:
  --written-since <ERA>"
    ));
    Ok(())
}

#[test]
fn metadata_snapshot_fails_when_none_exists() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;
    let stderr = run_fail(era_invalidate_cmd(args![
        &md,
        "--written-since",
        "0",
        "--metadata-snapshot"
    ]))?;
    assert!(stderr.contains("no current metadata snap"));
    Ok(())
}

#[test]
fn list_changes_in_metadata_snapshot() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = prep_metadata_from_file(&mut td, "emeta_with_metadata_snap.pack")?;

    let stdout = run_ok(era_invalidate_cmd(args![
        &md,
        "--written-since",
        "0",
        "--metadata-snapshot"
    ]))?;
    assert_eq!(
        stdout,
        "<blocks>\n  <range begin=\"0\" end=\"512\"/>\n</blocks>"
    );

    let stdout = run_ok(era_invalidate_cmd(args![
        &md,
        "--written-since",
        "1",
        "--metadata-snapshot"
    ]))?;
    assert_eq!(
        stdout,
        "<blocks>\n  <range begin=\"0\" end=\"64\"/>\n</blocks>"
    );

    let stdout = run_ok(era_invalidate_cmd(args![
        &md,
        "--written-since",
        "2",
        "--metadata-snapshot"
    ]))?;
    assert_eq!(
        stdout,
        "<blocks>\n  <range begin=\"0\" end=\"32\"/>\n</blocks>"
    );

    let stdout = run_ok(era_invalidate_cmd(args![
        &md,
        "--written-since",
        "3",
        "--metadata-snapshot"
    ]))?;
    assert_eq!(stdout, "<blocks>\n</blocks>");

    Ok(())
}

//------------------------------------------
