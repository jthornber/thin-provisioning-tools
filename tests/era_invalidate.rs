use anyhow::Result;

mod common;

use common::common_args::*;
use common::era::*;
use common::fixture::write_file;
use common::input_arg::*;
use common::program::*;
use common::target::*;
use common::test_dir::*;

use thinp::file_utils;

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

fn mk_no_writeset_md(td: &mut TestDir) -> Result<std::path::PathBuf> {
    let xml = td.mk_path("meta.xml");
    let md = td.mk_path("meta.bin");

    let content = b"\
<superblock uuid=\"\" block_size=\"128\" nr_blocks=\"16\" current_era=\"5\">
  <era_array>
    <era block=\"0\" era=\"1\"/>
    <era block=\"1\" era=\"2\"/>
    <era block=\"2\" era=\"3\"/>
    <era block=\"3\" era=\"4\"/>
    <era block=\"4\" era=\"5\"/>
    <era block=\"5\" era=\"0\"/>
    <era block=\"6\" era=\"0\"/>
    <era block=\"7\" era=\"0\"/>
    <era block=\"8\" era=\"0\"/>
    <era block=\"9\" era=\"0\"/>
    <era block=\"10\" era=\"0\"/>
    <era block=\"11\" era=\"0\"/>
    <era block=\"12\" era=\"0\"/>
    <era block=\"13\" era=\"0\"/>
    <era block=\"14\" era=\"0\"/>
    <era block=\"15\" era=\"0\"/>
  </era_array>
</superblock>";

    write_file(&xml, content)?;
    let _file = file_utils::create_sized_file(&md, 4096 * 4096);
    run_ok(era_restore_cmd(args!["-i", &xml, "-o", &md]))?;

    Ok(md)
}

#[test]
fn no_writesets_threshold_zero() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_no_writeset_md(&mut td)?;

    let stdout = run_ok(era_invalidate_cmd(args![&md, "--written-since", "0"]))?;
    let expected = "\
<blocks>
  <range begin=\"0\" end=\"16\"/>
</blocks>";
    assert_eq!(stdout, expected);

    Ok(())
}

#[test]
fn no_writesets_threshold_within_era_range() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_no_writeset_md(&mut td)?;

    let stdout = run_ok(era_invalidate_cmd(args![&md, "--written-since", "3"]))?;
    let expected = "\
<blocks>
  <range begin=\"2\" end=\"5\"/>
</blocks>";
    assert_eq!(stdout, expected);

    Ok(())
}

#[test]
fn no_writesets_threshold_above_all_eras() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_no_writeset_md(&mut td)?;

    let stdout = run_ok(era_invalidate_cmd(args![&md, "--written-since", "6"]))?;
    let expected = "\
<blocks>
</blocks>";
    assert_eq!(stdout, expected);

    Ok(())
}

// Note: use the compact writeset format (<marked>) for conciseness
// rather than the per-bit format (<bit>) that era_dump emits.
fn mk_writeset_md(td: &mut TestDir) -> Result<std::path::PathBuf> {
    let xml = td.mk_path("meta.xml");
    let md = td.mk_path("meta.bin");

    let content = b"\
<superblock uuid=\"\" block_size=\"128\" nr_blocks=\"16\" current_era=\"5\">
  <writeset era=\"3\" nr_bits=\"16\">
    <marked block_begin=\"0\" len=\"2\"/>
  </writeset>
  <writeset era=\"4\" nr_bits=\"16\">
    <marked block_begin=\"4\" len=\"2\"/>
  </writeset>
  <writeset era=\"5\" nr_bits=\"16\">
    <marked block_begin=\"8\" len=\"2\"/>
  </writeset>
  <era_array>
    <era block=\"0\" era=\"0\"/>
    <era block=\"1\" era=\"1\"/>
    <era block=\"2\" era=\"2\"/>
    <era block=\"3\" era=\"1\"/>
    <era block=\"4\" era=\"0\"/>
    <era block=\"5\" era=\"1\"/>
    <era block=\"6\" era=\"0\"/>
    <era block=\"7\" era=\"0\"/>
    <era block=\"8\" era=\"2\"/>
    <era block=\"9\" era=\"0\"/>
    <era block=\"10\" era=\"0\"/>
    <era block=\"11\" era=\"0\"/>
    <era block=\"12\" era=\"2\"/>
    <era block=\"13\" era=\"2\"/>
    <era block=\"14\" era=\"0\"/>
    <era block=\"15\" era=\"0\"/>
  </era_array>
</superblock>";

    write_file(&xml, content)?;
    let _file = file_utils::create_sized_file(&md, 4096 * 4096);
    run_ok(era_restore_cmd(args!["-i", &xml, "-o", &md]))?;

    Ok(md)
}

#[test]
fn threshold_at_oldest_writeset() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_writeset_md(&mut td)?;

    let stdout = run_ok(era_invalidate_cmd(args![&md, "--written-since", "3"]))?;
    let expected = "\
<blocks>
  <range begin=\"0\" end=\"2\"/>
  <range begin=\"4\" end=\"6\"/>
  <range begin=\"8\" end=\"10\"/>
</blocks>";
    assert_eq!(stdout, expected);

    Ok(())
}

#[test]
fn threshold_at_newest_writeset() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_writeset_md(&mut td)?;

    let stdout = run_ok(era_invalidate_cmd(args![&md, "--written-since", "5"]))?;
    let expected = "\
<blocks>
  <range begin=\"8\" end=\"10\"/>
</blocks>";
    assert_eq!(stdout, expected);

    Ok(())
}

#[test]
fn threshold_below_oldest_writeset() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_writeset_md(&mut td)?;

    let stdout = run_ok(era_invalidate_cmd(args![&md, "--written-since", "2"]))?;
    let expected = "\
<blocks>
  <range begin=\"0\" end=\"3\"/>
  <range begin=\"4\" end=\"6\"/>
  <range begin=\"8\" end=\"10\"/>
  <range begin=\"12\" end=\"14\"/>
</blocks>";
    assert_eq!(stdout, expected);

    Ok(())
}

#[test]
fn threshold_above_newest_writeset() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_writeset_md(&mut td)?;

    let stdout = run_ok(era_invalidate_cmd(args![&md, "--written-since", "6"]))?;
    let expected = "\
<blocks>
</blocks>";
    assert_eq!(stdout, expected);

    Ok(())
}

#[test]
fn threshold_zero() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_writeset_md(&mut td)?;

    let stdout = run_ok(era_invalidate_cmd(args![&md, "--written-since", "0"]))?;
    let expected = "\
<blocks>
  <range begin=\"0\" end=\"16\"/>
</blocks>";
    assert_eq!(stdout, expected);

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
