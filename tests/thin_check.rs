use anyhow::Result;
use duct::{cmd, Expression};
use std::path::{Path, PathBuf};
use std::str::from_utf8;
use tempfile::{tempdir, TempDir};
use thinp::file_utils;
use thinp::version::TOOLS_VERSION;

mod common;

use common::mk_path;
use common::xml_generator::{write_xml, FragmentedS, SingleThinS};

//------------------------------------------

macro_rules! thin_check {
    ( $( $arg: expr ),* ) => {
        {
            use std::ffi::OsString;
            let args: &[OsString] = &[$( Into::<OsString>::into($arg) ),*];
            duct::cmd("bin/thin_check", args).stdout_capture().stderr_capture()
        }
    };
}

// Returns stderr, a non zero status must be returned
fn run_fail(command: Expression) -> Result<String> {
    let output = command.stderr_capture().unchecked().run()?;
    assert!(!output.status.success());
    Ok(from_utf8(&output.stderr[0..]).unwrap().to_string())
}

fn mk_valid_md(dir: &TempDir) -> Result<PathBuf> {
    let xml = mk_path(dir.path(), "meta.xml");
    let md = mk_path(dir.path(), "meta.bin");

    let mut gen = SingleThinS::new(0, 1024, 2048, 2048);
    write_xml(&xml, &mut gen)?;

    let _file = file_utils::create_sized_file(&md, 4096 * 4096);
    cmd!("bin/thin_restore", "-i", xml, "-o", &md).run()?;
    Ok(md)
}

fn mk_corrupt_md(dir: &TempDir) -> Result<PathBuf> {
    let md = mk_path(dir.path(), "meta.bin");
    let _file = file_utils::create_sized_file(&md, 4096 * 4096);
    Ok(md)
}

fn accepts_flag(flag: &str) -> Result<()> {
    let dir = tempdir()?;
    let md = mk_valid_md(&dir)?;
    thin_check!(flag, &md).run()?;
    Ok(())
}

//------------------------------------------

#[test]
fn accepts_v() -> Result<()> {
    let stdout = thin_check!("-V").read()?;
    assert_eq!(stdout, TOOLS_VERSION);
    Ok(())
}

#[test]
fn accepts_version() -> Result<()> {
    let stdout = thin_check!("--version").read()?;
    assert_eq!(stdout, TOOLS_VERSION);
    Ok(())
}

const USAGE: &'static str = "Usage: thin_check [options] {device|file}\nOptions:\n  {-q|--quiet}\n  {-h|--help}\n  {-V|--version}\n  {-m|--metadata-snap}\n  {--override-mapping-root}\n  {--clear-needs-check-flag}\n  {--ignore-non-fatal-errors}\n  {--skip-mappings}\n  {--super-block-only}";

#[test]
fn accepts_h() -> Result<()> {
    let stdout = thin_check!("-h").read()?;
    assert_eq!(stdout, USAGE);
    Ok(())
}

#[test]
fn accepts_help() -> Result<()> {
    let stdout = thin_check!("--help").read()?;
    assert_eq!(stdout, USAGE);
    Ok(())
}

#[test]
fn rejects_bad_option() -> Result<()> {
    let stderr = run_fail(thin_check!("--hedgehogs-only"))?;
    assert!(stderr.contains("unrecognized option \'--hedgehogs-only\'"));
    Ok(())
}

#[test]
fn accepts_superblock_only() -> Result<()> {
    accepts_flag("--super-block-only")
}

#[test]
fn accepts_skip_mappings() -> Result<()> {
    accepts_flag("--skip-mappings")
}

#[test]
fn accepts_ignore_non_fatal_errors() -> Result<()> {
    accepts_flag("--ignore-non-fatal-errors")
}

#[test]
fn accepts_clear_needs_check_flag() -> Result<()> {
    accepts_flag("--clear-needs-check-flag")
}

#[test]
fn accepts_quiet() -> Result<()> {
    let dir = tempdir()?;
    let md = mk_valid_md(&dir)?;

    let output = thin_check!("--quiet", &md).run()?;
    assert_eq!(output.stdout.len(), 0);
    assert_eq!(output.stderr.len(), 0);
    Ok(())
}

#[test]
fn detects_corrupt_superblock_with_superblock_only() -> Result<()> {
    let dir = tempdir()?;
    let md = mk_corrupt_md(&dir)?;
    let output = thin_check!("--super-block-only", &md).unchecked().run()?;
    assert!(!output.status.success());
    Ok(())
}

#[test]
fn prints_help_message_for_tiny_metadata() -> Result<()> {
    let dir = tempdir()?;
    let md = mk_path(dir.path(), "meta.bin");
    let _file = file_utils::create_sized_file(&md, 1024);
    let stderr = run_fail(thin_check!(&md))?;
    assert!(stderr.contains("Metadata device/file too small.  Is this binary metadata?"));
    Ok(())
}

#[test]
fn spot_xml_data() -> Result<()> {
    let dir = tempdir()?;
    let xml = mk_path(dir.path(), "meta.xml");

    let mut gen = FragmentedS::new(4, 10240);
    write_xml(&xml, &mut gen)?;

    let stderr = run_fail(thin_check!(&xml))?;
    eprintln!("{}", stderr);
    assert!(
        stderr.contains("This looks like XML.  thin_check only checks the binary metadata format.")
    );
    Ok(())
}

#[test]
fn prints_info_fields() -> Result<()> {
    let dir = tempdir()?;
    let md = mk_valid_md(&dir)?;
    let stdout = thin_check!(&md).read()?;
    assert!(stdout.contains("TRANSACTION_ID="));
    assert!(stdout.contains("METADATA_FREE_BLOCKS="));
    Ok(())
}

//------------------------------------------
