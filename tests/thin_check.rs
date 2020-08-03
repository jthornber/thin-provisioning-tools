use anyhow::Result;
use thinp::file_utils;
use thinp::version::TOOLS_VERSION;

mod common;

use common::*;
use common::xml_generator::{write_xml, FragmentedS};

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

const USAGE: &str = "Usage: thin_check [options] {device|file}\nOptions:\n  {-q|--quiet}\n  {-h|--help}\n  {-V|--version}\n  {-m|--metadata-snap}\n  {--override-mapping-root}\n  {--clear-needs-check-flag}\n  {--ignore-non-fatal-errors}\n  {--skip-mappings}\n  {--super-block-only}";

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
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;

    let output = thin_check!("--quiet", &md).run()?;
    assert!(output.status.success());
    assert_eq!(output.stdout.len(), 0);
    assert_eq!(output.stderr.len(), 0);
    Ok(())
}

#[test]
fn detects_corrupt_superblock_with_superblock_only() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_zeroed_md(&mut td)?;
    let output = thin_check!("--super-block-only", &md).unchecked().run()?;
    assert!(!output.status.success());
    Ok(())
}

#[test]
fn prints_help_message_for_tiny_metadata() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = td.mk_path("meta.bin");
    let _file = file_utils::create_sized_file(&md, 1024);
    let stderr = run_fail(thin_check!(&md))?;
    assert!(stderr.contains("Metadata device/file too small.  Is this binary metadata?"));
    Ok(())
}

#[test]
fn spot_xml_data() -> Result<()> {
    let mut td = TestDir::new()?;
    let xml = td.mk_path("meta.xml");

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
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;
    let stdout = thin_check!(&md).read()?;
    assert!(stdout.contains("TRANSACTION_ID="));
    assert!(stdout.contains("METADATA_FREE_BLOCKS="));
    Ok(())
}

//------------------------------------------
