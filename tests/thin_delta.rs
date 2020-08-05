use anyhow::Result;
use thinp::version::TOOLS_VERSION;

mod common;
use common::*;

//------------------------------------------

#[test]
fn accepts_v() -> Result<()> {
    let stdout = thin_delta!("-V").read()?;
    assert_eq!(stdout, TOOLS_VERSION);
    Ok(())
}

#[test]
fn accepts_version() -> Result<()> {
    let stdout = thin_delta!("--version").read()?;
    assert_eq!(stdout, TOOLS_VERSION);
    Ok(())
}

const USAGE: &str = "Usage: thin_delta [options] <device or file>\nOptions:\n  {--thin1, --snap1}\n  {--thin2, --snap2}\n  {-m, --metadata-snap} [block#]\n  {--verbose}\n  {-h|--help}\n  {-V|--version}";

#[test]
fn accepts_h() -> Result<()> {
    let stdout = thin_delta!("-h").read()?;
    assert_eq!(stdout, USAGE);
    Ok(())
}

#[test]
fn accepts_help() -> Result<()> {
    let stdout = thin_delta!("--help").read()?;
    assert_eq!(stdout, USAGE);
    Ok(())
}

#[test]
fn rejects_bad_option() -> Result<()> {
    let stderr = run_fail(thin_delta!("--hedgehogs-only"))?;
    assert!(stderr.contains("unrecognized option \'--hedgehogs-only\'"));
    Ok(())
}

#[test]
fn snap1_unspecified() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;
    let stderr = run_fail(thin_delta!("--snap2", "45", &md))?;
    assert!(stderr.contains("--snap1 not specified"));
    Ok(())
}

#[test]
fn snap2_unspecified() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;
    let stderr = run_fail(thin_delta!("--snap1", "45", &md))?;
    assert!(stderr.contains("--snap2 not specified"));
    Ok(())
}

#[test]
fn dev_unspecified() -> Result<()> {
    let stderr = run_fail(thin_delta!("--snap1", "45", "--snap2", "46"))?;
    assert!(stderr.contains("No input device provided"));
    Ok(())
}

