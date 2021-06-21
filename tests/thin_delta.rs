use anyhow::Result;
use thinp::version::tools_version;

mod common;
use common::test_dir::*;
use common::*;

//------------------------------------------

#[test]
fn accepts_v() -> Result<()> {
    let stdout = thin_delta!("-V").read()?;
    assert!(stdout.contains(tools_version()));
    Ok(())
}

#[test]
fn accepts_version() -> Result<()> {
    let stdout = thin_delta!("--version").read()?;
    assert!(stdout.contains(tools_version()));
    Ok(())
}

const USAGE: &str = "Usage: thin_delta [options] <device or file>\nOptions:\n  {--thin1, --snap1, --root1}\n  {--thin2, --snap2, --root2}\n  {-m, --metadata-snap} [block#]\n  {--verbose}\n  {-h|--help}\n  {-V|--version}";

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
    assert!(stderr.contains("--snap1 or --root1 not specified"));
    Ok(())
}

#[test]
fn snap2_unspecified() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;
    let stderr = run_fail(thin_delta!("--snap1", "45", &md))?;
    assert!(stderr.contains("--snap2 or --root2 not specified"));
    Ok(())
}

#[test]
fn dev_unspecified() -> Result<()> {
    let stderr = run_fail(thin_delta!("--snap1", "45", "--snap2", "46"))?;
    // TODO: replace with msg::MISSING_INPUT_ARG once the rust version is ready
    assert!(stderr.contains("No input file provided"));
    Ok(())
}
