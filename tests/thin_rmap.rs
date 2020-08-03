use anyhow::Result;
use thinp::version::TOOLS_VERSION;

mod common;
use common::*;

//------------------------------------------

#[test]
fn accepts_v() -> Result<()> {
    let stdout = thin_rmap!("-V").read()?;
    assert_eq!(stdout, TOOLS_VERSION);
    Ok(())
}

#[test]
fn accepts_version() -> Result<()> {
    let stdout = thin_rmap!("--version").read()?;
    assert_eq!(stdout, TOOLS_VERSION);
    Ok(())
}

const USAGE: &str = "Usage: thin_rmap [options] {device|file}\nOptions:\n  {-h|--help}\n  {-V|--version}\n  {--region <block range>}*\nWhere:\n  <block range> is of the form <begin>..<one-past-the-end>\n  for example 5..45 denotes blocks 5 to 44 inclusive, but not block 45";

#[test]
fn accepts_h() -> Result<()> {
    let stdout = thin_rmap!("-h").read()?;
    assert_eq!(stdout, USAGE);
    Ok(())
}

#[test]
fn accepts_help() -> Result<()> {
    let stdout = thin_rmap!("--help").read()?;
    assert_eq!(stdout, USAGE);
    Ok(())
}

#[test]
fn rejects_bad_option() -> Result<()> {
    let stderr = run_fail(thin_rmap!("--hedgehogs-only"))?;
    assert!(stderr.contains("unrecognized option \'--hedgehogs-only\'"));
    Ok(())
}

#[test]
fn valid_region_format_should_pass() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;
    let output = thin_rmap!("--region", "23..7890", &md).unchecked().run()?;
    eprintln!("stdout: {:?}", output.stdout);
    eprintln!("stderr: {:?}", output.stderr);
    assert!(output.status.success());
    Ok(())
}

#[test]
fn invalid_regions_should_fail() -> Result<()> {
    let invalid_regions = ["23,7890", "23..six", "found..7890", "89..88", "89..89", "89..", "", "89...99"];
    for r in &invalid_regions {
        let mut td = TestDir::new()?;
        let md = mk_valid_md(&mut td)?;
        run_fail(thin_rmap!(r, &md))?;
    }
    Ok(())
}

#[test]
fn multiple_regions_should_pass() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;
    thin_rmap!("--region", "1..23", "--region", "45..78", &md).run()?;
    Ok(())
}

#[test]
fn junk_input() -> Result<()> {
    let mut td = TestDir::new()?;
    let xml = mk_valid_xml(&mut td)?;
    run_fail(thin_rmap!("--region", "0..-1", &xml))?;
    Ok(())
}

//------------------------------------------
