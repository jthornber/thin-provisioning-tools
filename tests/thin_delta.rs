use anyhow::Result;

mod common;

use common::common_args::*;
use common::test_dir::*;
use common::*;

//------------------------------------------

const USAGE: &str = "Usage: thin_delta [options] <device or file>\n\
                     Options:\n  \
                       {--thin1, --snap1, --root1}\n  \
                       {--thin2, --snap2, --root2}\n  \
                       {-m, --metadata-snap} [block#]\n  \
                       {--verbose}\n  \
                       {-h|--help}\n  \
                       {-V|--version}";

//------------------------------------------

test_accepts_help!(THIN_DELTA, USAGE);
test_accepts_version!(THIN_DELTA);
test_rejects_bad_option!(THIN_DELTA);

//------------------------------------------

#[test]
fn snap1_unspecified() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;
    let stderr = run_fail(THIN_DELTA, &["--snap2", "45", md.to_str().unwrap()])?;
    assert!(stderr.contains("--snap1 or --root1 not specified"));
    Ok(())
}

#[test]
fn snap2_unspecified() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;
    let stderr = run_fail(THIN_DELTA, &["--snap1", "45", md.to_str().unwrap()])?;
    assert!(stderr.contains("--snap2 or --root2 not specified"));
    Ok(())
}

#[test]
fn dev_unspecified() -> Result<()> {
    let stderr = run_fail(THIN_DELTA, &["--snap1", "45", "--snap2", "46"])?;
    // TODO: replace with msg::MISSING_INPUT_ARG once the rust version is ready
    assert!(stderr.contains("No input file provided"));
    Ok(())
}

//------------------------------------------
