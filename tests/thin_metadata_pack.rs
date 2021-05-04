use anyhow::Result;
use thinp::version::TOOLS_VERSION;

mod common;
use common::test_dir::*;
use common::*;

//------------------------------------------

#[test]
fn accepts_v() -> Result<()> {
    let stdout = thin_metadata_pack!("-V").read()?;
    assert!(stdout.contains(TOOLS_VERSION));
    Ok(())
}

#[test]
fn accepts_version() -> Result<()> {
    let stdout = thin_metadata_pack!("--version").read()?;
    assert!(stdout.contains(TOOLS_VERSION));
    Ok(())
}

const USAGE: &str = "thin_metadata_pack 0.9.0\nProduces a compressed file of thin metadata.  Only packs metadata blocks that are actually used.\n\nUSAGE:\n    thin_metadata_pack -i <DEV> -o <FILE>\n\nFLAGS:\n    -h, --help       Prints help information\n    -V, --version    Prints version information\n\nOPTIONS:\n    -i <DEV>         Specify thinp metadata binary device/file\n    -o <FILE>        Specify packed output file";

#[test]
fn accepts_h() -> Result<()> {
    let stdout = thin_metadata_pack!("-h").read()?;
    assert_eq!(stdout, USAGE);
    Ok(())
}

#[test]
fn accepts_help() -> Result<()> {
    let stdout = thin_metadata_pack!("--help").read()?;
    assert_eq!(stdout, USAGE);
    Ok(())
}

#[test]
fn rejects_bad_option() -> Result<()> {
    let stderr = run_fail(thin_metadata_pack!("--hedgehogs-only"))?;
    assert!(stderr.contains("Found argument \'--hedgehogs-only\'"));
    Ok(())
}

#[test]
fn missing_input_file() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_zeroed_md(&mut td)?;
    let stderr = run_fail(thin_metadata_pack!("-o", &md))?;
    assert!(
        stderr.contains("error: The following required arguments were not provided:\n    -i <DEV>")
    );
    Ok(())
}

#[test]
fn no_such_input_file() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_zeroed_md(&mut td)?;
    let stderr = run_fail(thin_metadata_pack!("-i", "no-such-file", "-o", &md))?;
    assert!(stderr.contains("Couldn't find input file"));
    Ok(())
}

#[test]
fn missing_output_file() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_zeroed_md(&mut td)?;
    let stderr = run_fail(thin_metadata_pack!("-i", &md))?;
    assert!(stderr
        .contains("error: The following required arguments were not provided:\n    -o <FILE>"));
    Ok(())
}

//------------------------------------------
