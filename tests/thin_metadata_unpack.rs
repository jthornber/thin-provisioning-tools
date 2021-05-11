use anyhow::Result;
use thinp::version::TOOLS_VERSION;

mod common;
use common::test_dir::*;
use common::*;

//------------------------------------------

#[test]
fn accepts_v() -> Result<()> {
    let stdout = thin_metadata_unpack!("-V").read()?;
    assert!(stdout.contains(TOOLS_VERSION));
    Ok(())
}

#[test]
fn accepts_version() -> Result<()> {
    let stdout = thin_metadata_unpack!("--version").read()?;
    assert!(stdout.contains(TOOLS_VERSION));
    Ok(())
}

const USAGE: &str = "thin_metadata_unpack 0.9.0\nUnpack a compressed file of thin metadata.\n\nUSAGE:\n    thin_metadata_unpack -i <DEV> -o <FILE>\n\nFLAGS:\n    -h, --help       Prints help information\n    -V, --version    Prints version information\n\nOPTIONS:\n    -i <DEV>         Specify thinp metadata binary device/file\n    -o <FILE>        Specify packed output file";

#[test]
fn accepts_h() -> Result<()> {
    let stdout = thin_metadata_unpack!("-h").read()?;
    assert_eq!(stdout, USAGE);
    Ok(())
}

#[test]
fn accepts_help() -> Result<()> {
    let stdout = thin_metadata_unpack!("--help").read()?;
    assert_eq!(stdout, USAGE);
    Ok(())
}

#[test]
fn rejects_bad_option() -> Result<()> {
    let stderr = run_fail(thin_metadata_unpack!("--hedgehogs-only"))?;
    assert!(stderr.contains("Found argument \'--hedgehogs-only\'"));
    Ok(())
}

#[test]
fn missing_input_file() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_zeroed_md(&mut td)?;
    let stderr = run_fail(thin_metadata_unpack!("-o", &md))?;
    assert!(
        stderr.contains("error: The following required arguments were not provided:\n    -i <DEV>")
    );
    Ok(())
}

#[test]
fn no_such_input_file() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_zeroed_md(&mut td)?;
    let stderr = run_fail(thin_metadata_unpack!("-i", "no-such-file", "-o", &md))?;
    assert!(stderr.contains("Couldn't find input file"));
    Ok(())
}

#[test]
fn missing_output_file() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_zeroed_md(&mut td)?;
    let stderr = run_fail(thin_metadata_unpack!("-i", &md))?;
    assert!(stderr
        .contains("error: The following required arguments were not provided:\n    -o <FILE>"));
    Ok(())
}

#[test]
fn garbage_input_file() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_zeroed_md(&mut td)?;
    let stderr = run_fail(thin_metadata_unpack!("-i", &md, "-o", "junk"))?;
    assert!(stderr.contains("Not a pack file."));
    Ok(())
}

#[test]
fn end_to_end() -> Result<()> {
    let mut td = TestDir::new()?;
    let md_in = mk_valid_md(&mut td)?;
    let md_out = mk_zeroed_md(&mut td)?;
    thin_metadata_pack!("-i", &md_in, "-o", "meta.pack").run()?;
    thin_metadata_unpack!("-i", "meta.pack", "-o", &md_out).run()?;

    let dump1 = thin_dump!(&md_in).read()?;
    let dump2 = thin_dump!(&md_out).read()?;
    assert_eq!(dump1, dump2);
    Ok(())
}

//------------------------------------------
