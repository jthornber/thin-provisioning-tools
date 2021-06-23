use anyhow::Result;
use duct::cmd;
use thinp::version::tools_version;

mod common;

use common::test_dir::*;
use common::*;

//------------------------------------------

#[test]
fn accepts_v() -> Result<()> {
    let stdout = cache_check!("-V").read()?;
    assert!(stdout.contains(tools_version()));
    Ok(())
}

#[test]
fn accepts_version() -> Result<()> {
    let stdout = cache_check!("--version").read()?;
    assert!(stdout.contains(tools_version()));
    Ok(())
}

const USAGE: &str = "Usage: cache_check [options] {device|file}\nOptions:\n  {-q|--quiet}\n  {-h|--help}\n  {-V|--version}\n  {--clear-needs-check-flag}\n  {--super-block-only}\n  {--skip-mappings}\n  {--skip-hints}\n  {--skip-discards}";

#[test]
fn accepts_h() -> Result<()> {
    let stdout = cache_check!("-h").read()?;
    assert_eq!(stdout, USAGE);
    Ok(())
}

#[test]
fn accepts_help() -> Result<()> {
    let stdout = cache_check!("--help").read()?;
    assert_eq!(stdout, USAGE);
    Ok(())
}

#[test]
fn missing_input_arg() -> Result<()> {
    let stderr = run_fail(cache_check!())?;
    assert!(stderr.contains(msg::MISSING_INPUT_ARG));
    Ok(())
}

#[test]
fn input_file_not_found() -> Result<()> {
    let stderr = run_fail(cache_check!("/arbitrary/filename"))?;
    assert!(stderr.contains(msg::FILE_NOT_FOUND));
    Ok(())
}

#[test]
fn metadata_cannot_be_a_directory() -> Result<()> {
    let stderr = run_fail(cache_check!("/tmp"))?;
    assert!(stderr.contains("Not a block device or regular file"));
    Ok(())
}

#[test]
fn unreadable_metadata() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;
    cmd!("chmod", "-r", &md).run()?;
    let stderr = run_fail(cache_check!(&md))?;
    assert!(stderr.contains("Permission denied"));
    Ok(())
}

#[test]
fn corrupt_metadata() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_zeroed_md(&mut td)?;
    run_fail(cache_check!(&md))?;
    Ok(())
}

#[test]
fn failing_q() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_zeroed_md(&mut td)?;
    let output = cache_check!("-q", &md).unchecked().run()?;
    assert!(!output.status.success());
    assert_eq!(output.stdout.len(), 0);
    assert_eq!(output.stderr.len(), 0);
    Ok(())
}

#[test]
fn failing_quiet() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_zeroed_md(&mut td)?;
    let output = cache_check!("--quiet", &md).unchecked().run()?;
    assert!(!output.status.success());
    assert_eq!(output.stdout.len(), 0);
    assert_eq!(output.stderr.len(), 0);
    Ok(())
}

//  (define-scenario (cache-check valid-metadata-passes)
//    "A valid metadata area passes"
//    (with-valid-metadata (md)
//      (run-ok (cache-check md))))
//
//  (define-scenario (cache-check bad-metadata-version)
//    "Invalid metadata version fails"
//    (with-cache-xml (xml)
//      (with-empty-metadata (md)
//        (cache-restore "-i" xml "-o" md "--debug-override-metadata-version" "12345")
//        (run-fail (cache-check md)))))
//
//  (define-scenario (cache-check tiny-metadata)
//    "Prints helpful message in case tiny metadata given"
//    (with-temp-file-sized ((md "cache.bin" 1024))
//      (run-fail-rcv (_ stderr) (cache-check md)
//        (assert-starts-with "Metadata device/file too small.  Is this binary metadata?" stderr))))
//
//  (define-scenario (cache-check spot-accidental-xml-data)
//    "Prints helpful message if XML metadata given"
//    (with-cache-xml (xml)
//      (system (fmt #f "man bash >> " xml))
//      (run-fail-rcv (_ stderr) (cache-check xml)
//        (assert-matches ".*This looks like XML.  cache_check only checks the binary metadata format." stderr))))
//
