use anyhow::Result;

mod common;

use common::common_args::*;
use common::input_arg::*;
use common::test_dir::*;
use common::*;

//------------------------------------------

const USAGE: &str = "Usage: cache_check [options] {device|file}\n\
                     Options:\n  \
                       {-q|--quiet}\n  \
                       {-h|--help}\n  \
                       {-V|--version}\n  \
                       {--clear-needs-check-flag}\n  \
                       {--super-block-only}\n  \
                       {--skip-mappings}\n  \
                       {--skip-hints}\n  \
                       {--skip-discards}";

//------------------------------------------

test_accepts_help!(CACHE_CHECK, USAGE);
test_accepts_version!(CACHE_CHECK);
test_rejects_bad_option!(CACHE_CHECK);

test_missing_input_arg!(CACHE_CHECK);
test_input_file_not_found!(CACHE_CHECK, ARG);
test_input_cannot_be_a_directory!(CACHE_CHECK, ARG);
test_unreadable_input_file!(CACHE_CHECK, ARG);

test_help_message_for_tiny_input_file!(CACHE_CHECK, ARG);
test_spot_xml_data!(CACHE_CHECK, "cache_check", ARG);
test_corrupted_input_data!(CACHE_CHECK, ARG);

//------------------------------------------

#[test]
fn failing_q() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_zeroed_md(&mut td)?;
    let output = run_fail_raw(CACHE_CHECK, &["-q", md.to_str().unwrap()])?;
    assert_eq!(output.stdout.len(), 0);
    assert_eq!(output.stderr.len(), 0);
    Ok(())
}

#[test]
fn failing_quiet() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_zeroed_md(&mut td)?;
    let output = run_fail_raw(CACHE_CHECK, &["--quiet", md.to_str().unwrap()])?;
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
