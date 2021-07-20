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

struct CacheCheck;

impl<'a> Program<'a> for CacheCheck {
    fn name() -> &'a str {
        "cache_check"
    }

    fn path() -> &'a std::ffi::OsStr {
        CACHE_CHECK.as_ref()
    }

    fn usage() -> &'a str {
        USAGE
    }

    fn arg_type() -> ArgType {
        ArgType::InputArg
    }

    fn bad_option_hint(option: &str) -> String {
        msg::bad_option_hint(option)
    }
}

impl<'a> InputProgram<'a> for CacheCheck {
    fn mk_valid_input(td: &mut TestDir) -> Result<std::path::PathBuf> {
        mk_valid_md(td)
    }

    fn file_not_found() -> &'a str {
        msg::FILE_NOT_FOUND
    }

    fn missing_input_arg() -> &'a str {
        msg::MISSING_INPUT_ARG
    }

    fn corrupted_input() -> &'a str {
        msg::BAD_SUPERBLOCK
    }
}

impl<'a> BinaryInputProgram<'_> for CacheCheck {}

//------------------------------------------

test_accepts_help!(CacheCheck);
test_accepts_version!(CacheCheck);
test_rejects_bad_option!(CacheCheck);

test_missing_input_arg!(CacheCheck);
test_input_file_not_found!(CacheCheck);
test_input_cannot_be_a_directory!(CacheCheck);
test_unreadable_input_file!(CacheCheck);

test_help_message_for_tiny_input_file!(CacheCheck);
test_spot_xml_data!(CacheCheck);
test_corrupted_input_data!(CacheCheck);

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
