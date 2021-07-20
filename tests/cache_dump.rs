use anyhow::Result;

mod common;

use common::common_args::*;
use common::input_arg::*;
use common::test_dir::*;
use common::*;

//------------------------------------------

const USAGE: &str = "Usage: cache_dump [options] {device|file}\n\
                     Options:\n  \
                       {-h|--help}\n  \
                       {-o <xml file>}\n  \
                       {-V|--version}\n  \
                       {--repair}";

//------------------------------------------

struct CacheDump;

impl<'a> Program<'a> for CacheDump {
    fn name() -> &'a str {
        "cache_dump"
    }

    fn path() -> &'a std::ffi::OsStr {
        CACHE_DUMP.as_ref()
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

impl<'a> InputProgram<'a> for CacheDump {
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

//------------------------------------------

test_accepts_help!(CacheDump);
test_accepts_version!(CacheDump);
test_rejects_bad_option!(CacheDump);

test_missing_input_arg!(CacheDump);
test_input_file_not_found!(CacheDump);
test_input_cannot_be_a_directory!(CacheDump);
test_unreadable_input_file!(CacheDump);

//------------------------------------------

/*
  (define-scenario (cache-dump restore-is-noop)
    "cache_dump followed by cache_restore is a noop."
    (with-valid-metadata (md)
      (run-ok-rcv (d1-stdout _) (cache-dump md)
        (with-temp-file-containing ((xml "cache.xml" d1-stdout))
          (run-ok (cache-restore "-i" xml "-o" md))
          (run-ok-rcv (d2-stdout _) (cache-dump md)
            (assert-equal d1-stdout d2-stdout))))))
*/
