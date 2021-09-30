use anyhow::Result;

mod common;

use common::cache::*;
use common::common_args::*;
use common::input_arg::*;
use common::output_option::*;
use common::program::*;
use common::target::*;
use common::test_dir::*;

//------------------------------------------

const USAGE: &str = "Usage: cache_repair [options] {device|file}\n\
                     Options:\n  \
                       {-h|--help}\n  \
                       {-i|--input} <input metadata (binary format)>\n  \
                       {-o|--output} <output metadata (binary format)>\n  \
                       {-V|--version}";

//-----------------------------------------

struct CacheRepair;

impl<'a> Program<'a> for CacheRepair {
    fn name() -> &'a str {
        "cache_repair"
    }

    fn path() -> &'a std::ffi::OsStr {
        CACHE_REPAIR.as_ref()
    }

    fn usage() -> &'a str {
        USAGE
    }

    fn arg_type() -> ArgType {
        ArgType::IoOptions
    }

    fn bad_option_hint(option: &str) -> String {
        msg::bad_option_hint(option)
    }
}

impl<'a> InputProgram<'a> for CacheRepair {
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
        "bad checksum in superblock"
    }
}

impl<'a> OutputProgram<'a> for CacheRepair {
    fn missing_output_arg() -> &'a str {
        msg::MISSING_OUTPUT_ARG
    }
}

impl<'a> MetadataWriter<'a> for CacheRepair {
    fn file_not_found() -> &'a str {
        msg::FILE_NOT_FOUND
    }
}

//-----------------------------------------

test_accepts_help!(CacheRepair);
test_accepts_version!(CacheRepair);
test_rejects_bad_option!(CacheRepair);

test_input_file_not_found!(CacheRepair);
test_input_cannot_be_a_directory!(CacheRepair);
test_corrupted_input_data!(CacheRepair);

test_missing_output_option!(CacheRepair);

//-----------------------------------------
