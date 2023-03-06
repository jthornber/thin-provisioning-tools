use anyhow::Result;

mod common;

use common::cache::*;
use common::common_args::*;
use common::fixture::*;
use common::input_arg::*;
use common::output_option::*;
use common::program::*;
use common::target::*;
use common::test_dir::*;

use thinp::tools_version;

//------------------------------------------

const USAGE: &str = concat!(
    "cache_repair ",
    tools_version!(),
    "
Repair binary cache metadata, and write it to a different device or file

USAGE:
    cache_repair [OPTIONS] --input <FILE> --output <FILE>

OPTIONS:
    -h, --help             Print help information
    -i, --input <FILE>     Specify the input device
    -o, --output <FILE>    Specify the output device
    -q, --quiet            Suppress output messages, return only exit code.
    -V, --version          Print version information"
);

//-----------------------------------------

struct CacheRepair;

impl<'a> Program<'a> for CacheRepair {
    fn name() -> &'a str {
        "cache_repair"
    }

    fn cmd<I>(args: I) -> Command
    where
        I: IntoIterator,
        I::Item: Into<std::ffi::OsString>,
    {
        cache_repair_cmd(args)
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

test_readonly_input_file!(CacheRepair);

//-----------------------------------------
// accepts empty argument

#[test]
fn accepts_empty_argument() -> Result<()> {
    let mut td = TestDir::new()?;
    let input = mk_valid_md(&mut td)?;
    let output = mk_zeroed_md(&mut td)?;
    run_ok(cache_repair_cmd(args!["-i", &input, "-o", &output, ""]))?;
    Ok(())
}

//-----------------------------------------
