use anyhow::Result;

mod common;

use common::common_args::*;
use common::input_arg::*;
use common::output_option::*;
use common::program::*;
use common::target::*;
use common::test_dir::*;
use common::thin::*;

//------------------------------------------

const USAGE: &str = "Produces a compressed file of thin metadata.  Only packs metadata blocks that are actually used.

Usage: thin_metadata_pack [OPTIONS] -i <DEV> -o <FILE>

Options:
  -f, --force    Force overwrite the output file
  -h, --help     Print help
  -i <DEV>       Specify thinp metadata binary device/file
  -o <FILE>      Specify packed output file
  -V, --version  Print version";

//------------------------------------------

struct ThinMetadataPack;

impl<'a> Program<'a> for ThinMetadataPack {
    fn name() -> &'a str {
        "thin_metadata_pack"
    }

    fn cmd<I>(args: I) -> Command
    where
        I: IntoIterator,
        I::Item: Into<std::ffi::OsString>,
    {
        thin_metadata_pack_cmd(args)
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

impl<'a> InputProgram<'a> for ThinMetadataPack {
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

impl<'a> OutputProgram<'a> for ThinMetadataPack {
    fn missing_output_arg() -> &'a str {
        msg::MISSING_OUTPUT_ARG
    }
}

//------------------------------------------

test_accepts_help!(ThinMetadataPack);
test_accepts_version!(ThinMetadataPack);
test_rejects_bad_option!(ThinMetadataPack);

test_missing_input_option!(ThinMetadataPack);
test_missing_output_option!(ThinMetadataPack);
test_input_file_not_found!(ThinMetadataPack);

//-----------------------------------------
