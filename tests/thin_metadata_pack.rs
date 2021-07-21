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

const USAGE: &str = concat!(
    "thin_metadata_pack ",
    include_str!("../VERSION"),
    "Produces a compressed file of thin metadata.  Only packs metadata blocks that are actually used.\n\
     \n\
     USAGE:\n    \
         thin_metadata_pack -i <DEV> -o <FILE>\n\
     \n\
     FLAGS:\n    \
         -h, --help       Prints help information\n    \
         -V, --version    Prints version information\n\
     \n\
     OPTIONS:\n    \
         -i <DEV>         Specify thinp metadata binary device/file\n    \
         -o <FILE>        Specify packed output file"
);

//------------------------------------------

struct ThinMetadataPack;

impl<'a> Program<'a> for ThinMetadataPack {
    fn name() -> &'a str {
        "thin_metadata_pack"
    }

    fn path() -> &'a std::ffi::OsStr {
        THIN_METADATA_PACK.as_ref()
    }

    fn usage() -> &'a str {
        USAGE
    }

    fn arg_type() -> ArgType {
        ArgType::IoOptions
    }

    fn bad_option_hint(option: &str) -> String {
        rust_msg::bad_option_hint(option)
    }
}

impl<'a> InputProgram<'a> for ThinMetadataPack {
    fn mk_valid_input(td: &mut TestDir) -> Result<std::path::PathBuf> {
        mk_valid_md(td)
    }

    fn file_not_found() -> &'a str {
        rust_msg::FILE_NOT_FOUND
    }

    fn missing_input_arg() -> &'a str {
        rust_msg::MISSING_INPUT_ARG
    }

    fn corrupted_input() -> &'a str {
        rust_msg::BAD_SUPERBLOCK
    }
}

impl<'a> OutputProgram<'a> for ThinMetadataPack {
    fn file_not_found() -> &'a str {
        rust_msg::FILE_NOT_FOUND
    }

    fn missing_output_arg() -> &'a str {
        rust_msg::MISSING_OUTPUT_ARG
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
