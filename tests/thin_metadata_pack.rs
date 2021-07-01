use anyhow::Result;

mod common;

use common::common_args::*;
use common::input_arg::*;
use common::output_option::*;
use common::*;

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

test_accepts_help!(THIN_METADATA_PACK, USAGE);
test_accepts_version!(THIN_METADATA_PACK);
test_rejects_bad_option!(THIN_METADATA_PACK);

test_missing_input_option!(THIN_METADATA_PACK);
test_missing_output_option!(THIN_METADATA_PACK, mk_valid_md);
test_input_file_not_found!(THIN_METADATA_PACK, OPTION);

//------------------------------------------
