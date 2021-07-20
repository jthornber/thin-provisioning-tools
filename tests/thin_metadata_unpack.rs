use anyhow::Result;

mod common;

use common::common_args::*;
use common::input_arg::*;
use common::output_option::*;
use common::test_dir::*;
use common::*;

//------------------------------------------

const USAGE: &str = concat!(
    "thin_metadata_unpack ",
    include_str!("../VERSION"),
    "Unpack a compressed file of thin metadata.\n\
     \n\
     USAGE:\n    \
         thin_metadata_unpack -i <DEV> -o <FILE>\n\
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

struct ThinMetadataUnpack;

impl<'a> Program<'a> for ThinMetadataUnpack {
    fn name() -> &'a str {
        "thin_metadata_pack"
    }

    fn path() -> &'a std::ffi::OsStr {
        THIN_METADATA_UNPACK.as_ref()
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

impl<'a> InputProgram<'a> for ThinMetadataUnpack {
    fn mk_valid_input(td: &mut TestDir) -> Result<std::path::PathBuf> {
        mk_zeroed_md(td) // FIXME: make a real pack file
    }

    fn file_not_found() -> &'a str {
        rust_msg::FILE_NOT_FOUND
    }

    fn missing_input_arg() -> &'a str {
        rust_msg::MISSING_INPUT_ARG
    }

    fn corrupted_input() -> &'a str {
        "Not a pack file"
    }
}

impl<'a> OutputProgram<'a> for ThinMetadataUnpack {
    fn file_not_found() -> &'a str {
        rust_msg::FILE_NOT_FOUND
    }

    fn missing_output_arg() -> &'a str {
        rust_msg::MISSING_OUTPUT_ARG
    }
}

//------------------------------------------

test_accepts_help!(ThinMetadataUnpack);
test_accepts_version!(ThinMetadataUnpack);
test_rejects_bad_option!(ThinMetadataUnpack);

test_missing_input_option!(ThinMetadataUnpack);
test_input_file_not_found!(ThinMetadataUnpack);
test_corrupted_input_data!(ThinMetadataUnpack);

test_missing_output_option!(ThinMetadataUnpack);

//------------------------------------------

// TODO: share with thin_restore/cache_restore/era_restore

#[test]
fn end_to_end() -> Result<()> {
    let mut td = TestDir::new()?;
    let md_in = mk_valid_md(&mut td)?;
    let md_out = mk_zeroed_md(&mut td)?;
    run_ok(
        THIN_METADATA_PACK,
        &["-i", md_in.to_str().unwrap(), "-o", "meta.pack"],
    )?;
    run_ok(
        THIN_METADATA_UNPACK,
        &["-i", "meta.pack", "-o", md_out.to_str().unwrap()],
    )?;

    let dump1 = run_ok(THIN_DUMP, &[md_in.to_str().unwrap()])?;
    let dump2 = run_ok(THIN_DUMP, &[md_out.to_str().unwrap()])?;
    assert_eq!(dump1, dump2);
    Ok(())
}

//------------------------------------------
