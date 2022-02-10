use anyhow::Result;

mod common;

use common::common_args::*;
use common::fixture::*;
use common::input_arg::*;
use common::output_option::*;
use common::process::*;
use common::program::*;
use common::target::*;
use common::test_dir::*;
use common::thin::*;

//------------------------------------------

const USAGE: &str = concat!(
    "thin_metadata_unpack ",
    thinp::tools_version!(),
    "Unpack a compressed file of thin metadata.

USAGE:
    thin_metadata_unpack -i <DEV> -o <FILE>

OPTIONS:
    -h, --help       Print help information
    -i <DEV>         Specify thinp metadata binary device/file
    -o <FILE>        Specify packed output file
    -V, --version    Print version information"
);

//------------------------------------------

struct ThinMetadataUnpack;

impl<'a> Program<'a> for ThinMetadataUnpack {
    fn name() -> &'a str {
        "thin_metadata_pack"
    }

    fn cmd<I>(args: I) -> Command
    where
        I: IntoIterator,
        I::Item: Into<std::ffi::OsString>,
    {
        thin_metadata_unpack_cmd(args)
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

impl<'a> InputProgram<'a> for ThinMetadataUnpack {
    fn mk_valid_input(td: &mut TestDir) -> Result<std::path::PathBuf> {
        mk_zeroed_md(td) // FIXME: make a real pack file
    }

    fn file_not_found() -> &'a str {
        msg::FILE_NOT_FOUND
    }

    fn missing_input_arg() -> &'a str {
        msg::MISSING_INPUT_ARG
    }

    fn corrupted_input() -> &'a str {
        "Not a pack file"
    }
}

impl<'a> OutputProgram<'a> for ThinMetadataUnpack {
    fn missing_output_arg() -> &'a str {
        msg::MISSING_OUTPUT_ARG
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
    run_ok(thin_metadata_pack_cmd(args![
        "-i",
        &md_in,
        "-o",
        "meta.pack"
    ]))?;
    run_ok(thin_metadata_unpack_cmd(args![
        "-i",
        "meta.pack",
        "-o",
        &md_out
    ]))?;

    let dump1 = run_ok(thin_dump_cmd(args![&md_in]))?;
    let dump2 = run_ok(thin_dump_cmd(args![&md_out]))?;
    assert_eq!(dump1, dump2);
    Ok(())
}

//------------------------------------------
