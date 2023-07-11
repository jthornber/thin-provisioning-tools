use anyhow::Result;

mod common;

use common::common_args::*;
use common::input_arg::*;
use common::output_option::*;
use common::process::*;
use common::program::*;
use common::target::*;
use common::test_dir::*;
use common::thin::*;

//------------------------------------------

const USAGE: &str = "Unpack a compressed file of thin metadata.

Usage: thin_metadata_unpack [OPTIONS] -i <FILE> -o <DEV>

Options:
  -f, --force    Force overwrite the output file
  -h, --help     Print help
  -i <FILE>      Specify packed input file
  -o <DEV>       Specify thinp metadata binary device/file
  -V, --version  Print version";

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
    fn mk_valid_input(_td: &mut TestDir) -> Result<std::path::PathBuf> {
        path_to(TestData::PackedMetadata)
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
    let md_packed = td.mk_path("meta.pack");
    let md_out = td.mk_path("meta.out");
    run_ok(thin_metadata_pack_cmd(args![
        "-i", &md_in, "-o", &md_packed
    ]))?;
    run_ok(thin_metadata_unpack_cmd(args![
        "-i", &md_packed, "-o", &md_out
    ]))?;

    let dump1 = run_ok(thin_dump_cmd(args![&md_in]))?;
    let dump2 = run_ok(thin_dump_cmd(args![&md_out]))?;
    assert_eq!(dump1, dump2);
    Ok(())
}

//------------------------------------------
