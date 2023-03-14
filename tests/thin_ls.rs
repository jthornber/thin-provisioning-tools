use anyhow::Result;

mod common;

use common::common_args::*;
use common::input_arg::*;
use common::process::*;
use common::program::*;
use common::target::*;
use common::test_dir::*;
use common::thin::*;

use thinp::tools_version;

//------------------------------------------

const USAGE: &str = concat!(
    "thin_ls ",
    tools_version!(),
    "
List thin volumes within a pool

USAGE:
    thin_ls [OPTIONS] <INPUT>

ARGS:
    <INPUT>    Specify the input device

OPTIONS:
    -h, --help               Print help information
    -m, --metadata-snap      Use metadata snapshot
        --no-headers         Don't output headers
    -o, --format <FIELDS>    Give a comma separated list of fields to be output
    -V, --version            Print version information"
);

//-----------------------------------------

struct ThinLs;

impl<'a> Program<'a> for ThinLs {
    fn name() -> &'a str {
        "thin_ls"
    }

    fn cmd<I>(args: I) -> Command
    where
        I: IntoIterator,
        I::Item: Into<std::ffi::OsString>,
    {
        thin_ls_cmd(args)
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

impl<'a> InputProgram<'a> for ThinLs {
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

test_accepts_help!(ThinLs);
test_accepts_version!(ThinLs);
test_rejects_bad_option!(ThinLs);

test_missing_input_arg!(ThinLs);
test_input_file_not_found!(ThinLs);
test_input_cannot_be_a_directory!(ThinLs);
test_unreadable_input_file!(ThinLs);

test_readonly_input_file!(ThinLs);

//------------------------------------------
// test reading metadata snapshot from a live metadata.
// here we use a corrupted metadata to ensure that "thin_ls -m" reads the
// metadata snapshot only.

#[test]
fn read_metadata_snapshot() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = prep_metadata_from_file(&mut td, "corrupted_tmeta_with_metadata_snap.pack")?;
    let _ = run_ok(thin_ls_cmd(args![&md, "-m"]))?;
    Ok(())
}

//------------------------------------------
