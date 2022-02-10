use anyhow::Result;
use std::fs::OpenOptions;
use std::io::Write;

mod common;

use common::common_args::*;
use common::era::*;
use common::fixture::*;
use common::input_arg::*;
use common::process::*;
use common::program::*;
use common::target::*;
use common::test_dir::*;

//------------------------------------------

const USAGE: &str = concat!(
    "era_dump ",
    thinp::tools_version!(),
    "Dump the era metadata to stdout in XML format

USAGE:
    era_dump [OPTIONS] <INPUT>

ARGS:
    <INPUT>    Specify the input device to dump

OPTIONS:
    -h, --help             Print help information
        --logical          Fold any unprocessed write sets into the final era array
    -o, --output <FILE>    Specify the output file rather than stdout
    -r, --repair           Repair the metadata whilst dumping it
    -V, --version          Print version information"
);

//------------------------------------------

struct EraDump;

impl<'a> Program<'a> for EraDump {
    fn name() -> &'a str {
        "era_dump"
    }

    fn cmd<I>(args: I) -> Command
    where
        I: IntoIterator,
        I::Item: Into<std::ffi::OsString>,
    {
        era_dump_cmd(args)
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

impl<'a> InputProgram<'a> for EraDump {
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

impl<'a> MetadataReader<'a> for EraDump {}

//------------------------------------------

test_accepts_help!(EraDump);
test_accepts_version!(EraDump);
test_rejects_bad_option!(EraDump);

test_missing_input_arg!(EraDump);
test_input_file_not_found!(EraDump);
test_input_cannot_be_a_directory!(EraDump);
test_unreadable_input_file!(EraDump);
test_tiny_input_file!(EraDump);

//------------------------------------------

// TODO: share with thin_dump
#[test]
fn dump_restore_cycle() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;
    let output = run_ok_raw(era_dump_cmd(args![&md]))?;

    let xml = td.mk_path("meta.xml");
    let mut file = OpenOptions::new()
        .read(false)
        .write(true)
        .create(true)
        .open(&xml)?;
    file.write_all(&output.stdout[0..])?;
    drop(file);

    let md2 = mk_zeroed_md(&mut td)?;
    run_ok(era_restore_cmd(args!["-i", &xml, "-o", &md2]))?;

    let output2 = run_ok_raw(era_dump_cmd(args![&md2]))?;
    assert_eq!(output.stdout, output2.stdout);

    Ok(())
}
