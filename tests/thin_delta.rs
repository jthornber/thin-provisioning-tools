use anyhow::Result;

mod common;

use common::common_args::*;
use common::process::*;
use common::program::*;
use common::target::*;
use common::test_dir::*;
use common::thin::*;

//------------------------------------------

const USAGE: &str = "Print the differences in the mappings between two thin devices

Usage: thin_delta [OPTIONS] <--root1 <BLOCKNR>|--thin1 <DEV_ID>> <--root2 <BLOCKNR>|--thin2 <DEV_ID>> <INPUT>

Arguments:
  <INPUT>  Specify the input device

Options:
  -h, --help             Print help
  -m, --metadata-snap    Use metadata snapshot
      --root1 <BLOCKNR>  The root block for the first thin volume to diff
      --root2 <BLOCKNR>  The root block for the second thin volume to diff
      --thin1 <DEV_ID>   The numeric identifier for the first thin volume to diff [aliases: snap1]
      --thin2 <DEV_ID>   The numeric identifier for the second thin volume to diff [aliases: snap2]
  -V, --version          Print version
      --verbose          Provide extra information on the mappings";

//------------------------------------------

struct ThinDelta;

impl<'a> Program<'a> for ThinDelta {
    fn name() -> &'a str {
        "thin_delta"
    }

    fn cmd<I>(args: I) -> Command
    where
        I: IntoIterator,
        I::Item: Into<std::ffi::OsString>,
    {
        thin_delta_cmd(args)
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

//------------------------------------------

test_accepts_help!(ThinDelta);
test_accepts_version!(ThinDelta);
test_rejects_bad_option!(ThinDelta);

//------------------------------------------

#[test]
fn snap1_unspecified() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;
    let stderr = run_fail(thin_delta_cmd(args!["--snap2", "45", &md]))?;
    assert!(stderr.contains(
        "the following required arguments were not provided:
  <--root1 <BLOCKNR>|--thin1 <DEV_ID>>"
    ));
    Ok(())
}

#[test]
fn snap2_unspecified() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;
    let stderr = run_fail(thin_delta_cmd(args!["--snap1", "45", &md]))?;
    assert!(stderr.contains(
        "the following required arguments were not provided:
  <--root2 <BLOCKNR>|--thin2 <DEV_ID>>"
    ));
    Ok(())
}

#[test]
fn missing_input_arg() -> Result<()> {
    let stderr = run_fail(thin_delta_cmd(args!["--snap1", "45", "--snap2", "46"]))?;
    assert!(stderr.contains(msg::MISSING_INPUT_ARG));
    Ok(())
}

//------------------------------------------
