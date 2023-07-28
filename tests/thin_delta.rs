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

#[test]
fn test_same_dev_id() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?; // TODO: parameterize metadata creation
    let thins = get_thins(&md)?;
    let thin_id = thins.keys().next().unwrap().to_string();

    let stdout = run_ok(thin_delta_cmd(args![
        "--thin1", &thin_id, "--thin2", &thin_id, &md
    ]))?;
    assert_eq!(stdout, format!(
"<superblock uuid=\"\" time=\"0\" transaction=\"1\" data_block_size=\"128\" nr_data_blocks=\"20480\">
  <diff left=\"{}\" right=\"{}\">
    <same begin=\"0\" length=\"1024\"/>
  </diff>
</superblock>", thin_id, thin_id));
    Ok(())
}

#[test]
fn test_same_root() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?; // TODO: parameterize metadata creation
    let thins = get_thins(&md)?;
    let root = thins.values().next().unwrap().0.to_string();

    let stdout = run_ok(thin_delta_cmd(args![
        "--root1", &root, "--root2", &root, &md
    ]))?;
    assert_eq!(stdout, format!(
"<superblock uuid=\"\" time=\"0\" transaction=\"1\" data_block_size=\"128\" nr_data_blocks=\"20480\">
  <diff left_root=\"{}\" right_root=\"{}\">
    <same begin=\"0\" length=\"1024\"/>
  </diff>
</superblock>", root, root));
    Ok(())
}

//------------------------------------------
