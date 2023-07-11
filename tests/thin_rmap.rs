use anyhow::Result;

mod common;

use common::common_args::*;
use common::process::*;
use common::program::*;
use common::target::*;
use common::test_dir::*;
use common::thin::*;

//------------------------------------------

const USAGE: &str = "Output reverse map of a thin provisioned region of blocks

Usage: thin_rmap --region <BLOCK_RANGE> <INPUT>

Arguments:
  <INPUT>  Specify the input device

Options:
  -h, --help                  Print help
      --region <BLOCK_RANGE>  Specify range of blocks on the data device
  -V, --version               Print version";

//------------------------------------------

struct ThinRmap;

impl<'a> Program<'a> for ThinRmap {
    fn name() -> &'a str {
        "thin_rmap"
    }

    fn cmd<I>(args: I) -> Command
    where
        I: IntoIterator,
        I::Item: Into<std::ffi::OsString>,
    {
        thin_rmap_cmd(args)
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

test_accepts_help!(ThinRmap);
test_accepts_version!(ThinRmap);
test_rejects_bad_option!(ThinRmap);

//------------------------------------------

#[test]
fn valid_region_format_should_pass() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;
    run_ok(thin_rmap_cmd(args![&md, "--region", "23..7890"]))?;
    Ok(())
}

#[test]
fn invalid_regions_should_fail() -> Result<()> {
    let invalid_regions = [
        "23,7890",
        "23..six",
        "found..7890",
        "89..88",
        "89..89",
        "89..",
        "",
        "89...99",
    ];
    for r in &invalid_regions {
        let mut td = TestDir::new()?;
        let md = mk_valid_md(&mut td)?;
        run_fail(thin_rmap_cmd(args![&md, r]))?;
    }
    Ok(())
}

#[test]
fn multiple_regions_should_pass() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;
    run_ok(thin_rmap_cmd(args![
        &md, "--region", "1..23", "--region", "45..78"
    ]))?;
    Ok(())
}

#[test]
fn junk_input() -> Result<()> {
    let mut td = TestDir::new()?;
    let xml = mk_valid_xml(&mut td)?;
    run_fail(thin_rmap_cmd(args![&xml, "--region", "0..-1"]))?;
    Ok(())
}

//------------------------------------------
