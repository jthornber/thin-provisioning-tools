use anyhow::Result;

mod common;

use common::common_args::*;
use common::process::*;
use common::program::*;
use common::target::*;

//------------------------------------------

const USAGE: &str = "Estimate the size of the metadata device needed for a given configuration.

Usage: thin_metadata_size [OPTIONS] --block-size <SIZE[bskmg]> --pool-size <SIZE[bskmgtp]> --max-thins <NUM>

Options:
  -b, --block-size <SIZE[bskmg]>   Specify the data block size
  -h, --help                       Print help
  -m, --max-thins <NUM>            Maximum number of thin devices and snapshots
  -n, --numeric-only[=<OPT>]       Output numeric value only
  -s, --pool-size <SIZE[bskmgtp]>  Specify the size of pool device
  -u, --unit <UNIT>                Specify the output unit in {bskKmMgG} [default: sector]
  -V, --version                    Print version";

//------------------------------------------

struct ThinMetadataSize;

impl<'a> Program<'a> for ThinMetadataSize {
    fn name() -> &'a str {
        "thin_metadata_size"
    }

    fn cmd<I>(args: I) -> Command
    where
        I: IntoIterator,
        I::Item: Into<std::ffi::OsString>,
    {
        thin_metadata_size_cmd(args)
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

test_accepts_help!(ThinMetadataSize);
test_accepts_version!(ThinMetadataSize);
test_rejects_bad_option!(ThinMetadataSize);

//------------------------------------------

#[test]
fn no_args() -> Result<()> {
    let _stderr = run_fail(thin_metadata_size_cmd([""; 0]))?;
    Ok(())
}

#[test]
fn missing_block_size_should_fail() -> Result<()> {
    let _stderr = run_fail(thin_metadata_size_cmd(args![
        "--pool-size",
        "2097152",
        "-m",
        "1"
    ]))?;
    Ok(())
}

#[test]
fn missing_pool_size_should_fail() -> Result<()> {
    let _stderr = run_fail(thin_metadata_size_cmd(args![
        "--block-size",
        "128",
        "-m",
        "1"
    ]))?;
    Ok(())
}

#[test]
fn missing_nr_thins_should_fail() -> Result<()> {
    let _stderr = run_fail(thin_metadata_size_cmd(args![
        "--pool-size",
        "2097152",
        "--block-size",
        "128"
    ]))?;
    Ok(())
}

#[test]
fn dev_size_and_block_size_succeeds() -> Result<()> {
    let out = run_ok_raw(thin_metadata_size_cmd(args![
        "--pool-size",
        "2097152",
        "--block-size",
        "128",
        "-m",
        "1"
    ]))?;
    let stdout = std::str::from_utf8(&out.stdout[..])
        .unwrap()
        .trim_end_matches(|c| c == '\n' || c == '\r')
        .to_string();
    assert_eq!(stdout, "1064 sectors");
    assert_eq!(out.stderr.len(), 0);
    Ok(())
}

#[test]
fn test_valid_block_sizes() -> Result<()> {
    let block_sizes = [128, 256, 384, 2097152];
    for bs in block_sizes {
        let bs = bs.to_string();
        run_ok(thin_metadata_size_cmd(args![
            "--pool-size",
            "16777216",
            "--block-size",
            &bs,
            "-m",
            "1"
        ]))?;
    }
    Ok(())
}

#[test]
fn invalid_block_size_should_fail() -> Result<()> {
    let block_sizes = [0, 64, 127, 2097153, 2097280, 4194304];
    for bs in block_sizes {
        let bs = bs.to_string();
        run_fail(thin_metadata_size_cmd(args![
            "--pool-size",
            "16777216",
            "--block-size",
            &bs,
            "-m",
            "1"
        ]))?;
    }
    Ok(())
}

//------------------------------------------
