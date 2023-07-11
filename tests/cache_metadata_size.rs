use anyhow::Result;

mod common;

use common::common_args::*;
use common::process::*;
use common::program::*;
use common::target::*;

//------------------------------------------

const USAGE: &str = "Estimate the size of the metadata device needed for a given configuration.

Usage: cache_metadata_size [OPTIONS] <--device-size <SIZE> --block-size <SIZE> | --nr-blocks <NUM>>

Options:
      --block-size <SIZE[bskmg]>     Specify the size of each cache block
      --device-size <SIZE[bskmgtp]>  Specify total size of the fast device used in the cache
  -h, --help                         Print help
      --max-hint-width <BYTES>       Specity the per-block hint width [default: 4]
  -n, --numeric-only[=<OPT>]         Output numeric value only
      --nr-blocks <NUM>              Specify the number of cache blocks
  -u, --unit <UNIT>                  Specify the output unit in {bskKmMgG} [default: sector]
  -V, --version                      Print version";

//------------------------------------------

struct CacheMetadataSize;

impl<'a> Program<'a> for CacheMetadataSize {
    fn name() -> &'a str {
        "cache_metadata_size"
    }

    fn cmd<I>(args: I) -> Command
    where
        I: IntoIterator,
        I::Item: Into<std::ffi::OsString>,
    {
        cache_metadata_size_cmd(args)
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

test_accepts_help!(CacheMetadataSize);
test_accepts_version!(CacheMetadataSize);
test_rejects_bad_option!(CacheMetadataSize);

//------------------------------------------

#[test]
fn no_args() -> Result<()> {
    let _stderr = run_fail(cache_metadata_size_cmd([""; 0]))?;
    Ok(())
}

#[test]
fn device_size_only() -> Result<()> {
    let _stderr = run_fail(cache_metadata_size_cmd(args!["--device-size", "204800"]))?;
    Ok(())
}

#[test]
fn block_size_only() -> Result<()> {
    let _stderr = run_fail(cache_metadata_size_cmd(args!["--block-size", "128"]))?;
    Ok(())
}

/*
#[test]
fn conradictory_info_fails() -> Result<()> {
    let stderr = run_fail(cache_metadata_size_cmd(
        args![
            "--device-size",
            "102400",
            "--block-size",
            "1000",
            "--nr-blocks",
            "6"
        ],
    ))?;
    assert_eq!(stderr, "Contradictory arguments given, --nr-blocks doesn't match the --device-size and --block-size.");
    Ok(())
}

#[test]
fn all_args_agree() -> Result<()> {
    let out = run_ok_raw(cache_metadata_size_cmd(
        args![
            "--device-size",
            "102400",
            "--block-size",
            "100",
            "--nr-blocks",
            "1024"
        ],
    ))?;
    let stdout = std::str::from_utf8(&out.stdout[..])
        .unwrap()
        .trim_end_matches(|c| c == '\n' || c == '\r')
        .to_string();
    assert_eq!(stdout, "8248 sectors");
    assert_eq!(out.stderr.len(), 0);
    Ok(())
}
*/

#[test]
fn dev_size_and_nr_blocks_conflicts() -> Result<()> {
    run_fail(cache_metadata_size_cmd(args![
        "--device-size",
        "102400",
        "--nr-blocks",
        "1024"
    ]))?;
    Ok(())
}

#[test]
fn block_size_and_nr_blocks_conflicts() -> Result<()> {
    run_fail(cache_metadata_size_cmd(args![
        "--block-size",
        "64",
        "--nr-blocks",
        "1024"
    ]))?;
    Ok(())
}

#[test]
fn nr_blocks_alone() -> Result<()> {
    let out = run_ok_raw(cache_metadata_size_cmd(args!["--nr-blocks", "1024"]))?;
    let stdout = std::str::from_utf8(&out.stdout[..])
        .unwrap()
        .trim_end_matches(|c| c == '\n' || c == '\r')
        .to_string();
    assert_eq!(stdout, "8248 sectors");
    assert_eq!(out.stderr.len(), 0);
    Ok(())
}

#[test]
fn dev_size_and_block_size_succeeds() -> Result<()> {
    let out = run_ok_raw(cache_metadata_size_cmd(args![
        "--device-size",
        "65536",
        "--block-size",
        "64"
    ]))?;
    let stdout = std::str::from_utf8(&out.stdout[..])
        .unwrap()
        .trim_end_matches(|c| c == '\n' || c == '\r')
        .to_string();
    assert_eq!(stdout, "8248 sectors");
    assert_eq!(out.stderr.len(), 0);
    Ok(())
}

#[test]
fn large_nr_blocks() -> Result<()> {
    let out = run_ok_raw(cache_metadata_size_cmd(args!["--nr-blocks", "67108864"]))?;
    let stdout = std::str::from_utf8(&out.stdout[..])
        .unwrap()
        .trim_end_matches(|c| c == '\n' || c == '\r')
        .to_string();
    assert_eq!(stdout, "3678208 sectors");
    assert_eq!(out.stderr.len(), 0);
    Ok(())
}

#[test]
fn test_valid_block_sizes() -> Result<()> {
    let block_sizes = [64, 128, 192, 2097152];
    for bs in block_sizes {
        let bs = bs.to_string();
        run_ok(cache_metadata_size_cmd(args![
            "--device-size",
            "16777216",
            "--block-size",
            &bs
        ]))?;
    }
    Ok(())
}

#[test]
fn invalid_block_size_should_fail() -> Result<()> {
    let block_sizes = [0, 32, 63, 2097153, 2097218, 4194304];
    for bs in block_sizes {
        let bs = bs.to_string();
        run_fail(cache_metadata_size_cmd(args![
            "--device-size",
            "16777216",
            "--block-size",
            &bs
        ]))?;
    }
    Ok(())
}

//------------------------------------------
