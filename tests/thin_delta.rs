use anyhow::Result;

mod common;

use common::common_args::*;
use common::process::*;
use common::program::*;
use common::target::*;
use common::test_dir::*;
use common::thin::*;

//------------------------------------------

const USAGE: &str = "Usage: thin_delta [options] <device or file>\n\
                     Options:\n  \
                       {--thin1, --snap1, --root1}\n  \
                       {--thin2, --snap2, --root2}\n  \
                       {-m, --metadata-snap} [block#]\n  \
                       {--verbose}\n  \
                       {-h|--help}\n  \
                       {-V|--version}";

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
        rust_cmd("thin_delta", args)
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
    assert!(stderr.contains("--snap1 or --root1 not specified"));
    Ok(())
}

#[test]
fn snap2_unspecified() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;
    let stderr = run_fail(thin_delta_cmd(args!["--snap1", "45", &md]))?;
    assert!(stderr.contains("--snap2 or --root2 not specified"));
    Ok(())
}

#[test]
fn dev_unspecified() -> Result<()> {
    let stderr = run_fail(thin_delta_cmd(args!["--snap1", "45", "--snap2", "46"]))?;
    // TODO: replace with msg::MISSING_INPUT_ARG once the rust version is ready
    assert!(stderr.contains("No input file provided"));
    Ok(())
}

//------------------------------------------
