use anyhow::Result;

use thinp::tools_version;

use crate::args;
use crate::common::process::*;
use crate::common::program::*;

//------------------------------------------
// help

pub fn test_help_short<'a, P>() -> Result<()>
where
    P: Program<'a>,
{
    let stdout = run_ok(P::cmd(args!["-h"]))?;
    assert_eq!(stdout, P::usage());
    Ok(())
}

pub fn test_help_long<'a, P>() -> Result<()>
where
    P: Program<'a>,
{
    let stdout = run_ok(P::cmd(vec!["--help"]))?;
    assert_eq!(stdout, P::usage());
    Ok(())
}

#[macro_export]
macro_rules! test_accepts_help {
    ($program: ident) => {
        #[test]
        fn accepts_h() -> Result<()> {
            test_help_short::<$program>()
        }

        #[test]
        fn accepts_help() -> Result<()> {
            test_help_long::<$program>()
        }
    };
}

//------------------------------------------
// version

pub fn test_version_short<'a, P>() -> Result<()>
where
    P: Program<'a>,
{
    let stdout = run_ok(P::cmd(args!["-V"]))?;
    assert!(stdout.contains(tools_version!()));
    Ok(())
}

pub fn test_version_long<'a, P>() -> Result<()>
where
    P: Program<'a>,
{
    let stdout = run_ok(P::cmd(args!["--version"]))?;
    assert!(stdout.contains(tools_version!()));
    Ok(())
}

#[macro_export]
macro_rules! test_accepts_version {
    ($program: ident) => {
        #[test]
        fn accepts_v() -> Result<()> {
            test_version_short::<$program>()
        }

        #[test]
        fn accepts_version() -> Result<()> {
            test_version_long::<$program>()
        }
    };
}

//------------------------------------------

pub fn test_rejects_bad_option<'a, P>() -> Result<()>
where
    P: Program<'a>,
{
    let option = "--hedgehogs-only";
    let stderr = run_fail(P::cmd(args![option]))?;
    assert!(stderr.contains(&P::bad_option_hint(option)));
    Ok(())
}

#[macro_export]
macro_rules! test_rejects_bad_option {
    ($program: ident) => {
        #[test]
        fn rejects_bad_option() -> Result<()> {
            test_rejects_bad_option::<$program>()
        }
    };
}

//------------------------------------------
