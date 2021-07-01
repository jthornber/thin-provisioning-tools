use crate::common::*;
use thinp::version::tools_version;

//------------------------------------------
// help

pub fn test_help_short(program: &str, usage: &str) -> Result<()> {
    let stdout = run_ok(program, &["-h"])?;
    assert_eq!(stdout, usage);
    Ok(())
}

pub fn test_help_long(program: &str, usage: &str) -> Result<()> {
    let stdout = run_ok(program, &["--help"])?;
    assert_eq!(stdout, usage);
    Ok(())
}

#[macro_export]
macro_rules! test_accepts_help {
    ($program: ident, $usage: expr) => {
        #[test]
        fn accepts_h() -> Result<()> {
            test_help_short($program, $usage)
        }

        #[test]
        fn accepts_help() -> Result<()> {
            test_help_long($program, $usage)
        }
    };
}

//------------------------------------------
// version

pub fn test_version_short(program: &str) -> Result<()> {
    let stdout = run_ok(program, &["-V"])?;
    assert!(stdout.contains(tools_version()));
    Ok(())
}

pub fn test_version_long(program: &str) -> Result<()> {
    let stdout = run_ok(program, &["--version"])?;
    assert!(stdout.contains(tools_version()));
    Ok(())
}

#[macro_export]
macro_rules! test_accepts_version {
    ($program: ident) => {
        #[test]
        fn accepts_v() -> Result<()> {
            test_version_short($program)
        }

        #[test]
        fn accepts_version() -> Result<()> {
            test_version_long($program)
        }
    };
}

//------------------------------------------

pub fn test_rejects_bad_option(program: &str) -> Result<()> {
    let stderr = run_fail(program, &["--hedgehogs-only"])?;
    assert!(stderr.contains("unrecognized option \'--hedgehogs-only\'"));
    Ok(())
}

#[macro_export]
macro_rules! test_rejects_bad_option {
    ($program: ident) => {
        #[test]
        fn rejects_bad_option() -> Result<()> {
            test_rejects_bad_option($program)
        }
    };
}

//------------------------------------------
