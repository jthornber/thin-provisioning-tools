use anyhow::Result;
use std::ffi::{OsStr, OsString};

//------------------------------------------

// Returns stdout. The command must return zero.
pub fn run_ok<S, I>(program: S, args: I) -> Result<String>
where
    S: AsRef<OsStr>,
    I: IntoIterator,
    I::Item: Into<OsString>,
{
    let command = duct::cmd(program.as_ref(), args)
        .stdout_capture()
        .stderr_capture();
    let output = command.run()?;
    assert!(output.status.success());
    let stdout = std::str::from_utf8(&output.stdout[..])
        .unwrap()
        .trim_end_matches(|c| c == '\n' || c == '\r')
        .to_string();
    Ok(stdout)
}

// Returns the entire output. The command must return zero.
pub fn run_ok_raw<S, I>(program: S, args: I) -> Result<std::process::Output>
where
    S: AsRef<OsStr>,
    I: IntoIterator,
    I::Item: Into<OsString>,
{
    let command = duct::cmd(program.as_ref(), args)
        .stdout_capture()
        .stderr_capture();
    let output = command.run()?;
    assert!(output.status.success());
    Ok(output)
}

// Returns stderr, a non zero status must be returned
pub fn run_fail<S, I>(program: S, args: I) -> Result<String>
where
    S: AsRef<OsStr>,
    I: IntoIterator,
    I::Item: Into<OsString>,
{
    let command = duct::cmd(program.as_ref(), args)
        .stdout_capture()
        .stderr_capture();
    let output = command.unchecked().run()?;
    assert!(!output.status.success());
    let stderr = std::str::from_utf8(&output.stderr[..]).unwrap().to_string();
    Ok(stderr)
}

// Returns the entire output, a non zero status must be returned
pub fn run_fail_raw<S, I>(program: S, args: I) -> Result<std::process::Output>
where
    S: AsRef<OsStr>,
    I: IntoIterator,
    I::Item: Into<OsString>,
{
    let command = duct::cmd(program.as_ref(), args)
        .stdout_capture()
        .stderr_capture();
    let output = command.unchecked().run()?;
    assert!(!output.status.success());
    Ok(output)
}

//------------------------------------------
