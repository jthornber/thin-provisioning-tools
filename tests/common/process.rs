use anyhow::Result;

//------------------------------------------

#[macro_export]
macro_rules! args {
    ( $( $arg: expr ),* ) => {
        {
            use std::ffi::OsStr;
            let args = [$( OsStr::new($arg) ),*];
            args
        }
    };
}

// Returns stdout. The command must return zero.
pub fn run_ok(command: duct::Expression) -> Result<String> {
    let command = command.stdout_capture().stderr_capture();
    let output = command.run()?;
    assert!(output.status.success());
    let stdout = std::str::from_utf8(&output.stdout[..])
        .unwrap()
        .trim_end_matches(|c| c == '\n' || c == '\r')
        .to_string();

    Ok(stdout)
}

// Returns the entire output. The command must return zero.
pub fn run_ok_raw(command: duct::Expression) -> Result<std::process::Output> {
    let command = command.stdout_capture().stderr_capture();
    let output = command.run()?;
    assert!(output.status.success());
    Ok(output)
}

// Returns stderr, a non zero status must be returned
pub fn run_fail(command: duct::Expression) -> Result<String> {
    let command = command.stdout_capture().stderr_capture();
    let output = command.unchecked().run()?;
    assert!(!output.status.success());
    let stderr = std::str::from_utf8(&output.stderr[..]).unwrap().to_string();
    Ok(stderr)
}

// Returns the entire output, a non zero status must be returned
pub fn run_fail_raw(command: duct::Expression) -> Result<std::process::Output> {
    let command = command.stdout_capture().stderr_capture();
    let output = command.unchecked().run()?;
    assert!(!output.status.success());
    Ok(output)
}

//------------------------------------------
