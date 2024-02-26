use anyhow::Result;
use std::ffi::OsString;
use std::fmt;
use std::process;

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

#[cfg(not(feature = "no_cleanup"))]
macro_rules! ensure_success {
    ( $status: expr ) => {
        if !$status.success() {
            return Err(anyhow::anyhow!("command failed with {}", $status));
        }
    };
}

#[cfg(not(feature = "no_cleanup"))]
macro_rules! ensure_fail {
    ( $status: expr ) => {
        if $status.success() {
            return Err(anyhow::anyhow!("command succeeded without error"));
        }
    };
}

#[cfg(feature = "no_cleanup")]
macro_rules! ensure_success {
    ( $status: expr ) => {
        assert!($status.success())
    };
}

#[cfg(feature = "no_cleanup")]
macro_rules! ensure_fail {
    ( $status: expr ) => {
        assert!(!$status.success());
    };
}

/// Holds a set of arguments for a shell command
#[derive(Debug)]
pub struct Command {
    program: OsString,
    args: Vec<OsString>,
}

#[macro_export]
macro_rules! cmd {
    ( $program:expr $(, $arg:expr )* $(,)? ) => {
        {
          //  use std::ffi::OsString;
            let args: &[OsString] = &[$( Into::<OsString>::into($arg) ),*];
            Command::new($program, args)
        }
    };
}

impl Command {
    pub fn new(program: OsString, args: Vec<OsString>) -> Self {
        Command { program, args }
    }

    pub fn to_expr(&self) -> duct::Expression {
        duct::cmd(&self.program, &self.args)
    }
}

impl fmt::Display for Command {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.program.clone().into_string().unwrap())?;
        for a in &self.args {
            write!(f, " {}", a.clone().into_string().unwrap())?;
        }
        Ok(())
    }
}

fn run_raw(command: Command) -> std::io::Result<std::process::Output> {
    let command = command.to_expr().stdout_capture().stderr_capture();
    command.unchecked().run()
}

fn log_output(output: &process::Output) {
    use std::str::from_utf8;

    if !output.stdout.is_empty() {
        eprintln!("stdout: \n{}<<END>>", from_utf8(&output.stdout).unwrap());
    }
    if !output.stderr.is_empty() {
        eprintln!("stderr: \n{}<<END>>", from_utf8(&output.stderr).unwrap());
    }
}

// Returns stdout. The command must return zero.
pub fn run_ok(command: Command) -> Result<String> {
    eprintln!("run_ok: {}", command);

    let output = run_raw(command)?;
    log_output(&output);
    ensure_success!(output.status);

    let stdout = std::str::from_utf8(&output.stdout[..])
        .unwrap()
        .trim_end_matches(|c| c == '\n' || c == '\r')
        .to_string();

    Ok(stdout)
}

// Returns the entire output. The command must return zero.
pub fn run_ok_raw(command: Command) -> Result<std::process::Output> {
    eprintln!("run_ok_raw: {}", command);

    let output = run_raw(command)?;
    log_output(&output);
    ensure_success!(output.status);

    Ok(output)
}

// Returns stderr, a non zero status must be returned
pub fn run_fail(command: Command) -> Result<String> {
    eprintln!("run_fail: {}", command);

    let output = run_raw(command)?;
    log_output(&output);
    ensure_fail!(output.status);

    let stderr = std::str::from_utf8(&output.stderr[..])
        .unwrap()
        .trim_end_matches(|c| c == '\n' || c == '\r')
        .to_string();

    Ok(stderr)
}

// Returns the entire output, a non zero status must be returned
pub fn run_fail_raw(command: Command) -> Result<std::process::Output> {
    eprintln!("run_fail_raw: {}", command);

    let output = run_raw(command)?;
    log_output(&output);
    ensure_fail!(output.status);

    Ok(output)
}

//------------------------------------------
