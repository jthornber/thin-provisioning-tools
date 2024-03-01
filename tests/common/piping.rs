use anyhow::Result;
use std::fs::OpenOptions;
use std::path::PathBuf;

use crate::args;
use crate::common::program::*;
use crate::common::test_dir::TestDir;

//------------------------------------------

pub fn test_no_stderr_on_broken_pipe<'a, P>(
    prep_input: fn(&mut TestDir) -> Result<PathBuf>,
    extra_args: &[&std::ffi::OsStr],
) -> Result<()>
where
    P: InputProgram<'a>,
{
    use anyhow::ensure;

    let mut td = TestDir::new()?;

    // prepare the input to produce more than 64KB output (the default pipe buffer size),
    // such that the program will be blocked on pipe write, then hits EPIPE.
    let md = prep_input(&mut td)?;

    let mut pipefd = [0i32; 2];
    unsafe {
        ensure!(libc::pipe2(pipefd.as_mut_slice().as_mut_ptr(), libc::O_CLOEXEC) == 0);
        ensure!(libc::fcntl(pipefd[0], libc::F_SETPIPE_SZ, 65536) == 65536);
    }

    let mut args = args![&md].to_vec();
    args.extend_from_slice(extra_args);
    let cmd = P::cmd(args)
        .to_expr()
        .stdout_file(pipefd[1]) // this transfers ownership of the fd
        .stderr_capture();
    let handle = cmd.unchecked().start()?;

    unsafe {
        // read outputs from the program
        let mut buf = vec![0; 128];
        libc::read(pipefd[0], buf.as_mut_slice().as_mut_ptr().cast(), 128);
        libc::close(pipefd[0]); // causing broken pipe
    }

    let output = handle.wait()?;
    ensure!(!output.status.success());
    ensure!(output.stderr.is_empty());

    Ok(())
}

pub fn test_no_stderr_on_broken_fifo<'a, P>(
    prep_input: fn(&mut TestDir) -> Result<PathBuf>,
    extra_args: &[&std::ffi::OsStr],
) -> Result<()>
where
    P: InputProgram<'a>,
{
    use anyhow::ensure;
    use std::io::Read;
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::OpenOptionsExt;

    let mut td = TestDir::new()?;

    // prepare the input to produce more than 64KB output (the default pipe buffer size),
    // such that the program will be blocked on pipe write, then hits EPIPE.
    let md = prep_input(&mut td)?;

    let out_fifo = td.mk_path("out_fifo");
    unsafe {
        let c_str = std::ffi::CString::new(out_fifo.as_os_str().as_encoded_bytes()).unwrap();
        ensure!(libc::mkfifo(c_str.as_ptr(), 0o666) == 0);
    };

    let mut fifo = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(&out_fifo)?;
    unsafe {
        ensure!(libc::fcntl(fifo.as_raw_fd(), libc::F_SETPIPE_SZ, 65536) == 65536);
        let new_flags = libc::O_RDONLY | libc::O_CLOEXEC;
        ensure!(libc::fcntl(fifo.as_raw_fd(), libc::F_SETFL, new_flags) == 0);
    }

    let mut args = args![&md, "-o", &out_fifo].to_vec();
    args.extend_from_slice(extra_args);
    let cmd = P::cmd(args).to_expr().stderr_capture();
    let handle = cmd.unchecked().start()?;

    // wait for the fifo is ready
    unsafe {
        let mut pollfd = libc::pollfd {
            fd: fifo.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        ensure!(libc::poll(&mut pollfd, 1, 10000) == 1);
    }

    // read outputs from the program
    let mut buf = vec![0; 128];
    fifo.read_exact(&mut buf)?;
    drop(fifo); // causing broken pipe

    let output = handle.wait()?;
    ensure!(!output.status.success());
    ensure!(output.stderr.is_empty());

    Ok(())
}

//------------------------------------------
