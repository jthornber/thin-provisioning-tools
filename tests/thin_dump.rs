use anyhow::Result;
use std::fs::OpenOptions;
use std::io::Write;

mod common;

use common::common_args::*;
use common::fixture::*;
use common::input_arg::*;
use common::process::*;
use common::program::*;
use common::target::*;
use common::test_dir::*;
use common::thin::*;

//------------------------------------------

const USAGE: &str = "Dump thin-provisioning metadata to stdout in XML format

Usage: thin_dump [OPTIONS] <INPUT>

Arguments:
  <INPUT>  Specify the input device to dump

Options:
      --data-block-size <SECTORS>  Provide the data block size for repairing
      --dev-id <THIN_ID>           Dump the specified device
  -f, --format <TYPE>              Choose the output format
  -h, --help                       Print help
  -m, --metadata-snap[=<BLOCKNR>]  Access the metadata snapshot on a live pool
      --nr-data-blocks <NUM>       Override the number of data blocks if needed
  -o, --output <FILE>              Specify the output file rather than stdout
  -q, --quiet                      Suppress output messages, return only exit code.
  -r, --repair                     Repair the metadata whilst dumping it
      --skip-mappings              Do not dump the mappings
      --transaction-id <NUM>       Override the transaction id if needed
  -V, --version                    Print version";

//-----------------------------------------

struct ThinDump;

impl<'a> Program<'a> for ThinDump {
    fn name() -> &'a str {
        "thin_dump"
    }

    fn cmd<I>(args: I) -> Command
    where
        I: IntoIterator,
        I::Item: Into<std::ffi::OsString>,
    {
        thin_dump_cmd(args)
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

impl<'a> InputProgram<'a> for ThinDump {
    fn mk_valid_input(td: &mut TestDir) -> Result<std::path::PathBuf> {
        mk_valid_md(td)
    }

    fn file_not_found() -> &'a str {
        msg::FILE_NOT_FOUND
    }

    fn missing_input_arg() -> &'a str {
        msg::MISSING_INPUT_ARG
    }

    fn corrupted_input() -> &'a str {
        msg::BAD_SUPERBLOCK
    }
}

//------------------------------------------

test_accepts_help!(ThinDump);
test_accepts_version!(ThinDump);
test_rejects_bad_option!(ThinDump);

test_missing_input_arg!(ThinDump);
test_input_file_not_found!(ThinDump);
test_input_cannot_be_a_directory!(ThinDump);
test_unreadable_input_file!(ThinDump);

test_readonly_input_file!(ThinDump);

//------------------------------------------
// test dump & restore cycle

#[test]
fn dump_restore_cycle() -> Result<()> {
    let mut td = TestDir::new()?;

    let md = prep_rebuilt_metadata(&mut td)?;
    let output = run_ok_raw(thin_dump_cmd(args![&md]))?;

    let xml = td.mk_path("meta.xml");
    let mut file = OpenOptions::new()
        .read(false)
        .write(true)
        .create(true)
        .open(&xml)?;
    file.write_all(&output.stdout[0..])?;
    drop(file);

    let md2 = mk_zeroed_md(&mut td)?;
    run_ok(thin_restore_cmd(args!["-i", &xml, "-o", &md2]))?;

    let output2 = run_ok_raw(thin_dump_cmd(args![&md2]))?;
    assert_eq!(output.stdout, output2.stdout);

    Ok(())
}

//------------------------------------------
// test no stderr with a normal dump

#[test]
fn no_stderr_on_success() -> Result<()> {
    let mut td = TestDir::new()?;

    let md = mk_valid_md(&mut td)?;
    let output = run_ok_raw(thin_dump_cmd(args![&md]))?;

    assert_eq!(output.stderr.len(), 0);
    Ok(())
}

//------------------------------------------
// test no stderr on broken pipe errors

fn test_no_stderr_on_broken_pipe(extra_args: &[&std::ffi::OsStr]) -> Result<()> {
    use anyhow::ensure;

    let mut td = TestDir::new()?;

    // use the metadata producing dump more than 64KB (the default pipe buffer size),
    // such that thin_dump will be blocked on writing to the pipe, then hits EPIPE.
    let md = prep_metadata(&mut td)?;

    let mut pipefd = [0i32; 2];
    unsafe {
        ensure!(libc::pipe2(pipefd.as_mut_slice().as_mut_ptr(), libc::O_CLOEXEC) == 0);
        ensure!(libc::fcntl(pipefd[0], libc::F_SETPIPE_SZ, 65536) == 65536);
    }

    let mut args = args![&md].to_vec();
    args.extend_from_slice(extra_args);
    let cmd = thin_dump_cmd(args)
        .to_expr()
        .stdout_file(pipefd[1]) // this transfers ownership of the fd
        .stderr_capture();
    let handle = cmd.unchecked().start()?;

    unsafe {
        // read outputs from thin_dump
        let mut buf = vec![0; 128];
        libc::read(pipefd[0], buf.as_mut_slice().as_mut_ptr().cast(), 128);
        libc::close(pipefd[0]); // causing broken pipe
    }

    let output = handle.wait()?;
    ensure!(!output.status.success());
    ensure!(output.stderr.is_empty());

    Ok(())
}

#[test]
fn no_stderr_on_broken_pipe_xml() -> Result<()> {
    test_no_stderr_on_broken_pipe(&[])
}

#[test]
fn no_stderr_on_broken_pipe_humanreadable() -> Result<()> {
    test_no_stderr_on_broken_pipe(&args!["--format", "human_readable"])
}

fn test_no_stderr_on_broken_fifo(extra_args: &[&std::ffi::OsStr]) -> Result<()> {
    use anyhow::ensure;
    use std::io::Read;
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::OpenOptionsExt;

    let mut td = TestDir::new()?;

    // use the metadata producing dump more than 64KB (the default pipe buffer size),
    // such that thin_dump will be blocked on writing to the pipe, then hits EPIPE.
    let md = prep_metadata(&mut td)?;

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
    let cmd = thin_dump_cmd(args).to_expr().stderr_capture();
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

    // read outputs from thin_dump
    let mut buf = vec![0; 128];
    fifo.read_exact(&mut buf)?;
    drop(fifo); // causing broken pipe

    let output = handle.wait()?;
    ensure!(!output.status.success());
    ensure!(output.stderr.is_empty());

    Ok(())
}

#[test]
fn no_stderr_on_broken_fifo_xml() -> Result<()> {
    test_no_stderr_on_broken_fifo(&[])
}

#[test]
fn no_stderr_on_broken_fifo_humanreadable() -> Result<()> {
    test_no_stderr_on_broken_fifo(&args!["--format", "human_readable"])
}

//------------------------------------------
// test dump metadata snapshot from a live metadata
// here we use a corrupted metadata to ensure that "thin_dump -m" reads the
// metadata snapshot only.

#[test]
fn dump_metadata_snapshot() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = prep_metadata_from_file(&mut td, "corrupted_tmeta_with_metadata_snap.pack")?;
    let output = run_ok_raw(thin_dump_cmd(args![&md, "-m"]))?;

    assert_eq!(output.stderr.len(), 0);
    Ok(())
}

//------------------------------------------
// test superblock overriding & repair
// TODO: share with thin_repair

fn override_something(flag: &str, value: &str, pattern: &str) -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;
    let output = run_ok_raw(thin_dump_cmd(args![&md, flag, value]))?;

    assert_eq!(output.stderr.len(), 0);
    assert!(std::str::from_utf8(&output.stdout[0..])?.contains(pattern));
    Ok(())
}

#[test]
fn override_transaction_id() -> Result<()> {
    override_something("--transaction-id", "2345", "transaction=\"2345\"")
}

#[test]
fn override_data_block_size() -> Result<()> {
    override_something("--data-block-size", "8192", "data_block_size=\"8192\"")
}

#[test]
fn override_nr_data_blocks() -> Result<()> {
    override_something("--nr-data-blocks", "234500", "nr_data_blocks=\"234500\"")
}

// FIXME: duplicate with superblock_succeeds in thin_repair.rs
#[test]
fn repair_superblock() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;
    let before = run_ok_raw(thin_dump_cmd(args![&md]))?;
    damage_superblock(&md)?;

    let after = run_ok_raw(thin_dump_cmd(args![
        "--repair",
        "--transaction-id=1",
        "--data-block-size=128",
        "--nr-data-blocks=20480",
        &md
    ]))?;
    assert_eq!(before.stdout, after.stdout);

    Ok(())
}

#[test]
fn repair_healthy_metadata() -> Result<()> {
    let mut td = TestDir::new()?;

    // use the metadata containing multiple transactions
    let md = prep_metadata(&mut td)?;

    let before = run_ok_raw(thin_dump_cmd(args![&md]))?;
    let after = run_ok_raw(thin_dump_cmd(args!["--repair", &md]))?;
    assert_eq!(before.stdout, after.stdout);

    Ok(())
}

#[test]
fn repair_metadata_with_empty_roots() -> Result<()> {
    let mut td = TestDir::new()?;

    // use the metadata containing empty roots
    let md = prep_metadata_from_file(&mut td, "tmeta_with_empty_roots.pack")?;
    let before = run_ok_raw(thin_dump_cmd(args![&md]))?;

    // repairing dump
    damage_superblock(&md)?;
    let after = run_ok_raw(thin_dump_cmd(args![
        "--repair",
        "--transaction-id=2",
        "--data-block-size=128",
        "--nr-data-blocks=1024",
        &md
    ]))?;

    assert_eq!(before.stdout, after.stdout);

    Ok(())
}

//------------------------------------------
// test compatibility between options
// TODO: share with thin_repair

#[test]
fn recovers_transaction_id_from_damaged_superblock() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;
    damage_superblock(&md)?;
    let stdout = run_ok(thin_dump_cmd(args![
        "--repair",
        "--data-block-size=128",
        "--nr-data-blocks=20480",
        &md
    ]))?;
    assert!(stdout.contains("transaction=\"1\""));
    assert!(stdout.contains("data_block_size=\"128\""));
    assert!(stdout.contains("nr_data_blocks=\"20480\""));
    Ok(())
}

#[test]
fn missing_data_block_size() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;
    damage_superblock(&md)?;
    let stderr = run_fail(thin_dump_cmd(args![
        "--repair",
        "--transaction-id=1",
        "--nr-data-blocks=20480",
        &md
    ]))?;
    assert!(stderr.contains("data block size"));
    Ok(())
}

#[test]
fn recovers_nr_data_blocks_from_damaged_superblock() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;
    damage_superblock(&md)?;
    let stdout = run_ok(thin_dump_cmd(args![
        "--repair",
        "--transaction-id=10",
        "--data-block-size=128",
        &md
    ]))?;
    assert!(stdout.contains("transaction=\"10\""));
    assert!(stdout.contains("data_block_size=\"128\""));
    assert!(stdout.contains("nr_data_blocks=\"1024\""));
    Ok(())
}

#[test]
fn recovers_tid_and_nr_data_blocks_from_damaged_superblock() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;
    damage_superblock(&md)?;
    let stdout = run_ok(thin_dump_cmd(args![
        "--repair",
        "--data-block-size=128",
        &md
    ]))?;
    assert!(stdout.contains("transaction=\"1\""));
    assert!(stdout.contains("data_block_size=\"128\""));
    assert!(stdout.contains("nr_data_blocks=\"1024\""));
    Ok(())
}

#[test]
fn repair_metadata_with_stale_superblock() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;
    let before = run_ok_raw(thin_dump_cmd(args![&md]))?;

    // produce stale superblock by overriding the data mapping root,
    // then update the superblock checksum.
    run_ok(thin_generate_damage_cmd(args![
        "-o",
        &md,
        "--override",
        "--mapping-root",
        "10"
    ]))?;

    let after = run_ok_raw(thin_dump_cmd(args!["--repair", &md]))?;
    assert_eq!(before.stdout, after.stdout);

    Ok(())
}

//------------------------------------------
