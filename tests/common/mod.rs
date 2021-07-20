#![allow(dead_code)]

use anyhow::Result;
use std::ffi::{OsStr, OsString};
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::PathBuf;

use thinp::file_utils;
use thinp::io_engine::*;

pub mod cache_xml_generator;
pub mod common_args;
pub mod input_arg;
pub mod output_option;
pub mod test_dir;
pub mod thin_xml_generator;

use crate::common::thin_xml_generator::{write_xml, SingleThinS};
use test_dir::TestDir;

//------------------------------------------

pub mod cpp_msg {
    pub const FILE_NOT_FOUND: &str = "No such file or directory";
    pub const MISSING_INPUT_ARG: &str = "No input file provided";
    pub const MISSING_OUTPUT_ARG: &str = "No output file provided";
    pub const BAD_SUPERBLOCK: &str = "bad checksum in superblock";

    pub fn bad_option_hint(option: &str) -> String {
        format!("unrecognized option '{}'", option)
    }
}

pub mod rust_msg {
    pub const FILE_NOT_FOUND: &str = "Couldn't find input file";
    pub const MISSING_INPUT_ARG: &str = "The following required arguments were not provided"; // TODO: be specific
    pub const MISSING_OUTPUT_ARG: &str = "The following required arguments were not provided"; // TODO: be specific
    pub const BAD_SUPERBLOCK: &str = "bad checksum in superblock";

    pub fn bad_option_hint(option: &str) -> String {
        format!("Found argument '{}' which wasn't expected", option)
    }
}

#[cfg(not(feature = "rust_tests"))]
pub use cpp_msg as msg;
#[cfg(feature = "rust_tests")]
pub use rust_msg as msg;

//------------------------------------------

#[macro_export]
macro_rules! path_to_cpp {
    ($name: literal) => {
        concat!("bin/", $name)
    };
}

#[macro_export]
macro_rules! path_to_rust {
    ($name: literal) => {
        env!(concat!("CARGO_BIN_EXE_", $name))
    };
}

#[cfg(not(feature = "rust_tests"))]
#[macro_export]
macro_rules! path_to {
    ($name: literal) => {
        path_to_cpp!($name)
    };
}

#[cfg(feature = "rust_tests")]
#[macro_export]
macro_rules! path_to {
    ($name: literal) => {
        path_to_rust!($name)
    };
}

//------------------------------------------

pub const CACHE_CHECK: &str = path_to!("cache_check");
pub const CACHE_DUMP: &str = path_to!("cache_dump");

pub const THIN_CHECK: &str = path_to!("thin_check");
pub const THIN_DELTA: &str = path_to_cpp!("thin_delta"); // TODO: rust version
pub const THIN_DUMP: &str = path_to!("thin_dump");
pub const THIN_METADATA_PACK: &str = path_to_rust!("thin_metadata_pack"); // rust-only
pub const THIN_METADATA_UNPACK: &str = path_to_rust!("thin_metadata_unpack"); // rust-only
pub const THIN_REPAIR: &str = path_to_cpp!("thin_repair"); // TODO: rust version
pub const THIN_RESTORE: &str = path_to!("thin_restore");
pub const THIN_RMAP: &str = path_to_cpp!("thin_rmap"); // TODO: rust version
pub const THIN_GENERATE_METADATA: &str = path_to_cpp!("thin_generate_metadata"); // cpp-only
pub const THIN_GENERATE_MAPPINGS: &str = path_to_cpp!("thin_generate_mappings"); // cpp-only
pub const THIN_GENERATE_DAMAGE: &str = path_to_cpp!("thin_generate_damage"); // cpp-only

//------------------------------------------

pub enum ArgType {
    InputArg,
    IoOptions,
}

pub trait Program<'a> {
    fn name() -> &'a str;
    fn path() -> &'a OsStr;
    fn usage() -> &'a str;
    fn arg_type() -> ArgType;

    // error messages
    fn bad_option_hint(option: &str) -> String;
}

pub trait InputProgram<'a>: Program<'a> {
    fn mk_valid_input(td: &mut TestDir) -> Result<PathBuf>;

    // error messages
    fn missing_input_arg() -> &'a str;
    fn file_not_found() -> &'a str;
    fn corrupted_input() -> &'a str;
}

pub trait BinaryInputProgram<'a>: InputProgram<'a> {}

pub trait OutputProgram<'a>: InputProgram<'a> {
    // error messages
    fn missing_output_arg() -> &'a str;
    fn file_not_found() -> &'a str;
}

pub trait BinaryOutputProgram<'a>: OutputProgram<'a> {}

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

pub fn mk_valid_xml(td: &mut TestDir) -> Result<PathBuf> {
    let xml = td.mk_path("meta.xml");
    let mut gen = SingleThinS::new(0, 1024, 2048, 2048);
    write_xml(&xml, &mut gen)?;
    Ok(xml)
}

pub fn mk_valid_md(td: &mut TestDir) -> Result<PathBuf> {
    let xml = td.mk_path("meta.xml");
    let md = td.mk_path("meta.bin");

    let mut gen = SingleThinS::new(0, 1024, 20480, 20480);
    write_xml(&xml, &mut gen)?;

    let _file = file_utils::create_sized_file(&md, 4096 * 4096);
    let args = ["-i", xml.to_str().unwrap(), "-o", md.to_str().unwrap()];
    run_ok(THIN_RESTORE, &args)?;

    Ok(md)
}

pub fn mk_zeroed_md(td: &mut TestDir) -> Result<PathBuf> {
    let md = td.mk_path("meta.bin");
    eprintln!("path = {:?}", md);
    let _file = file_utils::create_sized_file(&md, 1024 * 1024 * 16);
    Ok(md)
}

pub fn superblock_all_zeroes(path: &PathBuf) -> Result<bool> {
    let mut input = OpenOptions::new().read(true).write(false).open(path)?;
    let mut buf = vec![0; 4096];
    input.read_exact(&mut buf[0..])?;
    for b in buf {
        if b != 0 {
            return Ok(false);
        }
    }

    Ok(true)
}

pub fn damage_superblock(path: &PathBuf) -> Result<()> {
    let mut output = OpenOptions::new().read(false).write(true).open(path)?;
    let buf = [0u8; 512];
    output.write_all(&buf)?;
    Ok(())
}

//-----------------------------------------------

// FIXME: replace mk_valid_md with this?
pub fn prep_metadata(td: &mut TestDir) -> Result<PathBuf> {
    let md = mk_zeroed_md(td)?;
    let args = [
        "-o",
        md.to_str().unwrap(),
        "--format",
        "--nr-data-blocks",
        "102400",
    ];
    run_ok(THIN_GENERATE_METADATA, &args)?;

    // Create a 2GB device
    let args = ["-o", md.to_str().unwrap(), "--create-thin", "1"];
    run_ok(THIN_GENERATE_METADATA, &args)?;
    let args = [
        "-o",
        md.to_str().unwrap(),
        "--dev-id",
        "1",
        "--size",
        "2097152",
        "--rw=randwrite",
        "--seq-nr=16",
    ];
    run_ok(THIN_GENERATE_MAPPINGS, &args)?;

    // Take a few snapshots.
    let mut snap_id = 2;
    for _i in 0..10 {
        // take a snapshot
        let args = [
            "-o",
            md.to_str().unwrap(),
            "--create-snap",
            &snap_id.to_string(),
            "--origin",
            "1",
        ];
        run_ok(THIN_GENERATE_METADATA, &args)?;

        // partially overwrite the origin (64MB)
        let args = [
            "-o",
            md.to_str().unwrap(),
            "--dev-id",
            "1",
            "--size",
            "2097152",
            "--io-size",
            "131072",
            "--rw=randwrite",
            "--seq-nr=16",
        ];
        run_ok(THIN_GENERATE_MAPPINGS, &args)?;
        snap_id += 1;
    }

    Ok(md)
}

pub fn set_needs_check(md: &PathBuf) -> Result<()> {
    let args = ["-o", md.to_str().unwrap(), "--set-needs-check"];
    run_ok(THIN_GENERATE_METADATA, &args)?;
    Ok(())
}

pub fn generate_metadata_leaks(
    md: &PathBuf,
    nr_blocks: u64,
    expected: u32,
    actual: u32,
) -> Result<()> {
    let args = [
        "-o",
        md.to_str().unwrap(),
        "--create-metadata-leaks",
        "--nr-blocks",
        &nr_blocks.to_string(),
        "--expected",
        &expected.to_string(),
        "--actual",
        &actual.to_string(),
    ];
    run_ok(THIN_GENERATE_DAMAGE, &args)?;

    Ok(())
}

pub fn get_needs_check(md: &PathBuf) -> Result<bool> {
    use thinp::thin::superblock::*;

    let engine = SyncIoEngine::new(&md, 1, false)?;
    let sb = read_superblock(&engine, SUPERBLOCK_LOCATION)?;
    Ok(sb.flags.needs_check)
}

pub fn md5(md: &PathBuf) -> Result<String> {
    let output = duct::cmd!("md5sum", "-b", &md).stdout_capture().run()?;
    let csum = std::str::from_utf8(&output.stdout[0..])?.to_string();
    let csum = csum.split_ascii_whitespace().next().unwrap().to_string();
    Ok(csum)
}

// This checksums the file before and after the thunk is run to
// ensure it is unchanged.
pub fn ensure_untouched<F>(p: &PathBuf, thunk: F) -> Result<()>
where
    F: Fn() -> Result<()>,
{
    let csum = md5(p)?;
    thunk()?;
    assert_eq!(csum, md5(p)?);
    Ok(())
}

pub fn ensure_superblock_zeroed<F>(p: &PathBuf, thunk: F) -> Result<()>
where
    F: Fn() -> Result<()>,
{
    thunk()?;
    assert!(superblock_all_zeroes(p)?);
    Ok(())
}

//------------------------------------------
