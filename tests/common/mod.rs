#![allow(dead_code)]

use anyhow::Result;
use duct::{cmd, Expression};
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::str::from_utf8;
use thinp::file_utils;
use thinp::io_engine::*;

pub mod cache_xml_generator;
pub mod test_dir;
pub mod thin_xml_generator;

use crate::common::thin_xml_generator::{write_xml, SingleThinS};
use test_dir::TestDir;

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

// FIXME: write a macro to generate these commands
// Known issue of nested macro definition: https://github.com/rust-lang/rust/issues/35853
// RFC: https://github.com/rust-lang/rfcs/blob/master/text/3086-macro-metavar-expr.md
#[macro_export]
macro_rules! thin_check {
    ( $( $arg: expr ),* ) => {
        {
            use std::ffi::OsString;
            let args: &[OsString] = &[$( Into::<OsString>::into($arg) ),*];
            duct::cmd(path_to!("thin_check"), args).stdout_capture().stderr_capture()
        }
    };
}

#[macro_export]
macro_rules! thin_restore {
    ( $( $arg: expr ),* ) => {
        {
            use std::ffi::OsString;
            let args: &[OsString] = &[$( Into::<OsString>::into($arg) ),*];
            duct::cmd(path_to!("thin_restore"), args).stdout_capture().stderr_capture()
        }
    };
}

#[macro_export]
macro_rules! thin_dump {
    ( $( $arg: expr ),* ) => {
        {
            use std::ffi::OsString;
            let args: &[OsString] = &[$( Into::<OsString>::into($arg) ),*];
            duct::cmd(path_to!("thin_dump"), args).stdout_capture().stderr_capture()
        }
    };
}

#[macro_export]
macro_rules! thin_rmap {
    ( $( $arg: expr ),* ) => {
        {
            use std::ffi::OsString;
            let args: &[OsString] = &[$( Into::<OsString>::into($arg) ),*];
            duct::cmd(path_to_cpp!("thin_rmap"), args).stdout_capture().stderr_capture()
        }
    };
}

#[macro_export]
macro_rules! thin_repair {
    ( $( $arg: expr ),* ) => {
        {
            use std::ffi::OsString;
            let args: &[OsString] = &[$( Into::<OsString>::into($arg) ),*];
            duct::cmd(path_to_cpp!("thin_repair"), args).stdout_capture().stderr_capture()
        }
    };
}

#[macro_export]
macro_rules! thin_delta {
    ( $( $arg: expr ),* ) => {
        {
            use std::ffi::OsString;
            let args: &[OsString] = &[$( Into::<OsString>::into($arg) ),*];
            duct::cmd(path_to_cpp!("thin_delta"), args).stdout_capture().stderr_capture()
        }
    };
}

#[macro_export]
macro_rules! thin_metadata_pack {
    ( $( $arg: expr ),* ) => {
        {
            use std::ffi::OsString;
            let args: &[OsString] = &[$( Into::<OsString>::into($arg) ),*];
            duct::cmd(path_to_rust!("thin_metadata_pack"), args).stdout_capture().stderr_capture()
        }
    };
}

#[macro_export]
macro_rules! thin_metadata_unpack {
    ( $( $arg: expr ),* ) => {
        {
            use std::ffi::OsString;
            let args: &[OsString] = &[$( Into::<OsString>::into($arg) ),*];
            duct::cmd(path_to_rust!("thin_metadata_unpack"), args).stdout_capture().stderr_capture()
        }
    };
}

#[macro_export]
macro_rules! cache_check {
    ( $( $arg: expr ),* ) => {
        {
            use std::ffi::OsString;
            let args: &[OsString] = &[$( Into::<OsString>::into($arg) ),*];
            duct::cmd(path_to!("cache_check"), args).stdout_capture().stderr_capture()
        }
    };
}

#[macro_export]
macro_rules! thin_generate_metadata {
    ( $( $arg: expr ),* ) => {
        {
            use std::ffi::OsString;
            let args: &[OsString] = &[$( Into::<OsString>::into($arg) ),*];
            duct::cmd(path_to_cpp!("thin_generate_metadata"), args).stdout_capture().stderr_capture()
        }
    };
}

#[macro_export]
macro_rules! thin_generate_mappings {
    ( $( $arg: expr ),* ) => {
        {
            use std::ffi::OsString;
            let args: &[OsString] = &[$( Into::<OsString>::into($arg) ),*];
            duct::cmd(path_to_cpp!("thin_generate_mappings"), args).stdout_capture().stderr_capture()
        }
    };
}

#[macro_export]
macro_rules! thin_generate_damage {
    ( $( $arg: expr ),* ) => {
        {
            use std::ffi::OsString;
            let args: &[OsString] = &[$( Into::<OsString>::into($arg) ),*];
            duct::cmd("bin/thin_generate_damage", args).stdout_capture().stderr_capture()
        }
    };
}

//------------------------------------------

// Returns stderr, a non zero status must be returned
pub fn run_fail(command: Expression) -> Result<String> {
    let output = command.stderr_capture().unchecked().run()?;
    assert!(!output.status.success());
    Ok(from_utf8(&output.stderr[0..]).unwrap().to_string())
}

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
    thin_restore!("-i", xml, "-o", &md).run()?;
    Ok(md)
}

pub fn mk_zeroed_md(td: &mut TestDir) -> Result<PathBuf> {
    let md = td.mk_path("meta.bin");
    eprintln!("path = {:?}", md);
    let _file = file_utils::create_sized_file(&md, 1024 * 1024 * 16);
    Ok(md)
}

pub fn accepts_flag(flag: &str) -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;
    thin_check!(flag, &md).run()?;
    Ok(())
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
    thin_generate_metadata!("-o", &md, "--format", "--nr-data-blocks", "102400").run()?;

    // Create a 2GB device
    thin_generate_metadata!("-o", &md, "--create-thin", "1").run()?;
    thin_generate_mappings!(
        "-o",
        &md,
        "--dev-id",
        "1",
        "--size",
        format!("{}", 1024 * 1024 * 2),
        "--rw=randwrite",
        "--seq-nr=16"
    )
    .run()?;

    // Take a few snapshots.
    let mut snap_id = 2;
    for _i in 0..10 {
        // take a snapshot
        thin_generate_metadata!(
            "-o",
            &md,
            "--create-snap",
            format!("{}", snap_id),
            "--origin",
            "1"
        )
        .run()?;

        // partially overwrite the origin (64MB)
        thin_generate_mappings!(
            "-o",
            &md,
            "--dev-id",
            format!("{}", 1),
            "--size",
            format!("{}", 1024 * 1024 * 2),
            "--io-size",
            format!("{}", 64 * 1024 * 2),
            "--rw=randwrite",
            "--seq-nr=16"
        )
        .run()?;
        snap_id += 1;
    }

    Ok(md)
}

pub fn set_needs_check(md: &PathBuf) -> Result<()> {
    thin_generate_metadata!("-o", &md, "--set-needs-check").run()?;
    Ok(())
}

pub fn generate_metadata_leaks(
    md: &PathBuf,
    nr_blocks: u64,
    expected: u32,
    actual: u32,
) -> Result<()> {
    let output = thin_generate_damage!(
        "-o",
        &md,
        "--create-metadata-leaks",
        "--nr-blocks",
        format!("{}", nr_blocks),
        "--expected",
        format!("{}", expected),
        "--actual",
        format!("{}", actual)
    )
    .unchecked()
    .run()?;

    assert!(output.status.success());
    Ok(())
}

pub fn get_needs_check(md: &PathBuf) -> Result<bool> {
    use thinp::thin::superblock::*;

    let engine = SyncIoEngine::new(&md, 1, false)?;
    let sb = read_superblock(&engine, SUPERBLOCK_LOCATION)?;
    Ok(sb.flags.needs_check)
}

pub fn md5(md: &PathBuf) -> Result<String> {
    let output = cmd!("md5sum", "-b", &md).stdout_capture().run()?;
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
