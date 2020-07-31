use anyhow::Result;
use duct::{cmd, Expression};
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::str::from_utf8;
use tempfile::{tempdir, TempDir};
use thinp::file_utils;
use std::io::{Read};

pub mod xml_generator;
use crate::common::xml_generator::{write_xml, FragmentedS, SingleThinS};

//------------------------------------------

pub fn mk_path(dir: &Path, file: &str) -> PathBuf {
    let mut p = PathBuf::new();
    p.push(dir);
    p.push(PathBuf::from(file));
    p
}

// FIXME: write a macro to generate these commands
#[macro_export]
macro_rules! thin_check {
    ( $( $arg: expr ),* ) => {
        {
            use std::ffi::OsString;
            let args: &[OsString] = &[$( Into::<OsString>::into($arg) ),*];
            duct::cmd("bin/thin_check", args).stdout_capture().stderr_capture()
        }
    };
}

#[macro_export]
macro_rules! thin_restore {
    ( $( $arg: expr ),* ) => {
        {
            use std::ffi::OsString;
            let args: &[OsString] = &[$( Into::<OsString>::into($arg) ),*];
            duct::cmd("bin/thin_restore", args).stdout_capture().stderr_capture()
        }
    };
}

#[macro_export]
macro_rules! thin_dump {
    ( $( $arg: expr ),* ) => {
        {
            use std::ffi::OsString;
            let args: &[OsString] = &[$( Into::<OsString>::into($arg) ),*];
            duct::cmd("bin/thin_dump", args).stdout_capture().stderr_capture()
        }
    };
}


// Returns stderr, a non zero status must be returned
pub fn run_fail(command: Expression) -> Result<String> {
    let output = command.stderr_capture().unchecked().run()?;
    assert!(!output.status.success());
    Ok(from_utf8(&output.stderr[0..]).unwrap().to_string())
}

pub fn mk_valid_xml(dir: &TempDir) -> Result<PathBuf> {
    let xml = mk_path(dir.path(), "meta.xml");
    let mut gen = SingleThinS::new(0, 1024, 2048, 2048);
    write_xml(&xml, &mut gen)?;
    Ok(xml)
}

pub fn mk_valid_md(dir: &TempDir) -> Result<PathBuf> {
    let xml = mk_path(dir.path(), "meta.xml");
    let md = mk_path(dir.path(), "meta.bin");

    let mut gen = SingleThinS::new(0, 1024, 2048, 2048);
    write_xml(&xml, &mut gen)?;

    let _file = file_utils::create_sized_file(&md, 4096 * 4096);
    cmd!("bin/thin_restore", "-i", xml, "-o", &md).run()?;
    Ok(md)
}

pub fn mk_zeroed_md(dir: &TempDir) -> Result<PathBuf> {
    let md = mk_path(dir.path(), "meta.bin");
    let _file = file_utils::create_sized_file(&md, 4096 * 4096);
    Ok(md)
}

pub fn accepts_flag(flag: &str) -> Result<()> {
    let dir = tempdir()?;
    let md = mk_valid_md(&dir)?;
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

//------------------------------------------
