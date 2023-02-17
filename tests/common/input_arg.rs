use anyhow::Result;
use std::ffi::OsStr;

use thinp::file_utils;

use crate::args;
use crate::common::fixture::*;
use crate::common::process::*;
use crate::common::program::*;
use crate::common::test_dir::*;
use crate::common::thin_xml_generator::{write_xml, FragmentedS};

//------------------------------------------
// wrappers

type ArgsBuilder = fn(&mut TestDir, &OsStr, &dyn Fn(&[&OsStr]) -> Result<()>) -> Result<()>;

fn with_output_md_untouched(
    td: &mut TestDir,
    input: &OsStr,
    thunk: &dyn Fn(&[&OsStr]) -> Result<()>,
) -> Result<()> {
    let output = mk_zeroed_md(td)?;
    ensure_untouched(&output, || {
        let args = args!["-i", input, "-o", &output];
        thunk(&args)
    })
}

fn with_output_superblock_zeroed(
    td: &mut TestDir,
    input: &OsStr,
    thunk: &dyn Fn(&[&OsStr]) -> Result<()>,
) -> Result<()> {
    let output = mk_zeroed_md(td)?;
    ensure_superblock_zeroed(&output, || {
        let args = args!["-i", input, "-o", &output];
        thunk(&args)
    })
}

fn with_output_simple(
    td: &mut TestDir,
    input: &OsStr,
    thunk: &dyn Fn(&[&OsStr]) -> Result<()>,
) -> Result<()> {
    let output = mk_zeroed_md(td)?;
    let args = args!["-i", input, "-o", &output];
    thunk(&args)
}

fn input_arg_only(
    _td: &mut TestDir,
    input: &OsStr,
    thunk: &dyn Fn(&[&OsStr]) -> Result<()>,
) -> Result<()> {
    let args = args![input];
    thunk(&args)
}

fn build_args_fn(t: ArgType) -> Result<ArgsBuilder> {
    match t {
        ArgType::InputArg => Ok(input_arg_only),
        ArgType::IoOptions => Ok(with_output_md_untouched),
    }
}

//------------------------------------------
// test invalid arguments

pub fn test_missing_input_arg<'a, P>() -> Result<()>
where
    P: InputProgram<'a>,
{
    let args: [&str; 0] = [];
    let stderr = run_fail(P::cmd(args))?;
    assert!(stderr.contains(P::missing_input_arg()));
    Ok(())
}

#[macro_export]
macro_rules! test_missing_input_arg {
    ($program: ident) => {
        #[test]
        fn missing_input_arg() -> Result<()> {
            test_missing_input_arg::<$program>()
        }
    };
}

pub fn test_missing_input_option<'a, P>() -> Result<()>
where
    P: InputProgram<'a>,
{
    let mut td = TestDir::new()?;
    let output = mk_zeroed_md(&mut td)?;
    ensure_untouched(&output, || {
        let stderr = run_fail(P::cmd(args!["-o", &output]))?;
        assert!(stderr.contains(P::missing_input_arg()));
        Ok(())
    })
}

#[macro_export]
macro_rules! test_missing_input_option {
    ($program: ident) => {
        #[test]
        fn missing_input_option() -> Result<()> {
            test_missing_input_option::<$program>()
        }
    };
}

pub fn test_input_file_not_found<'a, P>() -> Result<()>
where
    P: InputProgram<'a>,
{
    let mut td = TestDir::new()?;

    let wrapper = build_args_fn(P::arg_type())?;
    wrapper(&mut td, "no-such-file".as_ref(), &|args: &[&OsStr]| {
        let stderr = run_fail(P::cmd(args))?;
        assert!(stderr.contains(P::file_not_found()));
        Ok(())
    })
}

#[macro_export]
macro_rules! test_input_file_not_found {
    ($program: ident) => {
        #[test]
        fn input_file_not_found() -> Result<()> {
            test_input_file_not_found::<$program>()
        }
    };
}

pub fn test_input_cannot_be_a_directory<'a, P>() -> Result<()>
where
    P: InputProgram<'a>,
{
    let mut td = TestDir::new()?;

    let wrapper = build_args_fn(P::arg_type())?;
    wrapper(&mut td, "/tmp".as_ref(), &|args: &[&OsStr]| {
        let stderr = run_fail(P::cmd(args))?;
        assert!(stderr.contains("Not a block device or regular file"));
        Ok(())
    })
}

#[macro_export]
macro_rules! test_input_cannot_be_a_directory {
    ($program: ident) => {
        #[test]
        fn input_cannot_be_a_directory() -> Result<()> {
            test_input_cannot_be_a_directory::<$program>()
        }
    };
}

pub fn test_unreadable_input_file<'a, P>() -> Result<()>
where
    P: InputProgram<'a>,
{
    unsafe {
        if libc::getuid() == 0 {
            return Ok(());
        }
    }

    let mut td = TestDir::new()?;

    // input an unreadable file
    let input = P::mk_valid_input(&mut td)?;
    duct::cmd!("chmod", "-r", &input).run()?;

    let wrapper = build_args_fn(P::arg_type())?;
    wrapper(&mut td, input.as_ref(), &|args: &[&OsStr]| {
        let stderr = run_fail(P::cmd(args))?;
        assert!(stderr.contains("Permission denied"));
        Ok(())
    })
}

#[macro_export]
macro_rules! test_unreadable_input_file {
    ($program: ident) => {
        #[test]
        fn unreadable_input_file() -> Result<()> {
            test_unreadable_input_file::<$program>()
        }
    };
}

//------------------------------------------
// test invalid content

pub fn test_tiny_input_file<'a, P>() -> Result<()>
where
    P: MetadataReader<'a>,
{
    let mut td = TestDir::new()?;

    let input = td.mk_path("meta.bin");
    file_utils::create_sized_file(&input, 1024)?;

    let wrapper = build_args_fn(P::arg_type())?;
    wrapper(&mut td, input.as_ref(), &|args: &[&OsStr]| {
        run_fail(P::cmd(args))?;
        Ok(())
    })
}

#[macro_export]
macro_rules! test_tiny_input_file {
    ($program: ident) => {
        #[test]
        fn tiny_input_file() -> Result<()> {
            test_tiny_input_file::<$program>()
        }
    };
}

pub fn test_help_message_for_tiny_input_file<'a, P>() -> Result<()>
where
    P: MetadataReader<'a>,
{
    let mut td = TestDir::new()?;

    let input = td.mk_path("meta.bin");
    file_utils::create_sized_file(&input, 1024)?;

    let wrapper = build_args_fn(P::arg_type())?;
    wrapper(&mut td, input.as_ref(), &|args: &[&OsStr]| {
        let stderr = run_fail(P::cmd(args))?;
        eprintln!("actual: {:?}", stderr);
        assert!(stderr.contains("Metadata device/file too small.  Is this binary metadata?"));
        Ok(())
    })
}

#[macro_export]
macro_rules! test_help_message_for_tiny_input_file {
    ($program: ident) => {
        #[test]
        fn prints_help_message_for_tiny_input_file() -> Result<()> {
            test_help_message_for_tiny_input_file::<$program>()
        }
    };
}

pub fn test_spot_xml_data<'a, P>() -> Result<()>
where
    P: MetadataReader<'a>,
{
    let mut td = TestDir::new()?;

    // input a large xml file
    let input = td.mk_path("meta.xml");
    let mut gen = FragmentedS::new(4, 10240);
    write_xml(&input, &mut gen)?;

    let wrapper = build_args_fn(P::arg_type())?;
    wrapper(&mut td, input.as_ref(), &|args: &[&OsStr]| {
        let stderr = run_fail(P::cmd(args))?;
        let msg =
            "This looks like XML.  This tool only supports the binary metadata format.".to_string();
        assert!(stderr.contains(&msg));
        Ok(())
    })
}

#[macro_export]
macro_rules! test_spot_xml_data {
    ($program: ident) => {
        #[test]
        fn spot_xml_data() -> Result<()> {
            test_spot_xml_data::<$program>()
        }
    };
}

pub fn test_corrupted_input_data<'a, P>() -> Result<()>
where
    P: InputProgram<'a>,
{
    let mut td = TestDir::new()?;
    let input = mk_zeroed_md(&mut td)?;

    let wrapper = match P::arg_type() {
        ArgType::InputArg => input_arg_only,
        ArgType::IoOptions => with_output_superblock_zeroed,
    };
    wrapper(&mut td, input.as_ref(), &|args: &[&OsStr]| {
        let stderr = run_fail(P::cmd(args))?;
        assert!(stderr.contains(P::corrupted_input()));
        Ok(())
    })
}

#[macro_export]
macro_rules! test_corrupted_input_data {
    ($program: ident) => {
        #[test]
        fn corrupted_input_data() -> Result<()> {
            test_corrupted_input_data::<$program>()
        }
    };
}

//------------------------------------------
// test special inputs

pub fn test_readonly_input_file<'a, P>() -> Result<()>
where
    P: InputProgram<'a>,
{
    let mut td = TestDir::new()?;

    // input an unreadable file
    let input = P::mk_valid_input(&mut td)?;
    duct::cmd!("chmod", "-w", &input).run()?;

    let wrapper = match P::arg_type() {
        ArgType::InputArg => input_arg_only,
        ArgType::IoOptions => with_output_simple,
    };

    wrapper(&mut td, input.as_ref(), &|args: &[&OsStr]| {
        run_ok(P::cmd(args))?;
        Ok(())
    })
}

#[macro_export]
macro_rules! test_readonly_input_file {
    ($program: ident) => {
        #[test]
        fn readonly_input_file() -> Result<()> {
            test_readonly_input_file::<$program>()
        }
    };
}

//------------------------------------------
