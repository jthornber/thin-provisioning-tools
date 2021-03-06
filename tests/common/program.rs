use anyhow::Result;
use std::ffi::OsStr;
use std::path::PathBuf;

use crate::common::test_dir::TestDir;

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
