use anyhow::Result;
use std::path::PathBuf;

pub use crate::common::process::*;
use crate::common::test_dir::TestDir;

//------------------------------------------

pub enum ArgType {
    InputArg,
    IoOptions,
}

pub trait Program<'a> {
    fn name() -> &'a str;
    fn cmd<I>(args: I) -> Command
    where
        I: IntoIterator,
        I::Item: Into<std::ffi::OsString>;
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

pub trait MetadataReader<'a>: InputProgram<'a> {}

pub trait OutputProgram<'a>: InputProgram<'a> {
    // error messages
    fn missing_output_arg() -> &'a str;
}

// programs that write existed files
pub trait MetadataWriter<'a>: OutputProgram<'a> {
    // error messages
    fn file_not_found() -> &'a str;
}

// programs that create output files (O_CREAT)
pub trait MetadataCreator<'a>: OutputProgram<'a> {}

//------------------------------------------
