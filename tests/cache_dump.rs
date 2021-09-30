use anyhow::Result;
use std::fs::OpenOptions;
use std::io::Write;

mod common;

use common::cache::*;
use common::common_args::*;
use common::fixture::*;
use common::input_arg::*;
use common::process::*;
use common::program::*;
use common::target::*;
use common::test_dir::*;

//------------------------------------------

const USAGE: &str = "Usage: cache_dump [options] {device|file}\n\
                     Options:\n  \
                       {-h|--help}\n  \
                       {-o <xml file>}\n  \
                       {-V|--version}\n  \
                       {--repair}";

//------------------------------------------

struct CacheDump;

impl<'a> Program<'a> for CacheDump {
    fn name() -> &'a str {
        "cache_dump"
    }

    fn path() -> &'a std::ffi::OsStr {
        CACHE_DUMP.as_ref()
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

impl<'a> InputProgram<'a> for CacheDump {
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

test_accepts_help!(CacheDump);
test_accepts_version!(CacheDump);
test_rejects_bad_option!(CacheDump);

test_missing_input_arg!(CacheDump);
test_input_file_not_found!(CacheDump);
test_input_cannot_be_a_directory!(CacheDump);
test_unreadable_input_file!(CacheDump);

//------------------------------------------

// TODO: share with thin_dump
#[test]
fn dump_restore_cycle() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;
    let output = run_ok_raw(CACHE_DUMP, args![&md])?;

    let xml = td.mk_path("meta.xml");
    let mut file = OpenOptions::new()
        .read(false)
        .write(true)
        .create(true)
        .open(&xml)?;
    file.write_all(&output.stdout[0..])?;
    drop(file);

    let md2 = mk_zeroed_md(&mut td)?;
    run_ok(CACHE_RESTORE, args!["-i", &xml, "-o", &md2])?;

    let output2 = run_ok_raw(CACHE_DUMP, args![&md2])?;
    assert_eq!(output.stdout, output2.stdout);

    Ok(())
}
