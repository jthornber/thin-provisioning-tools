use anyhow::Result;

mod common;

use common::common_args::*;
use common::input_arg::*;
use common::output_option::*;
use common::test_dir::*;
use common::*;

//------------------------------------------

const USAGE: &str = "Usage: thin_restore [options]\n\
                     Options:\n  \
                       {-h|--help}\n  \
                       {-i|--input} <input xml file>\n  \
                       {-o|--output} <output device or file>\n  \
                       {--transaction-id} <natural>\n  \
                       {--data-block-size} <natural>\n  \
                       {--nr-data-blocks} <natural>\n  \
                       {-q|--quiet}\n  \
                       {-V|--version}";

//------------------------------------------

struct ThinRestore;

impl<'a> Program<'a> for ThinRestore {
    fn name() -> &'a str {
        "thin_restore"
    }

    fn path() -> &'a str {
        THIN_RESTORE
    }

    fn usage() -> &'a str {
        USAGE
    }

    fn arg_type() -> ArgType {
        ArgType::IoOptions
    }

    fn bad_option_hint(option: &str) -> String {
        msg::bad_option_hint(option)
    }
}

impl<'a> InputProgram<'a> for ThinRestore {
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
        "" // we don't intent to verify error messages of XML parsing
    }
}

impl<'a> OutputProgram<'a> for ThinRestore {
    fn file_not_found() -> &'a str {
        msg::FILE_NOT_FOUND
    }

    fn missing_output_arg() -> &'a str {
        msg::MISSING_OUTPUT_ARG
    }
}

impl<'a> BinaryOutputProgram<'_> for ThinRestore {}

//-----------------------------------------

test_accepts_help!(ThinRestore);
test_accepts_version!(ThinRestore);

test_missing_input_option!(ThinRestore);
test_input_file_not_found!(ThinRestore);
test_corrupted_input_data!(ThinRestore);

test_missing_output_option!(ThinRestore);
test_tiny_output_file!(ThinRestore);

//-----------------------------------------

// TODO: share with cache_restore, era_restore

fn quiet_flag(flag: &str) -> Result<()> {
    let mut td = TestDir::new()?;
    let xml = mk_valid_xml(&mut td)?;
    let md = mk_zeroed_md(&mut td)?;

    let xml_path = xml.to_str().unwrap();
    let md_path = md.to_str().unwrap();
    let output = run_ok_raw(THIN_RESTORE, &["-i", xml_path, "-o", md_path, flag])?;

    assert_eq!(output.stdout.len(), 0);
    assert_eq!(output.stderr.len(), 0);
    Ok(())
}

#[test]
fn accepts_q() -> Result<()> {
    quiet_flag("-q")
}

#[test]
fn accepts_quiet() -> Result<()> {
    quiet_flag("--quiet")
}

//-----------------------------------------

// TODO: share with thin_dump

fn override_something(flag: &str, value: &str, pattern: &str) -> Result<()> {
    let mut td = TestDir::new()?;
    let xml = mk_valid_xml(&mut td)?;
    let md = mk_zeroed_md(&mut td)?;

    let xml_path = xml.to_str().unwrap();
    let md_path = md.to_str().unwrap();
    run_ok(THIN_RESTORE, &["-i", xml_path, "-o", md_path, flag, value])?;

    let output = run_ok(THIN_DUMP, &[md_path])?;
    assert!(output.contains(pattern));
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

//-----------------------------------------
