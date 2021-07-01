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

test_accepts_help!(THIN_RESTORE, USAGE);
test_accepts_version!(THIN_RESTORE);

test_missing_input_option!(THIN_RESTORE);
test_input_file_not_found!(THIN_RESTORE, OPTION);
test_corrupted_input_data!(THIN_RESTORE, OPTION);

test_missing_output_option!(THIN_RESTORE, mk_valid_xml);
test_tiny_output_file!(THIN_RESTORE, mk_valid_xml);

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
