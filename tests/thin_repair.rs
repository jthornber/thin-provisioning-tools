use anyhow::Result;
use std::str::from_utf8;
use thinp::version::tools_version;

mod common;
use common::test_dir::*;
use common::*;

//------------------------------------------

#[test]
fn accepts_v() -> Result<()> {
    let stdout = thin_repair!("-V").read()?;
    assert!(stdout.contains(tools_version()));
    Ok(())
}

#[test]
fn accepts_version() -> Result<()> {
    let stdout = thin_repair!("--version").read()?;
    assert!(stdout.contains(tools_version()));
    Ok(())
}

const USAGE: &str = "Usage: thin_repair [options] {device|file}\nOptions:\n  {-h|--help}\n  {-i|--input} <input metadata (binary format)>\n  {-o|--output} <output metadata (binary format)>\n  {--transaction-id} <natural>\n  {--data-block-size} <natural>\n  {--nr-data-blocks} <natural>\n  {-V|--version}";

#[test]
fn accepts_h() -> Result<()> {
    let stdout = thin_repair!("-h").read()?;
    assert_eq!(stdout, USAGE);
    Ok(())
}

#[test]
fn accepts_help() -> Result<()> {
    let stdout = thin_repair!("--help").read()?;
    assert_eq!(stdout, USAGE);
    Ok(())
}

#[test]
fn dont_repair_xml() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_zeroed_md(&mut td)?;
    let xml = mk_valid_xml(&mut td)?;
    run_fail(thin_repair!("-i", &xml, "-o", &md))?;
    Ok(())
}

#[test]
fn missing_input_file() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_zeroed_md(&mut td)?;
    let stderr = run_fail(thin_repair!("-i", "no-such-file", "-o", &md))?;
    assert!(superblock_all_zeroes(&md)?);
    // TODO: replace with msg::FILE_NOT_FOUND once the rust version is ready
    assert!(stderr.contains("Couldn't stat file"));
    Ok(())
}

#[test]
fn garbage_input_file() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_zeroed_md(&mut td)?;
    let md2 = mk_zeroed_md(&mut td)?;
    run_fail(thin_repair!("-i", &md, "-o", &md2))?;
    assert!(superblock_all_zeroes(&md2)?);
    Ok(())
}

#[test]
fn missing_output_file() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;
    let stderr = run_fail(thin_repair!("-i", &md))?;
    // TODO: replace with msg::MISSING_OUTPUT_ARG once the rust version is ready
    assert!(stderr.contains("No output file provided."));
    Ok(())
}

fn override_thing(flag: &str, val: &str, pattern: &str) -> Result<()> {
    let mut td = TestDir::new()?;
    let md1 = mk_valid_md(&mut td)?;
    let md2 = mk_zeroed_md(&mut td)?;
    let output = thin_repair!(flag, val, "-i", &md1, "-o", &md2).run()?;
    assert_eq!(output.stderr.len(), 0);
    let output = thin_dump!(&md2).run()?;
    assert!(from_utf8(&output.stdout[0..])?.contains(pattern));
    Ok(())
}

#[test]
fn override_transaction_id() -> Result<()> {
    override_thing("--transaction-id", "2345", "transaction=\"2345\"")
}

#[test]
fn override_data_block_size() -> Result<()> {
    override_thing("--data-block-size", "8192", "data_block_size=\"8192\"")
}

#[test]
fn override_nr_data_blocks() -> Result<()> {
    override_thing("--nr-data-blocks", "234500", "nr_data_blocks=\"234500\"")
}

#[test]
fn superblock_succeeds() -> Result<()> {
    let mut td = TestDir::new()?;
    let md1 = mk_valid_md(&mut td)?;
    let original = thin_dump!(
        "--transaction-id=5",
        "--data-block-size=128",
        "--nr-data-blocks=4096000",
        &md1
    )
    .run()?;
    assert_eq!(original.stderr.len(), 0);
    damage_superblock(&md1)?;
    let md2 = mk_zeroed_md(&mut td)?;
    thin_repair!(
        "--transaction-id=5",
        "--data-block-size=128",
        "--nr-data-blocks=4096000",
        "-i",
        &md1,
        "-o",
        &md2
    )
    .run()?;
    let repaired = thin_dump!(&md2).run()?;
    assert_eq!(repaired.stderr.len(), 0);
    assert_eq!(original.stdout, repaired.stdout);
    Ok(())
}

fn missing_thing(flag1: &str, flag2: &str, pattern: &str) -> Result<()> {
    let mut td = TestDir::new()?;
    let md1 = mk_valid_md(&mut td)?;
    damage_superblock(&md1)?;
    let md2 = mk_zeroed_md(&mut td)?;
    let stderr = run_fail(thin_repair!(flag1, flag2, "-i", &md1, "-o", &md2))?;
    assert!(stderr.contains(pattern));
    Ok(())
}

#[test]
fn missing_transaction_id() -> Result<()> {
    missing_thing(
        "--data-block-size=128",
        "--nr-data-blocks=4096000",
        "transaction id",
    )
}

#[test]
fn missing_data_block_size() -> Result<()> {
    missing_thing(
        "--transaction-id=5",
        "--nr-data-blocks=4096000",
        "data block size",
    )
}

#[test]
fn missing_nr_data_blocks() -> Result<()> {
    missing_thing(
        "--transaction-id=5",
        "--data-block-size=128",
        "nr data blocks",
    )
}
