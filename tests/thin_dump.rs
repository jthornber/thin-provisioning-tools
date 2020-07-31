use anyhow::Result;
use thinp::file_utils;
use std::fs::OpenOptions;
use std::io::{Write};
use std::str::from_utf8;

mod common;

use common::xml_generator::{write_xml, FragmentedS, SingleThinS};
use common::*;

//------------------------------------------

#[test]
fn small_input_file() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = td.mk_path("meta.bin");
    file_utils::create_sized_file(&md, 512)?;
    let _stderr = run_fail(thin_dump!(&md))?;
    Ok(())
}

#[test]
fn dump_restore_cycle() -> Result<()> {
    let mut td = TestDir::new()?;

    let md = mk_valid_md(&mut td)?;
    let output = thin_dump!(&md).run()?;

    let xml = td.mk_path("meta.xml");
    let mut file = OpenOptions::new().read(false).write(true).create(true).open(&xml)?;
    file.write_all(&output.stdout[0..])?;
    drop(file);

    let md2 = mk_zeroed_md(&mut td)?;
    thin_restore!("-i", &xml, "-o", &md2).run()?;

    let output2 = thin_dump!(&md2).run()?;
    assert_eq!(output.stdout, output2.stdout);

    Ok(())
}

#[test]
fn no_stderr() -> Result<()> {
    let mut td = TestDir::new()?;

    let md = mk_valid_md(&mut td)?;
    let output = thin_dump!(&md).run()?;

    assert_eq!(output.stderr.len(), 0);
    Ok(())
}

fn override_something(flag: &str, value: &str, pattern: &str) -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;
    let output = thin_dump!(&md, flag, value).run()?;

    assert_eq!(output.stderr.len(), 0);
    assert!(from_utf8(&output.stdout[0..])?.contains(pattern));
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

#[test]
fn repair_superblock() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;
    let before = thin_dump!("--transaction-id=5", "--data-block-size=128", "--nr-data-blocks=4096000", &md).run()?;
    damage_superblock(&md)?;

    let after = thin_dump!("--repair", "--transaction-id=5", "--data-block-size=128", "--nr-data-blocks=4096000", &md).run()?;
    assert_eq!(after.stderr.len(), 0);
    assert_eq!(before.stdout, after.stdout);
    
    Ok(())
}

#[test]
fn missing_transaction_id() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;
    damage_superblock(&md)?;
    let stderr = run_fail(thin_dump!("--repair", "--data-block-size=128", "--nr-data-blocks=4096000", &md))?;
    assert!(stderr.contains("transaction id"));
    Ok(())
}

#[test]
fn missing_data_block_size() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;
    damage_superblock(&md)?;
    let stderr = run_fail(thin_dump!("--repair", "--transaction-id=5", "--nr-data-blocks=4096000", &md))?;
    assert!(stderr.contains("data block size"));
    Ok(())
}

#[test]
fn missing_nr_data_blocks() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;
    damage_superblock(&md)?;
    let stderr = run_fail(thin_dump!("--repair", "--transaction-id=5", "--data-block-size=128", &md))?;
    assert!(stderr.contains("nr data blocks"));
    Ok(())
}


//  (define-scenario (thin-dump repair-superblock missing-data-block-size)
//    "--data-block-size is mandatory if the superblock is damaged"
//    (with-damaged-superblock (md)
//      (run-fail-rcv (_ stderr) (thin-dump "--repair" "--transaction-id=5" "--nr-data-blocks=4096000" md)
//        (assert-matches ".*data block size.*" stderr))))
//
//  (define-scenario (thin-dump repair-superblock missing-nr-data-blocks)
//    "--nr-data-blocks is mandatory if the superblock is damaged"
//    (with-damaged-superblock (md)
//      (run-fail-rcv (_ stderr) (thin-dump "--repair" "--transaction-id=5" "--data-block-size=128" md)
//        (assert-matches ".*nr data blocks.*" stderr))))
//
