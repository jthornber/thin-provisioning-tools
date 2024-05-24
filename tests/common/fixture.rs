use anyhow::Result;
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use thinp::file_utils;

use crate::common::test_dir::TestDir;

//------------------------------------------

pub fn mk_zeroed_md(td: &mut TestDir) -> Result<PathBuf> {
    let md = td.mk_path("meta.bin");
    eprintln!("path = {:?}", md);
    let _file = file_utils::create_sized_file(&md, 1024 * 1024 * 16);
    Ok(md)
}

pub fn mk_zeroed_md_sized(td: &mut TestDir, nr_bytes: u64) -> Result<PathBuf> {
    let md = td.mk_path("meta.bin");
    eprintln!("path = {:?}", md);
    let _file = file_utils::create_sized_file(&md, nr_bytes);
    Ok(md)
}

pub fn damage_superblock(path: &Path) -> Result<()> {
    let mut output = OpenOptions::new().read(false).write(true).open(path)?;
    let buf = [0u8; 512];
    output.write_all(&buf)?;
    Ok(())
}

//------------------------------------------

pub fn md5(md: &Path) -> Result<String> {
    let output = duct::cmd!("md5sum", "-b", &md).stdout_capture().run()?;
    let csum = std::str::from_utf8(&output.stdout[0..])?.to_string();
    let csum = csum.split_ascii_whitespace().next().unwrap().to_string();
    Ok(csum)
}

// This checksums the file before and after the thunk is run to
// ensure it is unchanged.
pub fn ensure_untouched<F>(p: &Path, thunk: F) -> Result<()>
where
    F: Fn() -> Result<()>,
{
    let csum = md5(p)?;
    thunk()?;
    assert_eq!(csum, md5(p)?);
    Ok(())
}

pub fn superblock_all_zeroes(path: &Path) -> Result<bool> {
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

pub fn ensure_superblock_zeroed<F>(p: &Path, thunk: F) -> Result<()>
where
    F: Fn() -> Result<()>,
{
    thunk()?;
    assert!(superblock_all_zeroes(p)?);
    Ok(())
}

//------------------------------------------

pub fn write_file(p: &Path, buf: &[u8]) -> Result<()> {
    let mut output = std::fs::File::create(p)?;
    output.write_all(buf)?;
    Ok(())
}

//------------------------------------------
