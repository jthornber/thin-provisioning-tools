use anyhow::Result;
use std::path::{Path, PathBuf};

use thinp::era::metadata_generator::CleanShutdownMeta;
use thinp::file_utils;

use crate::args;
use crate::common::era_xml_generator::write_xml;
use crate::common::process::*;
use crate::common::target::*;
use crate::common::test_dir::TestDir;

//-----------------------------------------------

pub fn mk_valid_xml(td: &mut TestDir) -> Result<PathBuf> {
    let xml = td.mk_path("meta.xml");
    let mut gen = CleanShutdownMeta::new(128, 512, 32, 4); // bs, nr_blocks, era, nr_wsets
    write_xml(&xml, &mut gen)?;
    Ok(xml)
}

pub fn mk_valid_md(td: &mut TestDir) -> Result<PathBuf> {
    let xml = td.mk_path("meta.xml");
    let md = td.mk_path("meta.bin");

    let mut gen = CleanShutdownMeta::new(128, 512, 32, 4);
    write_xml(&xml, &mut gen)?;

    let _file = file_utils::create_sized_file(&md, 4096 * 4096);
    run_ok(era_restore_cmd(args!["-i", &xml, "-o", &md]))?;

    Ok(md)
}

// Metadata with era 5 current and blocks 10..13 marked in its writeset.
// With @dirty it's marked as not shut down cleanly by the kernel's lazy mode.
pub fn mk_lazy_md(td: &mut TestDir, dirty: bool) -> Result<PathBuf> {
    let xml = td.mk_path("lazy.xml");
    let md = td.mk_path("lazy.bin");

    let dirty_attr = if dirty { " dirty=\"true\"" } else { "" };
    let content = format!(
        "<superblock uuid=\"\" block_size=\"128\" nr_blocks=\"200\" current_era=\"5\"{}>
  <writeset era=\"5\" nr_bits=\"200\">
    <marked block_begin=\"10\" len=\"3\"/>
  </writeset>
  <era_array>
  </era_array>
</superblock>",
        dirty_attr
    );
    std::fs::write(&xml, content)?;

    let _file = file_utils::create_sized_file(&md, 4096 * 4096);
    run_ok(era_restore_cmd(args!["-i", &xml, "-o", &md]))?;

    Ok(md)
}

// Returns (flags, version) from the superblock.
pub fn read_sb_flags_version(md: &Path) -> Result<(u32, u32)> {
    let buf = std::fs::read(md)?;
    let flags = u32::from_le_bytes(buf[4..8].try_into()?);
    let version = u32::from_le_bytes(buf[40..44].try_into()?);
    Ok((flags, version))
}

//-----------------------------------------------

fn unpack_metadata(input: &Path, output: &Path) -> Result<()> {
    let args = args!["-i", input, "-o", output];
    run_ok(thin_metadata_unpack_cmd(args))?;
    Ok(())
}

pub fn prep_metadata_from_file(td: &mut TestDir, filename: &str) -> Result<PathBuf> {
    let mut input = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    input.push("tests/testdata");
    input.push(filename);
    let output = td.mk_path("emeta.bin");
    unpack_metadata(&input, &output)?;
    Ok(output)
}

//-----------------------------------------------
