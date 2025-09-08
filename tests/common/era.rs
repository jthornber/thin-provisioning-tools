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
