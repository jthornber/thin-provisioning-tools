use anyhow::Result;
use std::path::PathBuf;
use thinp::file_utils;
use thinp_dev::cache::metadata_generator::CacheGenerator;

use crate::args;
use crate::common::cache_xml_generator::write_xml;
use crate::common::process::*;
use crate::common::target::*;
use crate::common::test_dir::TestDir;

//-----------------------------------------------

pub fn mk_valid_xml(td: &mut TestDir) -> Result<PathBuf> {
    let xml = td.mk_path("meta.xml");
    let mut gen = CacheGenerator::new(512, 128, 1024, 80, 50); // bs, cblocks, oblocks, res, dirty
    write_xml(&xml, &mut gen)?;
    Ok(xml)
}

pub fn mk_valid_md(td: &mut TestDir) -> Result<PathBuf> {
    let xml = td.mk_path("meta.xml");
    let md = td.mk_path("meta.bin");

    let mut gen = CacheGenerator::new(512, 4096, 32768, 80, 50);
    write_xml(&xml, &mut gen)?;

    let _file = file_utils::create_sized_file(&md, 4096 * 4096);
    run_ok(cache_restore_cmd(args!["-i", &xml, "-o", &md]))?;

    Ok(md)
}

//-----------------------------------------------
