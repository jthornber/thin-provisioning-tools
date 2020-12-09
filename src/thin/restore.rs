use anyhow::Result;

use std::collections::{BTreeMap, BTreeSet};
use std::fs::OpenOptions;
use std::path::Path;
use std::sync::Arc;

use crate::report::*;

use crate::thin::block_time::*;
use crate::thin::device_detail::*;
use crate::thin::superblock::*;
use crate::thin::xml::{self, *};

//------------------------------------------

#[derive(Default)]
struct Pass1 {
    //
}

impl MetadataVisitor for Pass1 {
    fn superblock_b(&mut self, sb: &xml::Superblock) -> Result<Visit> {
        todo!();
    }

    fn superblock_e(&mut self) -> Result<Visit> {
        todo!();
    }

    fn def_shared_b(&mut self, name: &str) -> Result<Visit> {
        todo!();
    }

    fn def_shared_e(&mut self) -> Result<Visit> {
        todo!();
    }

    fn device_b(&mut self, d: &Device) -> Result<Visit> {
        todo!();
    }

    fn device_e(&mut self) -> Result<Visit> {
        todo!();
    }

    fn map(&mut self, m: &Map) -> Result<Visit> {
        todo!();
    }

    fn ref_shared(&mut self, name: &str) -> Result<Visit> {
        todo!();
    }

    fn eof(&mut self) -> Result<Visit> {
        todo!();
    }
}

//------------------------------------------

pub struct ThinRestoreOptions<'a> {
    pub input: &'a Path,
    pub output: &'a Path,
    pub async_io: bool,
    pub report: Arc<Report>,
}

//------------------------------------------

pub fn restore(opts: ThinRestoreOptions) -> Result<()> {
    let input = OpenOptions::new()
        .read(true)
        .write(false)
        .open(opts.input)?;

    let mut pass = Pass1::default();
    xml::read(input, &mut pass)?;

    Ok(())
}

//------------------------------------------
