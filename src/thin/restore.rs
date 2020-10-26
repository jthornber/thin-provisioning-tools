use anyhow::Result;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::io_engine::{AsyncIoEngine, IoEngine, SyncIoEngine};
use crate::pdata::btree::{self, *};
use crate::pdata::space_map::*;
use crate::pdata::unpack::*;
use crate::report::*;
use crate::thin::block_time::*;
use crate::thin::device_detail::*;
use crate::thin::superblock::*;
use crate::thin::xml::{self, MetadataVisitor};

//------------------------------------------

pub struct ThinRestoreOptions<'a> {
    pub input: &'a Path,
    pub output: &'a Path,
    pub async_io: bool,
    pub report: Arc<Report>,
}

//------------------------------------------

pub fn restore(opts: ThinRestoreOptions) -> Result<()> {
    todo!();
}

//------------------------------------------
