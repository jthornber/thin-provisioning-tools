use anyhow::Result;
use std::path::Path;
use std::sync::Arc;

use crate::report::*;

//------------------------------------------

pub struct ThinRestoreOptions<'a> {
    pub input: &'a Path,
    pub output: &'a Path,
    pub async_io: bool,
    pub report: Arc<Report>,
}

//------------------------------------------

pub fn restore(_opts: ThinRestoreOptions) -> Result<()> {
    todo!();
}

//------------------------------------------
