use nom::{number::complete::*, IResult};
use std::fmt;

use crate::pdata::unpack::*;

//------------------------------------------

// TODO: build a data structure representating an array?

// FIXME: rename this struct
pub struct ArrayBlockEntry {
    pub block: u64,
}

impl Unpack for ArrayBlockEntry {
    fn disk_size() -> u32 {
        8
    }

    fn unpack(i: &[u8]) -> IResult<&[u8], ArrayBlockEntry> {
        let (i, n) = le_u64(i)?;
        let block = n;

        Ok((i, ArrayBlockEntry { block }))
    }
}

impl fmt::Display for ArrayBlockEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.block)
    }
}

//------------------------------------------
