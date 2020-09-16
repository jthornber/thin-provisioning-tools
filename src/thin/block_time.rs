use nom::{number::complete::*, IResult};
use std::fmt;

use crate::pdata::unpack::*;

//------------------------------------------

#[derive(Clone)]
pub struct BlockTime {
    pub block: u64,
    pub time: u32,
}

impl Unpack for BlockTime {
    fn disk_size() -> u32 {
        8
    }

    fn unpack(i: &[u8]) -> IResult<&[u8], BlockTime> {
        let (i, n) = le_u64(i)?;
        let block = n >> 24;
        let time = n & ((1 << 24) - 1);

        Ok((
            i,
            BlockTime {
                block,
                time: time as u32,
            },
        ))
    }
}

impl fmt::Display for BlockTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} @ {}", self.block, self.time)
    }
}

//------------------------------------------
