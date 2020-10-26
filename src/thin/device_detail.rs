use std::fmt;

use crate::pdata::unpack::*;
use nom::{number::complete::*, IResult};

//------------------------------------------

#[derive(Clone, Copy)]
pub struct DeviceDetail {
    pub mapped_blocks: u64,
    pub transaction_id: u64,
    pub creation_time: u32,
    pub snapshotted_time: u32,
}

impl fmt::Display for DeviceDetail {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "mapped = {}, trans = {}, create = {}, snap = {}",
              self.mapped_blocks,
              self.transaction_id,
              self.creation_time,
              self.snapshotted_time)?;
        Ok(())
    }
}

impl Unpack for DeviceDetail {
    fn disk_size() -> u32 {
        24
    }

    fn unpack(i: &[u8]) -> IResult<&[u8], DeviceDetail> {
        let (i, mapped_blocks) = le_u64(i)?;
        let (i, transaction_id) = le_u64(i)?;
        let (i, creation_time) = le_u32(i)?;
        let (i, snapshotted_time) = le_u32(i)?;

        Ok((
            i,
            DeviceDetail {
                mapped_blocks,
                transaction_id,
                creation_time,
                snapshotted_time,
            },
        ))
    }
}

//------------------------------------------
