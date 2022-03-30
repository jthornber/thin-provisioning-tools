use byteorder::{LittleEndian, WriteBytesExt};
use nom::{number::complete::*, IResult};
use std::fmt;
use std::io;

use crate::pdata::unpack::*;

//------------------------------------------

#[derive(Clone, Copy, Debug)]
pub struct DeviceDetail {
    pub mapped_blocks: u64,
    pub transaction_id: u64,
    pub creation_time: u32,
    pub snapshotted_time: u32,
}

impl fmt::Display for DeviceDetail {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "mapped = {}, trans = {}, create = {}, snap = {}",
            self.mapped_blocks, self.transaction_id, self.creation_time, self.snapshotted_time
        )?;
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

impl Pack for DeviceDetail {
    fn pack<W: WriteBytesExt>(&self, w: &mut W) -> io::Result<()> {
        w.write_u64::<LittleEndian>(self.mapped_blocks)?;
        w.write_u64::<LittleEndian>(self.transaction_id)?;
        w.write_u32::<LittleEndian>(self.creation_time)?;
        w.write_u32::<LittleEndian>(self.snapshotted_time)
    }
}

//------------------------------------------
