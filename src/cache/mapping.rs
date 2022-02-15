use byteorder::WriteBytesExt;
use nom::number::complete::*;
use nom::IResult;
use std::io;

use crate::pdata::unpack::*;

//------------------------------------------

pub const MAX_ORIGIN_BLOCKS: u64 = 1 << 48;
const FLAGS_MASK: u64 = (1 << 16) - 1;

//------------------------------------------

pub enum MappingFlags {
    Valid = 1,
    Dirty = 2,
}

#[derive(Clone, Copy, Default)]
pub struct Mapping {
    pub oblock: u64,
    pub flags: u32,
}

impl Mapping {
    pub fn is_valid(&self) -> bool {
        (self.flags & MappingFlags::Valid as u32) != 0
    }

    pub fn is_dirty(&self) -> bool {
        (self.flags & MappingFlags::Dirty as u32) != 0
    }

    pub fn set_dirty(&mut self, dirty: bool) {
        if dirty {
            self.flags |= MappingFlags::Dirty as u32;
        } else {
            self.flags &= !(MappingFlags::Dirty as u32);
        }
    }
}

impl Unpack for Mapping {
    fn disk_size() -> u32 {
        8
    }

    fn unpack(i: &[u8]) -> IResult<&[u8], Mapping> {
        let (i, n) = le_u64(i)?;
        let oblock = n >> 16;
        let flags = n & FLAGS_MASK;

        Ok((
            i,
            Mapping {
                oblock,
                flags: flags as u32,
            },
        ))
    }
}

impl Pack for Mapping {
    fn pack<W: WriteBytesExt>(&self, data: &mut W) -> io::Result<()> {
        let m: u64 = (self.oblock << 16) | self.flags as u64;
        m.pack(data)
    }
}

//------------------------------------------
