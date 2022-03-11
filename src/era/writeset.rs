use byteorder::{LittleEndian, WriteBytesExt};
use nom::{number::complete::*, IResult};
use std::io;

use crate::pdata::unpack::*;

//------------------------------------------

#[derive(Clone, Copy, Debug)]
pub struct Writeset {
    pub nr_bits: u32,
    pub root: u64,
}

impl Unpack for Writeset {
    fn disk_size() -> u32 {
        12
    }

    fn unpack(i: &[u8]) -> IResult<&[u8], Writeset> {
        let (i, nr_bits) = le_u32(i)?;
        let (i, root) = le_u64(i)?;
        Ok((i, Writeset { nr_bits, root }))
    }
}

impl Pack for Writeset {
    fn pack<W: WriteBytesExt>(&self, w: &mut W) -> io::Result<()> {
        w.write_u32::<LittleEndian>(self.nr_bits)?;
        w.write_u64::<LittleEndian>(self.root)
    }
}

//------------------------------------------
