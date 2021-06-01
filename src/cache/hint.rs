use anyhow::Result;
use byteorder::WriteBytesExt;
use nom::IResult;
use std::convert::TryInto;

use crate::pdata::unpack::*;

//------------------------------------------

#[derive(Clone, Copy)]
pub struct Hint {
    pub hint: [u8; 4],
}

impl Unpack for Hint {
    fn disk_size() -> u32 {
        4
    }

    fn unpack(i: &[u8]) -> IResult<&[u8], Hint> {
        let size = 4;
        Ok((
            &i[size..],
            Hint {
                hint: i[0..size].try_into().unwrap(),
            },
        ))
    }
}

impl Pack for Hint {
    fn pack<W: WriteBytesExt>(&self, data: &mut W) -> Result<()> {
        for v in &self.hint {
            data.write_u8(*v)?;
        }
        Ok(())
    }
}

impl Default for Hint {
    fn default() -> Self {
        Hint { hint: [0; 4] }
    }
}

//------------------------------------------
