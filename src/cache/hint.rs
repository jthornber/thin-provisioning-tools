use nom::IResult;
use std::marker::PhantomData;
use std::convert::TryInto;

use crate::pdata::unpack::*;

//------------------------------------------

#[derive(Clone, Copy)]
pub struct Hint<Width> {
    pub hint: [u8; 4], // FIXME: support various hint sizes
    _not_used: PhantomData<Width>,
}

impl<Width: typenum::Unsigned> Unpack for Hint<Width> {
    fn disk_size() -> u32 {
        Width::to_u32()
    }

    // FIXME: support different width
    fn unpack(i: &[u8]) -> IResult<&[u8], Hint<Width>> {
        let size = Width::to_usize();
        Ok((
            &i[size..],
            Hint {
                hint: i[0..size].try_into().unwrap(),
                _not_used: PhantomData,
            },
        ))
    }
}

//------------------------------------------
