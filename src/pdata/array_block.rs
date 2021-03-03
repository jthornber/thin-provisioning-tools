use anyhow::anyhow;
use anyhow::Result;
use nom::{multi::count, number::complete::*, IResult};

use crate::pdata::unpack::Unpack;

//------------------------------------------

pub struct ArrayBlockHeader {
    pub csum: u32,
    pub max_entries: u32,
    pub nr_entries: u32,
    pub value_size: u32,
    pub blocknr: u64,
}

impl Unpack for ArrayBlockHeader {
    fn disk_size() -> u32 {
        24
    }

    fn unpack(data: &[u8]) -> IResult<&[u8], ArrayBlockHeader> {
        let (i, csum) = le_u32(data)?;
        let (i, max_entries) = le_u32(i)?;
        let (i, nr_entries) = le_u32(i)?;
        let (i, value_size) = le_u32(i)?;
        let (i, blocknr) = le_u64(i)?;

        Ok((
            i,
            ArrayBlockHeader {
                csum,
                max_entries,
                nr_entries,
                value_size,
                blocknr
            },
        ))
    }
}

pub struct ArrayBlock<V: Unpack> {
    pub header: ArrayBlockHeader,
    pub values: Vec<V>,
}

impl<V: Unpack> ArrayBlock<V> {
    pub fn get_header(&self) -> &ArrayBlockHeader {
        &self.header
    }
}

fn convert_result<V>(r: IResult<&[u8], V>) -> Result<(&[u8], V)> {
    r.map_err(|_| anyhow!("parse error"))
}

pub fn unpack_array_block<V: Unpack>(
    data: &[u8],
) -> Result<ArrayBlock<V>> {
    // TODO: collect errors
    let (i, header) = ArrayBlockHeader::unpack(data).map_err(|_e| anyhow!("Couldn't parse header"))?;
    let (_i, values) = convert_result(count(V::unpack, header.nr_entries as usize)(i))?;

    Ok(ArrayBlock {
        header,
        values,
    })
}

//------------------------------------------
