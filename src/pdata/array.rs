use byteorder::{LittleEndian, WriteBytesExt};
use nom::{multi::count, number::complete::*, IResult};
use std::fmt;
use std::io;
use thiserror::Error;

use crate::io_engine::BLOCK_SIZE;
use crate::pdata::btree_error;
use crate::pdata::unpack::{Pack, Unpack};

#[cfg(test)]
mod tests;

//------------------------------------------

const ARRAY_BLOCK_HEADER_SIZE: u32 = 24;

#[derive(Debug, PartialEq, Eq)]
pub struct ArrayBlockHeader {
    pub max_entries: u32,
    pub nr_entries: u32,
    pub value_size: u32,
    pub blocknr: u64,
}

impl Unpack for ArrayBlockHeader {
    fn disk_size() -> u32 {
        ARRAY_BLOCK_HEADER_SIZE
    }

    fn unpack(data: &[u8]) -> IResult<&[u8], ArrayBlockHeader> {
        let (i, _csum) = le_u32(data)?;
        let (i, max_entries) = le_u32(i)?;
        let (i, nr_entries) = le_u32(i)?;
        let (i, value_size) = le_u32(i)?;
        let (i, blocknr) = le_u64(i)?;

        Ok((
            i,
            ArrayBlockHeader {
                max_entries,
                nr_entries,
                value_size,
                blocknr,
            },
        ))
    }
}

impl Pack for ArrayBlockHeader {
    fn pack<W: WriteBytesExt>(&self, w: &mut W) -> io::Result<()> {
        // csum needs to be calculated right for the whole metadata block.
        w.write_u32::<LittleEndian>(0)?;
        w.write_u32::<LittleEndian>(self.max_entries)?;
        w.write_u32::<LittleEndian>(self.nr_entries)?;
        w.write_u32::<LittleEndian>(self.value_size)?;
        w.write_u64::<LittleEndian>(self.blocknr)
    }
}

//------------------------------------------

pub struct ArrayBlock<V: Unpack> {
    pub header: ArrayBlockHeader,
    pub values: Vec<V>,
}

impl<V: Unpack> ArrayBlock<V> {
    pub fn set_block(&mut self, b: u64) {
        self.header.blocknr = b;
    }
}

//------------------------------------------

#[derive(Error, Clone, Debug)]
pub enum ArrayError {
    //#[error("io_error {0}")]
    IoError(u64),

    //#[error("block error: {0}")]
    ArrayBlockError(String),

    //#[error("value error: {0}")]
    ValueError(String),

    //#[error("index: {0:?}")]
    IndexContext(u64, Box<ArrayError>),

    //#[error("aggregate: {0:?}")]
    Aggregate(Vec<ArrayError>),

    //#[error("{0:?}, {1}")]
    Path(Vec<u64>, Box<ArrayError>),

    #[error(transparent)]
    BTreeError(#[from] btree_error::BTreeError),
}

impl fmt::Display for ArrayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ArrayError::IoError(b) => write!(f, "io error {}", b),
            ArrayError::ArrayBlockError(msg) => write!(f, "array block error: {}", msg),
            ArrayError::ValueError(msg) => write!(f, "value error: {}", msg),
            ArrayError::IndexContext(idx, e) => {
                write!(f, "{}, effecting index {}", e, idx)?;
                Ok(())
            }
            ArrayError::Aggregate(errs) => {
                for e in errs {
                    write!(f, "{}", e)?
                }
                Ok(())
            }
            ArrayError::Path(path, e) => write!(f, "{} {}", e, btree_error::encode_node_path(path)),
            ArrayError::BTreeError(e) => write!(f, "{}", e),
        }
    }
}

pub fn io_err(path: &[u64], blocknr: u64) -> ArrayError {
    ArrayError::Path(path.to_vec(), Box::new(ArrayError::IoError(blocknr)))
}

pub fn array_block_err(path: &[u64], msg: &str) -> ArrayError {
    ArrayError::Path(
        path.to_vec(),
        Box::new(ArrayError::ArrayBlockError(msg.to_string())),
    )
}

pub fn value_err(msg: String) -> ArrayError {
    ArrayError::ValueError(msg)
}

pub fn aggregate_error(errs: Vec<ArrayError>) -> ArrayError {
    ArrayError::Aggregate(errs)
}

impl ArrayError {
    pub fn index_context(self, index: u64) -> ArrayError {
        ArrayError::IndexContext(index, Box::new(self))
    }
}

pub type Result<T> = std::result::Result<T, ArrayError>;

//------------------------------------------

fn convert_result<'a, V>(path: &[u64], r: IResult<&'a [u8], V>) -> Result<(&'a [u8], V)> {
    r.map_err(|_| array_block_err(path, "parse error"))
}

pub fn unpack_array_block<V: Unpack>(path: &[u64], data: &[u8]) -> Result<ArrayBlock<V>> {
    let (i, header) = ArrayBlockHeader::unpack(data)
        .map_err(|_| array_block_err(path, "Couldn't parse array block header"))?;

    // check value_size
    if header.value_size != V::disk_size() {
        return Err(array_block_err(
            path,
            &format!(
                "value_size mismatch: expected {}, was {}",
                V::disk_size(),
                header.value_size
            ),
        ));
    }

    // check max_entries
    if header.value_size * header.max_entries + ARRAY_BLOCK_HEADER_SIZE > BLOCK_SIZE as u32 {
        return Err(array_block_err(
            path,
            &format!("max_entries is too large ({})", header.max_entries),
        ));
    }

    // check nr_entries
    if header.nr_entries > header.max_entries {
        return Err(array_block_err(path, "nr_entries > max_entries"));
    }

    let (_i, values) = convert_result(path, count(V::unpack, header.nr_entries as usize)(i))?;

    Ok(ArrayBlock { header, values })
}

pub fn pack_array_block<W: WriteBytesExt, V: Pack + Unpack>(
    ablock: &ArrayBlock<V>,
    w: &mut W,
) -> io::Result<()> {
    ablock.header.pack(w)?;
    for v in ablock.values.iter() {
        v.pack(w)?;
    }
    Ok(())
}

//------------------------------------------

pub fn calc_max_entries<V: Unpack>() -> usize {
    (BLOCK_SIZE - ArrayBlockHeader::disk_size() as usize) / V::disk_size() as usize
}

//------------------------------------------
