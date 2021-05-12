use nom::{multi::count, number::complete::*, IResult};
use std::fmt;
use thiserror::Error;

use crate::checksum;
use crate::io_engine::BLOCK_SIZE;
use crate::pdata::btree;
use crate::pdata::unpack::Unpack;

//------------------------------------------

const ARRAY_BLOCK_HEADER_SIZE: u32 = 24;

pub struct ArrayBlockHeader {
    pub csum: u32,
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
                blocknr,
            },
        ))
    }
}

pub struct ArrayBlock<V: Unpack> {
    pub header: ArrayBlockHeader,
    pub values: Vec<V>,
}

//------------------------------------------

#[derive(Error, Clone, Debug)]
pub enum ArrayError {
    //#[error("io_error {0}")]
    IoError(u64),

    //#[error("block error: {0}")]
    BlockError(String),

    //#[error("value error: {0}")]
    ValueError(String),

    //#[error("index: {0:?}")]
    IndexContext(u64, Box<ArrayError>),

    //#[error("aggregate: {0:?}")]
    Aggregate(Vec<ArrayError>),

    //#[error("{0:?}, {1}")]
    Path(Vec<u64>, Box<ArrayError>),

    #[error(transparent)]
    BTreeError(#[from] btree::BTreeError),
}

impl fmt::Display for ArrayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ArrayError::IoError(b) => write!(f, "io error {}", b),
            ArrayError::BlockError(msg) => write!(f, "block error: {}", msg),
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
            ArrayError::Path(path, e) => write!(f, "{} {}", e, btree::encode_node_path(path)),
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
        Box::new(ArrayError::BlockError(msg.to_string())),
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
    let bt = checksum::metadata_block_type(data);
    if bt != checksum::BT::ARRAY {
        return Err(array_block_err(
            path,
            &format!(
                "checksum failed for array block {}, {:?}",
                path.last().unwrap(),
                bt
            ),
        ));
    }

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

    // TODO: check nr_entries < max_entries

    // TODO: check block_nr

    let (_i, values) = convert_result(path, count(V::unpack, header.nr_entries as usize)(i))?;

    Ok(ArrayBlock { header, values })
}

//------------------------------------------
