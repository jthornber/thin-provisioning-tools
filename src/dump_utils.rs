use anyhow::Context;
use std::sync::Arc;

use crate::checksum;
use crate::io_engine::IoEngine;
use crate::pdata::array::{self, unpack_array_block};
use crate::pdata::array_walker::ArrayVisitor;
use crate::pdata::unpack;

//------------------------------------------

// A wrapper for callers to identify the error type
#[derive(Debug)]
pub struct OutputError;

impl std::fmt::Display for OutputError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "output error")
    }
}

pub fn walk_array_blocks<V, I>(
    engine: Arc<dyn IoEngine + Send + Sync>,
    ablocks: I,
    visitor: &dyn ArrayVisitor<V>,
) -> anyhow::Result<()>
where
    I: IntoIterator<Item = (u64, (Vec<u64>, u64))>,
    V: unpack::Unpack,
{
    use crate::pdata::array::array_block_err;

    for (index, (mut path, blocknr)) in ablocks.into_iter() {
        path.push(blocknr);

        let b = engine
            .read(blocknr)
            .map_err(|_| array::io_err(&path, blocknr).index_context(index))?;

        let bt = checksum::metadata_block_type(b.get_data());
        if bt != checksum::BT::ARRAY {
            let e = array_block_err(
                &path,
                &format!("checksum failed for array block {}, {:?}", b.loc, bt),
            )
            .index_context(index);
            return Err(e.into());
        }

        let ablock = unpack_array_block::<V>(&path, b.get_data())?;
        visitor.visit(index, ablock).context(OutputError)?;
    }

    Ok(())
}

//------------------------------------------
