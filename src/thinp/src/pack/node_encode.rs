use std::{io, io::Write};
use thiserror::Error;

use nom::{bytes::complete::*, number::complete::*, IResult};

use crate::pack::vm::*;

//-------------------------------------------

#[derive(Error, Debug)]
pub enum PackError {
    #[error("Couldn't parse binary data")]
    ParseError,

    #[error("Write error")]
    WriteError { source: std::io::Error },
}

pub type PResult<T> = Result<T, PackError>;

fn nom_to_pr<T>(r: IResult<&[u8], T>) -> PResult<(&[u8], T)> {
    r.map_err(|_source| PackError::ParseError)
}

fn io_to_pr<T>(r: io::Result<T>) -> PResult<T> {
    r.map_err(|source| PackError::WriteError { source })
}

//-------------------------------------------

fn run64(i: &[u8], count: usize) -> IResult<&[u8], Vec<u64>> {
    let (i, ns) = nom::multi::many_m_n(count, count, le_u64)(i)?;
    Ok((i, ns))
}

struct NodeSummary {
    is_leaf: bool,
    max_entries: usize,
    value_size: usize,
}

fn summarise_node(data: &[u8]) -> IResult<&[u8], NodeSummary> {
    let (i, _csum) = le_u32(data)?;
    let (i, flags) = le_u32(i)?;
    let (i, _blocknr) = le_u64(i)?;
    let (i, _nr_entries) = le_u32(i)?;
    let (i, max_entries) = le_u32(i)?;
    let (i, value_size) = le_u32(i)?;
    let (i, _padding) = le_u32(i)?;
    Ok((
        i,
        NodeSummary {
            is_leaf: flags == 2,
            max_entries: max_entries as usize,
            value_size: value_size as usize,
        },
    ))
}

pub fn pack_btree_node<W: Write>(w: &mut W, data: &[u8]) -> PResult<()> {
    let (_, info) = nom_to_pr(summarise_node(data))?;
    let (i, hdr) = nom_to_pr(take(32usize)(data))?;
    let (i, keys) = nom_to_pr(run64(i, info.max_entries))?;

    if info.is_leaf {
        if info.value_size == std::mem::size_of::<u64>() {
            let (tail, values) = nom_to_pr(run64(i, info.max_entries))?;

            io_to_pr(pack_literal(w, hdr))?;
            io_to_pr(pack_u64s(w, &keys))?;
            io_to_pr(pack_shifted_u64s(w, &values))?;
            if !tail.is_empty() {
                io_to_pr(pack_literal(w, tail))?;
            }

            Ok(())
        } else {
            // We don't bother packing the values if they aren't u64
            let tail = i;

            io_to_pr(pack_literal(w, hdr))?;
            io_to_pr(pack_u64s(w, &keys))?;
            io_to_pr(pack_literal(w, tail))?;

            Ok(())
        }
    } else {
        // Internal node, values are also u64s
        let (tail, values) = nom_to_pr(run64(i, info.max_entries))?;

        io_to_pr(pack_literal(w, hdr))?;
        io_to_pr(pack_u64s(w, &keys))?;
        io_to_pr(pack_u64s(w, &values))?;
        if !tail.is_empty() {
            io_to_pr(pack_literal(w, tail))?;
        }

        Ok(())
    }
}

pub fn pack_superblock<W: Write>(w: &mut W, bytes: &[u8]) -> PResult<()> {
    io_to_pr(pack_literal(w, bytes))
}

pub fn pack_bitmap<W: Write>(w: &mut W, bytes: &[u8]) -> PResult<()> {
    io_to_pr(pack_literal(w, bytes))
}

pub fn pack_index<W: Write>(w: &mut W, bytes: &[u8]) -> PResult<()> {
    io_to_pr(pack_literal(w, bytes))
}

pub fn pack_array<W: Write>(w: &mut W, bytes: &[u8]) -> PResult<()> {
    io_to_pr(pack_literal(w, bytes))
}

//-------------------------------------
