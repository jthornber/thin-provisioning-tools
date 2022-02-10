use anyhow::{anyhow, Result};
use byteorder::WriteBytesExt;
use std::io::Cursor;

use crate::checksum;
use crate::io_engine::*;
use crate::math::*;
use crate::pdata::array::*;
use crate::pdata::btree_builder::*;
use crate::pdata::unpack::*;
use crate::write_batcher::*;

//------------------------------------------

pub struct ArrayBlockBuilder<V: Unpack + Pack> {
    array_io: ArrayIO<V>,
    nr_entries: u64, // size of the array
    entries_per_block: usize,
    array_blocks: Vec<u64>, // emitted array blocks
    values: Vec<V>,         // internal buffer
}

pub struct ArrayBuilder<V: Unpack + Pack> {
    block_builder: ArrayBlockBuilder<V>,
}

struct ArrayIO<V: Unpack + Pack> {
    dummy: std::marker::PhantomData<V>,
}

struct WriteResult {
    loc: u64,
}

//------------------------------------------

fn calc_max_entries<V: Unpack>() -> usize {
    (BLOCK_SIZE - ArrayBlockHeader::disk_size() as usize) / V::disk_size() as usize
}

//------------------------------------------

impl<V: Unpack + Pack + Clone + Default> ArrayBlockBuilder<V> {
    pub fn new(nr_entries: u64) -> ArrayBlockBuilder<V> {
        let entries_per_block = calc_max_entries::<V>();
        let nr_blocks = div_up(nr_entries, entries_per_block as u64) as usize;
        let next_cap = std::cmp::min(nr_entries, entries_per_block as u64) as usize;

        ArrayBlockBuilder {
            array_io: ArrayIO::new(),
            nr_entries,
            entries_per_block,
            array_blocks: Vec::with_capacity(nr_blocks),
            values: Vec::<V>::with_capacity(next_cap),
        }
    }

    pub fn push_value(&mut self, w: &mut WriteBatcher, index: u64, v: V) -> Result<()> {
        let bi = index / self.entries_per_block as u64;
        let i = (index % self.entries_per_block as u64) as usize;

        if index >= self.nr_entries {
            return Err(anyhow!("array index out of bounds"));
        }

        while (self.array_blocks.len() as u64) < bi {
            self.emit_block(w)?;
        }

        if bi < self.array_blocks.len() as u64 || i < self.values.len() {
            return Err(anyhow!("unordered array index"));
        }

        if i > self.values.len() {
            self.values.resize_with(i, Default::default);
        }
        self.values.push(v);

        Ok(())
    }

    pub fn complete(mut self, w: &mut WriteBatcher) -> Result<Vec<u64>> {
        // Emit all the remaining queued values
        let nr_blocks = self.array_blocks.capacity();
        while self.array_blocks.len() < nr_blocks {
            self.emit_block(w)?;
        }

        Ok(self.array_blocks)
    }

    /// Emit a fully utilized array block
    fn emit_block(&mut self, w: &mut WriteBatcher) -> Result<()> {
        let nr_blocks = self.array_blocks.capacity();
        let cur_bi = self.array_blocks.len();
        let next_cap;
        if cur_bi < nr_blocks - 1 {
            let next_begin = (cur_bi as u64 + 1) * self.entries_per_block as u64;
            next_cap =
                std::cmp::min(self.nr_entries - next_begin, self.entries_per_block as u64) as usize;
        } else {
            next_cap = 0;
        }

        let mut values = Vec::<V>::with_capacity(next_cap);
        std::mem::swap(&mut self.values, &mut values);

        values.resize_with(values.capacity(), Default::default);
        let wresult = self.array_io.write(w, values)?;
        self.array_blocks.push(wresult.loc);

        Ok(())
    }
}

//------------------------------------------

impl<V: Unpack + Pack + Clone + Default> ArrayBuilder<V> {
    pub fn new(nr_entries: u64) -> ArrayBuilder<V> {
        ArrayBuilder {
            block_builder: ArrayBlockBuilder::<V>::new(nr_entries),
        }
    }

    pub fn push_value(&mut self, w: &mut WriteBatcher, index: u64, v: V) -> Result<()> {
        self.block_builder.push_value(w, index, v)
    }

    pub fn complete(self, w: &mut WriteBatcher) -> Result<u64> {
        let blocks = self.block_builder.complete(w)?;
        let mut index_builder = BTreeBuilder::<u64>::new(Box::new(NoopRC {}));

        for (i, b) in blocks.iter().enumerate() {
            index_builder.push_value(w, i as u64, *b)?;
        }
        index_builder.complete(w)
    }
}

//------------------------------------------

impl<V: Unpack + Pack> ArrayIO<V> {
    pub fn new() -> ArrayIO<V> {
        ArrayIO {
            dummy: std::marker::PhantomData,
        }
    }

    fn write(&self, w: &mut WriteBatcher, values: Vec<V>) -> Result<WriteResult> {
        let header = ArrayBlockHeader {
            csum: 0,
            max_entries: calc_max_entries::<V>() as u32,
            nr_entries: values.len() as u32,
            value_size: V::disk_size(),
            blocknr: 0,
        };

        let ablock = ArrayBlock { header, values };

        write_array_block(w, ablock)
    }
}

fn write_array_block<V: Unpack + Pack>(
    w: &mut WriteBatcher,
    mut ablock: ArrayBlock<V>,
) -> Result<WriteResult> {
    let b = w.alloc()?;
    ablock.set_block(b.loc);

    let mut cursor = Cursor::new(b.get_data());
    pack_array_block(&ablock, &mut cursor)?;
    let loc = b.loc;
    w.write(b, checksum::BT::ARRAY)?;

    Ok(WriteResult { loc })
}

pub fn pack_array_block<W: WriteBytesExt, V: Pack + Unpack>(
    ablock: &ArrayBlock<V>,
    w: &mut W,
) -> Result<()> {
    ablock.header.pack(w)?;
    for v in ablock.values.iter() {
        v.pack(w)?;
    }
    Ok(())
}

//------------------------------------------
