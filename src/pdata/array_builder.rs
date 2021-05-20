use anyhow::Result;
use byteorder::WriteBytesExt;
use std::collections::VecDeque;
use std::io::Cursor;

use crate::checksum;
use crate::io_engine::*;
use crate::pdata::array::*;
use crate::pdata::btree_builder::*;
use crate::pdata::unpack::*;
use crate::write_batcher::*;

//------------------------------------------

pub struct ArrayBlockBuilder<V: Unpack + Pack> {
    array_io: ArrayIO<V>,
    max_entries_per_block: usize,
    values: VecDeque<(u64, V)>,
    array_blocks: Vec<u64>,
    nr_entries: u64,
    nr_emitted: u64,
    nr_queued: u64,
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
        ArrayBlockBuilder {
            array_io: ArrayIO::new(),
            max_entries_per_block: calc_max_entries::<V>(),
            values: VecDeque::new(),
            array_blocks: Vec::new(),
            nr_entries,
            nr_emitted: 0,
            nr_queued: 0,
        }
    }

    pub fn push_value(&mut self, w: &mut WriteBatcher, index: u64, v: V) -> Result<()> {
        assert!(index >= self.nr_emitted + self.nr_queued);
        assert!(index < self.nr_entries);

        self.values.push_back((index, v));
        self.nr_queued = index - self.nr_emitted + 1;

        if self.nr_queued > self.max_entries_per_block as u64 {
            self.emit_blocks(w)?;
        }

        Ok(())
    }

    pub fn complete(mut self, w: &mut WriteBatcher) -> Result<Vec<u64>> {
        if self.nr_emitted + self.nr_queued < self.nr_entries {
            // FIXME: flushing with a default values looks confusing
            self.push_value(w, self.nr_entries - 1, Default::default())?;
        }
        self.emit_all(w)?;
        Ok(self.array_blocks)
    }

    /// Emit all the remaining queued values
    fn emit_all(&mut self, w: &mut WriteBatcher) -> Result<()> {
        match self.nr_queued {
            0 => {
                // There's nothing to emit
                Ok(())
            }
            n if n <= self.max_entries_per_block as u64 => self.emit_values(w),
            _ => {
                panic!(
                    "There shouldn't be more than {} queued values",
                    self.max_entries_per_block
                );
            }
        }
    }

    /// Emit one or more fully utilized array blocks
    fn emit_blocks(&mut self, w: &mut WriteBatcher) -> Result<()> {
        while self.nr_queued > self.max_entries_per_block as u64 {
            self.emit_values(w)?;
        }
        Ok(())
    }

    /// Emit an array block with the queued values
    fn emit_values(&mut self, w: &mut WriteBatcher) -> Result<()> {
        let mut values = Vec::<V>::with_capacity(self.max_entries_per_block);
        let mut nr_free = self.max_entries_per_block;

        while !self.values.is_empty() && nr_free > 0 {
            let len = self.values.front().unwrap().0 - self.nr_emitted + 1;
            if len <= nr_free as u64 {
                let (_, v) = self.values.pop_front().unwrap();
                if len > 1 {
                    values.resize_with(values.len() + len as usize - 1, Default::default);
                }
                values.push(v);
                nr_free -= len as usize;
                self.nr_emitted += len;
                self.nr_queued -= len;
            } else {
                values.resize_with(values.len() + nr_free as usize, Default::default);
                self.nr_emitted += nr_free as u64;
                self.nr_queued -= nr_free as u64;
                nr_free = 0;
            }
        }

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
        let mut index_builder = Builder::<u64>::new(Box::new(NoopRC {}));

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

fn pack_array_block<W: WriteBytesExt, V: Pack + Unpack>(
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
