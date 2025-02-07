use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;

use crate::checksum;
use crate::io_engine::{IoEngine, ReadHandler, BLOCK_SIZE};
use crate::pdata::btree::{self, *};
use crate::pdata::btree_walker::*;
use crate::pdata::space_map::aggregator::*;
use crate::pdata::space_map::common::*;
use crate::pdata::space_map::metadata::*;
use crate::pdata::space_map::*;
use crate::pdata::unpack::*;

//------------------------------------------

enum SmType {
    DataSm,
    MetadataSm,
}

use SmType::*;

//------------------------------------------

fn inc_entries(sm: &ASpaceMap, entries: &[IndexEntry]) -> Result<()> {
    let mut sm = sm.lock().unwrap();
    for ie in entries {
        sm.inc(ie.blocknr, 1)?;
    }
    Ok(())
}

fn gather_data_index_entries(
    engine: Arc<dyn IoEngine + Send + Sync>,
    bitmap_root: u64,
    metadata_sm: ASpaceMap,
    ignore_non_fatal: bool,
) -> Result<Vec<IndexEntry>> {
    let entries_map = btree_to_map_with_sm::<IndexEntry>(
        &mut vec![0],
        engine,
        metadata_sm.clone(),
        ignore_non_fatal,
        bitmap_root,
    )?;

    let entries: Vec<IndexEntry> = entries_map.values().cloned().collect();
    inc_entries(&metadata_sm, &entries[0..])?;

    Ok(entries)
}

fn gather_metadata_index_entries(
    engine: Arc<dyn IoEngine + Send + Sync>,
    bitmap_root: u64,
    nr_blocks: u64,
    metadata_sm: ASpaceMap,
) -> Result<Vec<IndexEntry>> {
    let b = engine.read(bitmap_root)?;
    let entries = load_metadata_index(&b, nr_blocks)?.indexes;
    metadata_sm.lock().unwrap().inc(bitmap_root, 1)?;
    inc_entries(&metadata_sm, &entries[0..])?;

    Ok(entries)
}

/*
fn gather_index_entries(
    engine: Arc<dyn IoEngine + Send + Sync>,
    bitmap_root: u64,
    metadata_sm: ASpaceMap,
    ignore_non_fatal: bool,
    kind: SmType,
) -> Result<Vec<IndexEntry>> {
    match kind {
        DataSm => gather_data_index_entries(engine, bitmap_root, metadata_sm, ignore_non_fatal),
        MetadataSm => gather_metadata_index_entries(
            engine,
            bitmap_root,
            metadata_sm.get_nr_blocks()?,
            metadata_sm,
        ),
    }
}
*/

//------------------------------------------

struct IndexHandler<'a> {
    loc_to_block_index: HashMap<u64, u64>,
    aggregator: &'a Aggregator,
    batch: Vec<(u64, u32)>,
}

impl<'a> IndexHandler<'a> {
    fn new(entries: HashMap<u64, u64>, aggregator: &'a Aggregator) -> Self {
        Self {
            loc_to_block_index: entries,
            aggregator,
            batch: Vec::with_capacity(4096 * 4),
        }
    }

    fn push(&mut self, blocknr: u64, count: u32) {
        self.batch.push((blocknr, count))
    }

    fn flush_batch(&mut self) {
        self.aggregator.set_batch(&self.batch);
        self.batch.clear();
    }
}

impl<'a> ReadHandler for IndexHandler<'a> {
    fn handle(&mut self, loc: u64, data: std::io::Result<&[u8]>) {
        if let Some(block_index) = self.loc_to_block_index.get(&loc) {
            match data {
                Ok(data) => {
                    if checksum::metadata_block_type(data) != checksum::BT::BITMAP {
                        todo!();
                    }

                    let bitmap = unpack::<Bitmap>(data);
                    if bitmap.is_err() {
                        todo!();
                    }
                    let bitmap = bitmap.unwrap();

                    let mut blocknr = block_index * ENTRIES_PER_BITMAP as u64;
                    for e in bitmap.entries.iter() {
                        match e {
                            BitmapEntry::Small(count) => {
                                if *count > 0 {
                                    self.push(blocknr, *count as u32);
                                }
                            }
                            BitmapEntry::Overflow => {
                                // For overflow entries, we need to check the ref count tree
                                // We'll handle this in the next step
                            }
                        }
                        blocknr += 1;
                    }
                    self.flush_batch();
                }
                Err(_e) => {
                    todo!();
                }
            }
        } else {
            todo!();
        }
    }

    fn complete(&mut self) {}
}

pub fn read_space_map(
    engine: Arc<dyn IoEngine + Send + Sync>,
    root: SMRoot,
    ignore_non_fatal: bool,
    metadata_sm: ASpaceMap,
    kind: SmType,
) -> Result<Aggregator> {
    let nr_blocks = root.nr_blocks as usize;
    let aggregator = Aggregator::new(nr_blocks);

    // First, load the bitmap data
    let entries = {
        match kind {
            DataSm => gather_data_index_entries(
                engine.clone(),
                root.bitmap_root,
                metadata_sm,
                ignore_non_fatal,
            )?,
            MetadataSm => gather_metadata_index_entries(
                engine.clone(),
                root.bitmap_root,
                root.nr_blocks,
                metadata_sm,
            )?,
        }
    };
    eprintln!("nr index entries: {}", entries.len());

    let mut loc_to_index = HashMap::new();
    let mut blocks = Vec::with_capacity(entries.len());
    for (i, e) in entries.iter().enumerate() {
        blocks.push(e.blocknr);
        loc_to_index.insert(e.blocknr, i as u64);
    }

    let buffer_size_m = 16;
    let mut index_handler = IndexHandler::new(loc_to_index, &aggregator);
    let mut reader = engine.build_stream_reader(BLOCK_SIZE, buffer_size_m)?;

    // FIXME: is the cloned() expensive?
    reader.read_blocks(&mut blocks.iter().cloned(), &mut index_handler)?;

    // Now, handle the overflow entries in the ref count tree
    struct OverflowVisitor<'a> {
        aggregator: &'a Aggregator,
    }

    impl<'a> NodeVisitor<u32> for OverflowVisitor<'a> {
        fn visit(
            &self,
            _path: &[u64],
            _kr: &KeyRange,
            _h: &NodeHeader,
            keys: &[u64],
            values: &[u32],
        ) -> btree::Result<()> {
            for (&k, &v) in keys.iter().zip(values.iter()) {
                for _ in 0..v {
                    self.aggregator.inc_single(k);
                }
            }
            Ok(())
        }

        fn visit_again(&self, _path: &[u64], _b: u64) -> btree::Result<()> {
            Ok(())
        }

        fn end_walk(&self) -> btree::Result<()> {
            Ok(())
        }
    }

    let visitor = OverflowVisitor {
        aggregator: &aggregator,
    };
    let walker = BTreeWalker::new(engine, ignore_non_fatal);
    walker.walk(&mut vec![0], &visitor, root.ref_count_root)?;

    Ok(aggregator)
}

pub fn read_data_space_map(
    engine: Arc<dyn IoEngine + Send + Sync>,
    root: SMRoot,
    ignore_non_fatal: bool,
    metadata_sm: ASpaceMap,
) -> Result<Aggregator> {
    read_space_map(engine, root, ignore_non_fatal, metadata_sm, DataSm)
}

pub fn read_metadata_space_map(
    engine: Arc<dyn IoEngine + Send + Sync>,
    root: SMRoot,
    ignore_non_fatal: bool,
    metadata_sm: ASpaceMap,
) -> Result<Aggregator> {
    read_space_map(engine, root, ignore_non_fatal, metadata_sm, MetadataSm)
}

//------------------------------------------
