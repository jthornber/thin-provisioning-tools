use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;

use crate::checksum;
use crate::io_engine::buffer_pool::*;
use crate::io_engine::{IoEngine, ReadHandler, BLOCK_SIZE};
use crate::pdata::btree::{self, *};
use crate::pdata::btree_layer_walker::*;
use crate::pdata::btree_walker::*;
use crate::pdata::space_map::aggregator::*;
use crate::pdata::space_map::common::*;
use crate::pdata::space_map::metadata::*;
use crate::pdata::unpack::*;

//------------------------------------------

pub enum SmType {
    DataSm,
    MetadataSm,
}

use SmType::*;

//------------------------------------------

fn inc_entries(sm: &Aggregator, entries: &[IndexEntry]) -> Result<()> {
    for ie in entries {
        sm.inc_single(ie.blocknr);
    }
    Ok(())
}

fn gather_data_index_entries(
    engine: Arc<dyn IoEngine + Send + Sync>,
    bitmap_root: u64,
    metadata_sm: &Aggregator,
    ignore_non_fatal: bool,
) -> Result<Vec<IndexEntry>> {
    let entries_map = btree_to_map_with_aggregator::<IndexEntry>(
        engine.as_ref(),
        metadata_sm,
        bitmap_root,
        ignore_non_fatal,
    )?;

    let entries: Vec<IndexEntry> = entries_map.values().cloned().collect();
    inc_entries(metadata_sm, &entries[0..])?;

    Ok(entries)
}

fn gather_metadata_index_entries(
    engine: Arc<dyn IoEngine + Send + Sync>,
    bitmap_root: u64,
    nr_blocks: u64,
    metadata_sm: &Aggregator,
) -> Result<Vec<IndexEntry>> {
    let b = engine.read(bitmap_root)?;
    let entries = load_metadata_index(&b, nr_blocks)?.indexes;
    metadata_sm.inc_single(bitmap_root);
    inc_entries(metadata_sm, &entries[0..])?;

    Ok(entries)
}

/// Loads space map index entries from disk.
///
/// Takes the full SMRoot rather than the root block number for extensibility,
/// while the metadata_sm for tracking reference counts currently matches
/// SMRoot.nr_blocks in size. This property may change in the future.
fn gather_index_entries(
    engine: Arc<dyn IoEngine + Send + Sync>,
    root: &SMRoot,
    metadata_sm: &Aggregator,
    ignore_non_fatal: bool,
    kind: SmType,
) -> Result<Vec<IndexEntry>> {
    match kind {
        DataSm => {
            gather_data_index_entries(engine, root.bitmap_root, metadata_sm, ignore_non_fatal)
        }
        MetadataSm => {
            gather_metadata_index_entries(engine, root.bitmap_root, root.nr_blocks, metadata_sm)
        }
    }
}

//------------------------------------------

struct IndexHandler<'a> {
    loc_to_block_index: HashMap<u64, u64>,
    aggregator: &'a Aggregator,
    batch: Vec<(u64, u32)>,
    error: Option<anyhow::Error>,
}

impl<'a> IndexHandler<'a> {
    fn new(entries: HashMap<u64, u64>, aggregator: &'a Aggregator) -> Self {
        Self {
            loc_to_block_index: entries,
            aggregator,
            batch: Vec::with_capacity(4096 * 4),
            error: None,
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
        use anyhow::anyhow;

        if let Some(block_index) = self.loc_to_block_index.get(&loc) {
            match data {
                Ok(data) => {
                    match checksum::metadata_block_type(data) {
                        checksum::BT::BITMAP => {}
                        checksum::BT::UNKNOWN => {
                            self.error.get_or_insert_with(|| {
                                anyhow!(
                                    "checksum error at block {}, index entry {}",
                                    loc,
                                    block_index
                                )
                            });
                            return;
                        }
                        _ => {
                            self.error.get_or_insert_with(|| {
                                anyhow!(
                                    "block {} of index entry {} is not a bitmap",
                                    loc,
                                    block_index
                                )
                            });
                            return;
                        }
                    }

                    let bitmap = unpack::<Bitmap>(data);
                    if bitmap.is_err() {
                        self.error.get_or_insert_with(|| {
                            anyhow!(
                                "incomplete data at block {}, index entry {}",
                                loc,
                                block_index
                            )
                        });
                        return;
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
                Err(e) => {
                    self.error.get_or_insert_with(|| {
                        anyhow!("{} at block {}, index entry {}", e, loc, block_index)
                    });
                }
            }
        } else {
            self.error
                .get_or_insert_with(|| anyhow!("unexpected bitmap {} without index entry", loc));
        }
    }

    fn complete(&mut self) {}
}

pub fn read_space_map(
    engine: Arc<dyn IoEngine + Send + Sync>,
    root: SMRoot,
    ignore_non_fatal: bool,
    metadata_sm: &Aggregator,
    kind: SmType,
) -> Result<(Aggregator, Vec<IndexEntry>)> {
    let nr_blocks = root.nr_blocks as usize;
    let aggregator = Arc::new(Aggregator::new(nr_blocks));

    // First, load the bitmap data
    let entries = gather_index_entries(engine.clone(), &root, metadata_sm, ignore_non_fatal, kind)?;

    let mut loc_to_index = HashMap::new();
    for (i, e) in entries.iter().enumerate() {
        loc_to_index.insert(e.blocknr, i as u64);
    }

    // Stick with small blocks since they're likely to be spread out.
    let io_block_size = BLOCK_SIZE;
    let buffer_size = 16 * 1024 * 1024;
    let nr_io_blocks = buffer_size / io_block_size;
    let mut pool = BufferPool::new(nr_io_blocks, io_block_size);

    let mut index_handler = IndexHandler::new(loc_to_index, &aggregator);

    engine.read_blocks(
        &mut pool,
        &mut entries.iter().map(|e| e.blocknr),
        &mut index_handler,
    )?;

    if let Some(e) = index_handler.error {
        return Err(e);
    }

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
            let batch = keys
                .iter()
                .copied()
                .zip(values.iter().copied())
                .collect::<Vec<(u64, u32)>>();
            self.aggregator.set_batch(&batch);
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
    read_nodes(
        engine.as_ref(),
        &visitor,
        metadata_sm,
        root.ref_count_root,
        ignore_non_fatal,
    )?;

    Ok((Arc::into_inner(aggregator).unwrap(), entries))
}

pub fn read_data_space_map(
    engine: Arc<dyn IoEngine + Send + Sync>,
    root: SMRoot,
    ignore_non_fatal: bool,
    metadata_sm: &Aggregator,
) -> Result<(Aggregator, Vec<IndexEntry>)> {
    read_space_map(engine, root, ignore_non_fatal, metadata_sm, DataSm)
}

pub fn read_metadata_space_map(
    engine: Arc<dyn IoEngine + Send + Sync>,
    root: SMRoot,
    ignore_non_fatal: bool,
    metadata_sm: &Aggregator,
) -> Result<(Aggregator, Vec<IndexEntry>)> {
    read_space_map(engine, root, ignore_non_fatal, metadata_sm, MetadataSm)
}

//------------------------------------------
