use anyhow::{anyhow, Result};
use std::io::Cursor;
use std::sync::Arc;

use crate::checksum;
use crate::io_engine::IoEngine;
use crate::pdata::btree::{self, *};
use crate::pdata::btree_walker::*;
use crate::pdata::space_map::common::*;
use crate::pdata::space_map::metadata::*;
use crate::pdata::space_map::*;
use crate::pdata::unpack::*;
use crate::report::Report;

//------------------------------------------

pub struct BitmapLeak {
    blocknr: u64, // blocknr for the first entry in the bitmap
    loc: u64,     // location of the bitmap
}

//------------------------------------------

struct OverflowChecker<'a> {
    kind: &'a str,
    sm: &'a dyn SpaceMap,
}

impl<'a> OverflowChecker<'a> {
    fn new(kind: &'a str, sm: &'a dyn SpaceMap) -> OverflowChecker<'a> {
        OverflowChecker { kind, sm }
    }
}

impl<'a> NodeVisitor<u32> for OverflowChecker<'a> {
    fn visit(
        &self,
        _path: &[u64],
        _kr: &KeyRange,
        _h: &NodeHeader,
        keys: &[u64],
        values: &[u32],
    ) -> btree::Result<()> {
        for n in 0..keys.len() {
            let k = keys[n];
            let v = values[n];
            let expected = self.sm.get(k).unwrap();
            if expected != v {
                return Err(value_err(format!(
                    "Bad reference count for {} block {}.  Expected {}, but space map contains {}.",
                    self.kind, k, expected, v
                )));
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

//------------------------------------------

fn inc_entries(sm: &ASpaceMap, entries: &[IndexEntry]) -> Result<()> {
    let mut sm = sm.lock().unwrap();
    for ie in entries {
        // FIXME: checksumming bitmaps?
        sm.inc(ie.blocknr, 1)?;
    }
    Ok(())
}

// Compare the reference counts in bitmaps against the expected values
//
// `sm` - The in-core space map of expected reference counts
fn check_low_ref_counts(
    engine: Arc<dyn IoEngine + Send + Sync>,
    report: Arc<Report>,
    kind: &str,
    entries: Vec<IndexEntry>,
    sm: ASpaceMap,
) -> Result<Vec<BitmapLeak>> {
    // gathering bitmap blocknr
    let mut blocks = Vec::with_capacity(entries.len());
    for i in &entries {
        blocks.push(i.blocknr);
    }

    // read bitmap blocks
    // FIXME: we should do this in batches
    let blocks = engine.read_many(&blocks)?;

    // compare ref-counts in bitmap blocks
    let mut leaks = 0;
    let mut failed = false;
    let mut blocknr = 0;
    let mut bitmap_leaks = Vec::new();
    let sm = sm.lock().unwrap();
    let nr_blocks = sm.get_nr_blocks()?;
    for b in blocks.iter().take(entries.len()) {
        match b {
            Err(_e) => {
                return Err(anyhow!("Unable to read bitmap block"));
            }
            Ok(b) => {
                if checksum::metadata_block_type(b.get_data()) != checksum::BT::BITMAP {
                    report.fatal(&format!(
                        "Index entry points to block ({}) that isn't a bitmap",
                        b.loc
                    ));
                    failed = true;

                    // FIXME: revert the ref-count at b.loc?
                }

                let bitmap = unpack::<Bitmap>(b.get_data())?;
                let first_blocknr = blocknr;
                let mut contains_leak = false;
                for e in bitmap.entries.iter() {
                    if blocknr >= nr_blocks {
                        break;
                    }

                    match e {
                        BitmapEntry::Small(actual) => {
                            let expected = sm.get(blocknr)?;
                            if *actual == 1 && expected == 0 {
                                leaks += 1;
                                contains_leak = true;
                            } else if *actual != expected as u8 {
                                report.fatal(&format!("Bad reference count for {} block {}.  Expected {}, but space map contains {}.",
                                          kind, blocknr, expected, actual));
                                failed = true;
                            }
                        }
                        BitmapEntry::Overflow => {
                            let expected = sm.get(blocknr)?;
                            if expected < 3 {
                                report.fatal(&format!("Bad reference count for {} block {}.  Expected {}, but space map says it's >= 3.",
                                                  kind, blocknr, expected));
                                failed = true;
                            }
                        }
                    }
                    blocknr += 1;
                }
                if contains_leak {
                    bitmap_leaks.push(BitmapLeak {
                        blocknr: first_blocknr,
                        loc: b.loc,
                    });
                }
            }
        }
    }

    if leaks > 0 {
        report.non_fatal(&format!("{} {} blocks have leaked.", leaks, kind));
    }

    if failed {
        Err(anyhow!("Fatal errors in {} space map", kind))
    } else {
        Ok(bitmap_leaks)
    }
}

fn gather_disk_index_entries(
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
    metadata_sm: ASpaceMap,
) -> Result<Vec<IndexEntry>> {
    let b = engine.read(bitmap_root)?;
    let entries = check_and_unpack_metadata_index(&b)?.indexes;
    metadata_sm.lock().unwrap().inc(bitmap_root, 1)?;
    inc_entries(&metadata_sm, &entries[0..])?;

    Ok(entries)
}

//------------------------------------------

// This checks the space map and returns any leak blocks for auto-repair to process.
//
// `disk_sm` - The in-core space map of expected data block ref-counts
// `metadata_sm` - The in-core space for storing ref-counts of verified blocks
pub fn check_disk_space_map(
    engine: Arc<dyn IoEngine + Send + Sync>,
    report: Arc<Report>,
    root: SMRoot,
    disk_sm: ASpaceMap,
    metadata_sm: ASpaceMap,
    ignore_non_fatal: bool,
) -> Result<Vec<BitmapLeak>> {
    let entries = gather_disk_index_entries(
        engine.clone(),
        root.bitmap_root,
        metadata_sm.clone(),
        ignore_non_fatal,
    )?;

    // check overflow ref-counts
    {
        let sm = disk_sm.lock().unwrap();
        let v = OverflowChecker::new("data", &*sm);
        let w = BTreeWalker::new_with_sm(engine.clone(), metadata_sm.clone(), ignore_non_fatal)?;
        w.walk(&mut vec![0], &v, root.ref_count_root)?;
    }

    // check low ref-counts in bitmaps
    check_low_ref_counts(engine, report, "data", entries, disk_sm)
}

// This checks the space map and returns any leak blocks for auto-repair to process.
//
// `metadata_sm`: The in-core space map of expected metadata block ref-counts
pub fn check_metadata_space_map(
    engine: Arc<dyn IoEngine + Send + Sync>,
    report: Arc<Report>,
    root: SMRoot,
    metadata_sm: ASpaceMap,
    ignore_non_fatal: bool,
) -> Result<Vec<BitmapLeak>> {
    count_btree_blocks::<u32>(
        engine.clone(),
        &mut vec![0],
        root.ref_count_root,
        metadata_sm.clone(),
        false,
    )?;
    let entries =
        gather_metadata_index_entries(engine.clone(), root.bitmap_root, metadata_sm.clone())?;

    // check overflow ref-counts
    {
        let sm = metadata_sm.lock().unwrap();
        let v = OverflowChecker::new("metadata", &*sm);
        let w = BTreeWalker::new(engine.clone(), ignore_non_fatal);
        w.walk(&mut vec![0], &v, root.ref_count_root)?;
    }

    // check low ref-counts in bitmaps
    check_low_ref_counts(engine, report, "metadata", entries, metadata_sm)
}

// This assumes the only errors in the space map are leaks.  Entries should just be
// those that contain leaks.
pub fn repair_space_map(
    engine: Arc<dyn IoEngine + Send + Sync>,
    entries: Vec<BitmapLeak>,
    sm: ASpaceMap,
) -> Result<()> {
    let sm = sm.lock().unwrap();

    let mut blocks = Vec::with_capacity(entries.len());
    for i in &entries {
        blocks.push(i.loc);
    }

    // FIXME: we should do this in batches
    let rblocks = engine.read_many(&blocks[0..])?;
    let mut write_blocks = Vec::new();

    for (i, rb) in rblocks.into_iter().enumerate() {
        if let Ok(b) = rb {
            let be = &entries[i];
            let mut blocknr = be.blocknr;
            let mut bitmap = unpack::<Bitmap>(b.get_data())?;
            for e in bitmap.entries.iter_mut() {
                if blocknr >= sm.get_nr_blocks()? {
                    break;
                }

                if let BitmapEntry::Small(actual) = e {
                    let expected = sm.get(blocknr)?;
                    if *actual == 1 && expected == 0 {
                        *e = BitmapEntry::Small(0);
                    }
                }

                blocknr += 1;
            }

            let mut out = Cursor::new(b.get_data());
            bitmap.pack(&mut out)?;
            checksum::write_checksum(b.get_data(), checksum::BT::BITMAP)?;

            write_blocks.push(b);
        } else {
            return Err(anyhow!("Unable to reread bitmap blocks for repair"));
        }
    }

    let results = engine.write_many(&write_blocks[0..])?;
    for ret in results {
        if ret.is_err() {
            return Err(anyhow!("Unable to repair space map: {:?}", ret));
        }
    }
    Ok(())
}

//------------------------------------------
