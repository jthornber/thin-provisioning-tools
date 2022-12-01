use anyhow::{anyhow, Result};
use rand::prelude::SliceRandom;
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;

use crate::checksum;
use crate::commands::engine::*;
use crate::io_engine::IoEngine;
use crate::pdata::btree_walker::btree_to_map;
use crate::pdata::space_map::common::*;
use crate::pdata::space_map::metadata::*;
use crate::pdata::unpack::{unpack, Pack};
use crate::thin::superblock::*;

//------------------------------------------

fn find_blocks_of_rc(
    engine: Arc<dyn IoEngine + Send + Sync>,
    sm_root: SMRoot,
    ref_count: u32,
) -> Result<Vec<u64>> {
    let mut found = Vec::<u64>::new();
    if ref_count < 3 {
        let b = engine.read(sm_root.bitmap_root)?;
        let entries = check_and_unpack_metadata_index(&b)?.indexes;
        let bitmaps: Vec<u64> = entries.iter().map(|ie| ie.blocknr).collect();
        let nr_bitmaps = bitmaps.len();

        let rblocks = engine.read_many(&bitmaps)?;
        let mut blocknr = 0;
        for (idx, rb) in rblocks.into_iter().enumerate() {
            if let Ok(b) = rb {
                let bitmap = unpack::<Bitmap>(b.get_data())?;
                let len = if idx == nr_bitmaps - 1 {
                    (engine.get_nr_blocks() % ENTRIES_PER_BITMAP as u64) as usize
                } else {
                    ENTRIES_PER_BITMAP
                };
                for e in &bitmap.entries[..len] {
                    if BitmapEntry::Small(ref_count as u8) == *e {
                        found.push(blocknr);
                    }
                    blocknr += 1;
                }
            } else {
                return Err(anyhow!("Cannot read bitmap: {}", rb.unwrap_err()));
            }
        }
    } else {
        let mut path = Vec::new();
        let high_rc = btree_to_map::<u32>(&mut path, engine, false, sm_root.ref_count_root)?;
        for (k, v) in high_rc.iter() {
            if *v == ref_count {
                found.push(*k);
            }
        }
    }

    Ok(found)
}

//------------------------------------------

fn adjust_bitmap_entries(
    engine: &dyn IoEngine,
    sm_root: SMRoot,
    blocks: &[u64],
    ref_count: u32,
) -> Result<()> {
    let entry = if ref_count < 3 {
        BitmapEntry::Small(ref_count as u8)
    } else {
        BitmapEntry::Overflow
    };

    let index_block = engine.read(sm_root.bitmap_root)?;
    let entries = check_and_unpack_metadata_index(&index_block)?.indexes;

    let bi = blocks_to_bitmaps(blocks);
    let bitmaps: Vec<u64> = bi.iter().map(|i| entries[*i].blocknr).collect();

    let rblocks = engine.read_many(&bitmaps)?;
    let mut wblocks = Vec::new();
    let mut blocks_iter = blocks.iter();
    let mut block = blocks_iter.next();
    for (rb, idx) in rblocks.into_iter().zip(bi) {
        if let Ok(b) = rb {
            let mut bitmap = unpack::<Bitmap>(b.get_data())?;
            let high = (ENTRIES_PER_BITMAP * (idx + 1)) as u64;

            while block.is_some() && *block.unwrap() < high {
                let i = (*block.unwrap() % ENTRIES_PER_BITMAP as u64) as usize;
                bitmap.entries[i] = entry;
                block = blocks_iter.next();
            }

            let mut out = Cursor::new(b.get_data());
            bitmap.pack(&mut out)?;
            checksum::write_checksum(b.get_data(), checksum::BT::BITMAP)?;

            wblocks.push(b);
        } else {
            return Err(anyhow!("Errors in reading bitmaps"));
        }
    }

    let results = engine.write_many(&wblocks)?;
    for ret in results {
        if ret.is_err() {
            return Err(anyhow!("Errors in writing bitmaps"));
        }
    }

    Ok(())
}

fn create_metadata_leaks(
    engine: Arc<dyn IoEngine + Send + Sync>,
    sm_root: SMRoot,
    nr_leaks: usize,
    expected_rc: u32,
    actual_rc: u32,
) -> Result<()> {
    let mut blocks = find_blocks_of_rc(engine.clone(), sm_root.clone(), expected_rc)?;
    if blocks.len() < nr_leaks {
        return Err(anyhow!(
            "no sufficient blocks with ref counts {}",
            expected_rc
        ));
    }

    blocks.shuffle(&mut rand::thread_rng());
    blocks.truncate(nr_leaks);
    blocks.sort_unstable();

    #[allow(clippy::if_same_then_else)]
    if expected_rc > 2 {
        if actual_rc > 2 {
            // adjust mapped rc in ref count btree
            todo!();
        } else {
            // 1. adjust_bitmap_entries(engine, sm_root, &blocks, actual_rc);
            // 2. remove mappings from the ref count btree
            // 3. update superblock
            todo!();
        }
    } else if actual_rc > 2 {
        // 1. adjust_bitmap_entries(engine, sm_root, &blocks, actual_rc);
        // 2. insert mappings to the ref count btree
        // 3. update superblock
        todo!();
    } else {
        adjust_bitmap_entries(engine.as_ref(), sm_root, &blocks, actual_rc)
    }
}

//------------------------------------------

pub enum DamageOp {
    CreateMetadataLeaks {
        nr_blocks: usize,
        expected_rc: u32,
        actual_rc: u32,
    },
}

pub struct ThinDamageOpts<'a> {
    pub engine_opts: EngineOptions,
    pub op: DamageOp,
    pub output: &'a Path,
}

pub fn damage_metadata(opts: ThinDamageOpts) -> Result<()> {
    let engine = EngineBuilder::new(opts.output, &opts.engine_opts)
        .write(true)
        .build()?;
    let sb = read_superblock(engine.as_ref(), SUPERBLOCK_LOCATION)?;
    let sm_root = unpack::<SMRoot>(&sb.metadata_sm_root)?;

    match opts.op {
        DamageOp::CreateMetadataLeaks {
            nr_blocks,
            expected_rc,
            actual_rc,
        } => create_metadata_leaks(engine, sm_root, nr_blocks, expected_rc, actual_rc),
    }
}

//------------------------------------------
