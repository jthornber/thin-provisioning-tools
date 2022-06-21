use anyhow::Result;
use roaring::RoaringBitmap;
use std::sync::Arc;

use crate::io_engine::*;
use crate::pdata::space_map::common::*;
use crate::pdata::space_map::metadata::*;
use crate::pdata::unpack::*;

//----------------------------------

struct IndexInfo {
    key: u64,
    loc: u64,
}

pub fn allocated_blocks(
    engine: Arc<dyn IoEngine + Send + Sync>,
    sm_root: u64,
) -> Result<RoaringBitmap> {
    // Walk index tree to find where the bitmaps are.
    let b = engine.read(sm_root)?;
    let (_, indexes) = MetadataIndex::unpack(b.get_data())?;

    let mut infos = Vec::new();
    for (key, entry) in indexes.indexes.iter().enumerate() {
        infos.push(IndexInfo {
            key: key as u64,
            loc: entry.blocknr,
        });
    }

    // Read bitmaps in sequence
    infos.sort_by(|lhs, rhs| lhs.loc.partial_cmp(&rhs.loc).unwrap());

    let mut bits = RoaringBitmap::new();
    for info in &infos {
        let b = engine.read(info.loc)?;
        let base = info.key * ENTRIES_PER_BITMAP as u64;
        let (_, bm) = Bitmap::unpack(b.get_data())?;

        for i in 0..bm.entries.len() {
            if let BitmapEntry::Small(0) = bm.entries[i] {
                // nothing
            } else {
                bits.insert((base + i as u64) as u32);
            }
        }
    }

    Ok(bits)
}

//----------------------------------
