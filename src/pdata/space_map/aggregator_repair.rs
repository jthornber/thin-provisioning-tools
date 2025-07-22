use anyhow::Result;
use std::sync::Arc;

use crate::checksum;
use crate::io_engine::IoEngine;
use crate::pdata::space_map::aggregator::*;
use crate::pdata::space_map::base::RefCount;
use crate::pdata::space_map::common::*;
use crate::pdata::unpack::*;

//------------------------------------------

pub use crate::pdata::space_map::checker::BitmapLeak;

//------------------------------------------

pub fn repair_space_map(
    engine: Arc<dyn IoEngine + Send + Sync>,
    entries: Vec<BitmapLeak>,
    sm: &Aggregator,
) -> Result<()> {
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
                if blocknr >= sm.get_nr_blocks() as u64 {
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

            let mut out = std::io::Cursor::new(b.get_data());
            bitmap.pack(&mut out)?;
            checksum::write_checksum(b.get_data(), checksum::BT::BITMAP)?;

            write_blocks.push(b);
        } else {
            return Err(anyhow::anyhow!("Unable to reread bitmap blocks for repair"));
        }
    }

    let results = engine.write_many(&write_blocks[0..])?;
    for ret in results {
        if ret.is_err() {
            return Err(anyhow::anyhow!("Unable to repair space map: {:?}", ret));
        }
    }
    Ok(())
}

//------------------------------------------
