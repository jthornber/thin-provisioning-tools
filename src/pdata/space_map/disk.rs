use anyhow::Result;

use crate::pdata::btree_builder::*;
use crate::pdata::space_map::common::*;
use crate::pdata::space_map::*;
use crate::write_batcher::*;

//------------------------------------------

pub fn write_disk_sm(w: &mut WriteBatcher, sm: &dyn SpaceMap) -> Result<SMRoot> {
    let (index_entries, ref_count_root) = write_common(w, sm)?;

    let mut index_builder: BTreeBuilder<IndexEntry> = BTreeBuilder::new(Box::new(NoopRC {}));
    for (i, ie) in index_entries.iter().enumerate() {
        index_builder.push_value(w, i as u64, *ie)?;
    }

    let bitmap_root = index_builder.complete(w)?;
    w.flush()?;

    Ok(SMRoot {
        nr_blocks: sm.get_nr_blocks()?,
        nr_allocated: sm.get_nr_allocated()?,
        bitmap_root,
        ref_count_root,
    })
}

//------------------------------------------
