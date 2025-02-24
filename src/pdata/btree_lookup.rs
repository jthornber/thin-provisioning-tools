use anyhow::Result;

use crate::io_engine::*;
use crate::pdata::btree::*;
use crate::pdata::unpack::*;

//------------------------------------------

// Inefficient because we unpack entire nodes, but I think that's
// fine for the current use cases (eg, lookup of dev_details).
pub fn btree_lookup<V>(engine: &dyn IoEngine, root: u64, key: u64) -> Result<Option<V>>
where
    V: Unpack + Clone,
{
    let mut path = Vec::new();
    let mut loc = root;
    let mut is_root = true;

    loop {
        let block = engine.read(loc)?;
        let node = unpack_node::<V>(&path, block.get_data(), true, is_root)?;
        match node {
            Node::Internal { keys, values, .. } => {
                // Select a child ...
                let idx = match keys.binary_search(&key) {
                    Ok(idx) => idx,
                    Err(idx) => {
                        if idx == 0 {
                            return Ok(None);
                        }

                        idx - 1
                    }
                };

                // ... and move to it.
                loc = values[idx];
                path.push(values[idx]);
            }
            Node::Leaf { keys, values, .. } => {
                let idx = keys.binary_search(&key);
                return match idx {
                    Ok(idx) => Ok(Some(values[idx].clone())),
                    Err(_) => Ok(None),
                };
            }
        }

        is_root = false; // After the first iteration, we are no longer at the root.
    }
}

//------------------------------------------
