use crate::io_engine::*;
use crate::pdata::btree::*;
use crate::pdata::unpack::*;

//------------------------------------------

pub fn get_depth_<V: Unpack>(
    engine: &dyn IoEngine,
    path: &mut Vec<u64>,
    root: u64,
    is_root: bool,
) -> Result<usize> {
    use Node::*;

    let b = engine.read(root).map_err(|_| io_err(path))?;
    let node = check_and_unpack_node::<V>(&b, true, is_root).map_err(|e| node_err(path, e))?;

    match node {
        Internal { values, .. } => {
            // recurse down to the first good leaf
            let mut error = None;
            for child in values {
                if path.contains(&child) {
                    continue; // skip loops
                }

                path.push(child);
                match get_depth_::<V>(engine, path, child, false) {
                    Ok(n) => return Ok(n + 1),
                    Err(e) => {
                        error.get_or_insert(e);
                    }
                }
                path.pop();
            }
            Err(error.unwrap_or_else(|| node_err(path, NodeError::NumEntriesTooSmall)))
        }
        Leaf { .. } => Ok(0),
    }
}

/// Gets the depth of a bottom level mapping tree.  0 means the root is a leaf node.
pub fn get_depth<V: Unpack>(engine: &dyn IoEngine, root: u64) -> Result<usize> {
    let mut path = vec![root];
    get_depth_::<V>(engine, &mut path, root, true)
}

//------------------------------------------
