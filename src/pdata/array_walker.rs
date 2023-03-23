use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::checksum;
use crate::io_engine::*;
use crate::pdata::array::{self, *};
use crate::pdata::btree::*;
use crate::pdata::btree_error::{self, *};
use crate::pdata::btree_walker::*;
use crate::pdata::space_map::*;
use crate::pdata::unpack::*;

#[cfg(test)]
mod tests;

//------------------------------------------

pub struct ArrayWalker {
    engine: Arc<dyn IoEngine + Send + Sync>,
    sm: Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    ignore_non_fatal: bool,
}

pub trait ArrayVisitor<V: Unpack> {
    fn visit(&self, index: u64, b: ArrayBlock<V>) -> array::Result<()>;
}

//------------------------------------------

struct BlockValueVisitor<'a, V> {
    engine: Arc<dyn IoEngine + Send + Sync>,
    array_visitor: &'a dyn ArrayVisitor<V>,
    sm: Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    array_errs: Mutex<Vec<ArrayError>>,
}

impl<'a, V: Unpack> BlockValueVisitor<'a, V> {
    pub fn new(
        e: Arc<dyn IoEngine + Send + Sync>,
        sm: Arc<Mutex<dyn SpaceMap + Send + Sync>>,
        v: &'a dyn ArrayVisitor<V>,
    ) -> BlockValueVisitor<'a, V> {
        BlockValueVisitor {
            engine: e,
            array_visitor: v,
            sm,
            array_errs: Mutex::new(Vec::new()),
        }
    }

    fn visit_array_block(&self, path: &[u64], index: u64, b: &Block) -> array::Result<()> {
        let mut array_path = path.to_vec();
        array_path.push(b.loc);

        let bt = checksum::metadata_block_type(b.get_data());
        if bt != checksum::BT::ARRAY {
            return Err(array_block_err(
                &array_path,
                &format!("checksum failed for array block {}, {:?}", b.loc, bt),
            ));
        }

        let array_block = unpack_array_block::<V>(&array_path, b.get_data())?;
        self.array_visitor.visit(index, array_block)
    }
}

impl<'a, V: Unpack> NodeVisitor<u64> for BlockValueVisitor<'a, V> {
    fn visit(
        &self,
        path: &[u64],
        kr: &KeyRange,
        _h: &NodeHeader,
        keys: &[u64],
        values: &[u64],
    ) -> btree_error::Result<()> {
        if keys.is_empty() {
            return Ok(());
        }

        // Verify key's continuity.
        // The ordering of keys had been verified in unpack_node(),
        // so comparing the keys against its context is sufficient.
        if *keys.first().unwrap() + keys.len() as u64 != *keys.last().unwrap() + 1 {
            return Err(btree_error::value_err("gaps in array indices".to_string()));
        }
        if let Some(end) = kr.end {
            if *keys.last().unwrap() + 1 != end {
                return Err(btree_error::value_err(
                    "non-contiguous array indices".to_string(),
                ));
            }
        }

        match self.engine.read_many(values) {
            Err(_) => {
                // IO completely failed on all the child blocks
                for (i, b) in values.iter().enumerate() {
                    // TODO: report the affected range of entries in the array?
                    let e = array::io_err(path, *b).index_context(keys[i]);
                    self.array_errs.lock().unwrap().push(e);
                }
            }
            Ok(rblocks) => {
                for (i, rb) in rblocks.into_iter().enumerate() {
                    match rb {
                        Err(_) => {
                            let e = array::io_err(path, values[i]).index_context(keys[i]);
                            self.array_errs.lock().unwrap().push(e);
                        }
                        Ok(b) => {
                            if let Err(e) = self.visit_array_block(path, keys[i], &b) {
                                self.array_errs
                                    .lock()
                                    .unwrap()
                                    .push(e.index_context(keys[i]));
                            } else {
                                let mut sm = self.sm.lock().unwrap();
                                sm.inc(b.loc, 1).unwrap();
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn visit_again(&self, _path: &[u64], _b: u64) -> btree_error::Result<()> {
        Ok(())
    }

    fn end_walk(&self) -> btree_error::Result<()> {
        Ok(())
    }
}

//------------------------------------------

impl ArrayWalker {
    pub fn new(engine: Arc<dyn IoEngine + Send + Sync>, ignore_non_fatal: bool) -> ArrayWalker {
        let nr_blocks = engine.get_nr_blocks();
        let r: ArrayWalker = ArrayWalker {
            engine,
            sm: Arc::new(Mutex::new(RestrictedSpaceMap::new(nr_blocks))),
            ignore_non_fatal,
        };
        r
    }

    pub fn new_with_sm(
        engine: Arc<dyn IoEngine + Send + Sync>,
        sm: Arc<Mutex<dyn SpaceMap + Send + Sync>>,
        ignore_non_fatal: bool,
    ) -> array::Result<ArrayWalker> {
        {
            let sm = sm.lock().unwrap();
            assert_eq!(sm.get_nr_blocks().unwrap(), engine.get_nr_blocks());
        }

        Ok(ArrayWalker {
            engine,
            sm,
            ignore_non_fatal,
        })
    }

    pub fn walk<V>(&self, visitor: &dyn ArrayVisitor<V>, root: u64) -> array::Result<()>
    where
        V: Unpack,
    {
        let w =
            BTreeWalker::new_with_sm(self.engine.clone(), self.sm.clone(), self.ignore_non_fatal)?;
        let mut path = vec![0];
        let v = BlockValueVisitor::<V>::new(self.engine.clone(), self.sm.clone(), visitor);
        let btree_err = w.walk(&mut path, &v, root).map_err(ArrayError::BTreeError);

        let mut array_errs = v.array_errs.into_inner().unwrap();
        if let Err(e) = btree_err {
            array_errs.push(e);
        }

        match array_errs.len() {
            0 => Ok(()),
            1 => Err(array_errs[0].clone()),
            _ => Err(ArrayError::Aggregate(array_errs)),
        }
    }
}

//------------------------------------------

struct BlockPathCollector {
    ablocks: Mutex<BTreeMap<u64, (Vec<u64>, u64)>>,
}

impl BlockPathCollector {
    fn new() -> BlockPathCollector {
        BlockPathCollector {
            ablocks: Mutex::new(BTreeMap::new()),
        }
    }
}

impl NodeVisitor<u64> for BlockPathCollector {
    fn visit(
        &self,
        path: &[u64],
        kr: &KeyRange,
        _h: &NodeHeader,
        keys: &[u64],
        values: &[u64],
    ) -> btree_error::Result<()> {
        // Verify key's continuity.
        // The ordering of keys had been verified in unpack_node(),
        // so comparing the keys against the key range is sufficient.
        if *keys.first().unwrap() + keys.len() as u64 != *keys.last().unwrap() + 1 {
            return Err(btree_error::value_err("gaps in array indices".to_string()));
        }
        if let Some(end) = kr.end {
            if *keys.last().unwrap() + 1 != end {
                return Err(btree_error::value_err(
                    "non-contiguous array indices".to_string(),
                ));
            }
        }

        let mut ablocks = self.ablocks.lock().unwrap();
        for (k, v) in keys.iter().zip(values) {
            ablocks.insert(*k, (path.to_vec(), *v));
        }

        Ok(())
    }

    fn visit_again(&self, _path: &[u64], _b: u64) -> btree_error::Result<()> {
        Ok(())
    }

    fn end_walk(&self) -> btree_error::Result<()> {
        Ok(())
    }
}

pub fn collect_array_blocks_with_path(
    engine: Arc<dyn IoEngine + Send + Sync>,
    ignore_non_fatal: bool,
    root: u64,
) -> btree_error::Result<BTreeMap<u64, (Vec<u64>, u64)>> {
    let walker = BTreeWalker::new(engine, ignore_non_fatal);
    let visitor = BlockPathCollector::new();
    walker.walk(&mut vec![0], &visitor, root)?;
    Ok(visitor.ablocks.into_inner().unwrap())
}

//------------------------------------------
