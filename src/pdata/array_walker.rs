use std::sync::{Arc, Mutex};

use crate::io_engine::*;
use crate::pdata::array::{self, *};
use crate::pdata::btree::{self, *};
use crate::pdata::btree_walker::*;
use crate::pdata::space_map::*;
use crate::pdata::unpack::*;

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

// FIXME: Eliminate this structure by impl NodeVisitor for ArrayWalker?
struct BlockValueVisitor<'a, V> {
    engine: Arc<dyn IoEngine + Send + Sync>,
    array_visitor: &'a mut dyn ArrayVisitor<V>,
    sm: Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    array_errs: Mutex<Vec<ArrayError>>,
}

impl<'a, V: Unpack + Copy> BlockValueVisitor<'a, V> {
    pub fn new(
        e: Arc<dyn IoEngine + Send + Sync>,
        sm: Arc<Mutex<dyn SpaceMap + Send + Sync>>,
        v: &'a mut dyn ArrayVisitor<V>,
    ) -> BlockValueVisitor<'a, V> {
        BlockValueVisitor {
            engine: e,
            array_visitor: v,
            sm,
            array_errs: Mutex::new(Vec::new()),
        }
    }
}

impl<'a, V: Unpack + Copy> NodeVisitor<u64> for BlockValueVisitor<'a, V> {
    fn visit(
        &self,
        path: &[u64],
        kr: &KeyRange,
        _h: &NodeHeader,
        keys: &[u64],
        values: &[u64],
    ) -> btree::Result<()> {
        if keys.is_empty() {
            return Ok(());
        }

        // The ordering of array indices had been verified in unpack_node(),
        // thus checking the upper bound implies key continuity among siblings.
        if *keys.first().unwrap() + keys.len() as u64 != *keys.last().unwrap() + 1 {
            return Err(btree::value_err("gaps in array indicies".to_string()));
        }
        if let Some(end) = kr.end {
            if *keys.last().unwrap() + 1 != end {
                return Err(btree::value_err(
                    "gaps or overlaps in array indicies".to_string(),
                ));
            }
        }

        // FIXME: will the returned blocks be reordered?
        match self.engine.read_many(values) {
            Err(_) => {
                // IO completely failed on all the child blocks
                for (i, b) in values.iter().enumerate() {
                    // TODO: report indices of array entries based on the type size
                    let mut array_errs = self.array_errs.lock().unwrap();
                    array_errs.push(array::io_err(&path, *b).index_context(keys[i]));
                }
            }
            Ok(rblocks) => {
                for (i, rb) in rblocks.into_iter().enumerate() {
                    match rb {
                        Err(_) => {
                            let mut array_errs = self.array_errs.lock().unwrap();
                            array_errs.push(array::io_err(&path, values[i]).index_context(keys[i]));
                        }
                        Ok(b) => {
                            let mut path = path.to_vec();
                            path.push(b.loc);
                            match unpack_array_block::<V>(&path, b.get_data()) {
                                Ok(array_block) => {
                                    if let Err(e) = self.array_visitor.visit(keys[i], array_block) {
                                        self.array_errs.lock().unwrap().push(e);
                                    }
                                    let mut sm = self.sm.lock().unwrap();
                                    sm.inc(b.loc, 1).unwrap();
                                }
                                Err(e) => {
                                    self.array_errs.lock().unwrap().push(e);
                                }
                            }
                            path.pop();
                        }
                    }
                }
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

impl ArrayWalker {
    pub fn new(engine: Arc<dyn IoEngine + Send + Sync>, ignore_non_fatal: bool) -> ArrayWalker {
        let nr_blocks = engine.get_nr_blocks() as u64;
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

    pub fn walk<V>(&self, visitor: &mut dyn ArrayVisitor<V>, root: u64) -> array::Result<()>
    where
        V: Unpack + Copy,
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
