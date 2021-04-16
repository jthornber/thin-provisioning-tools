use std::sync::{Arc, Mutex};

use crate::io_engine::*;
use crate::pdata::array::{self, *};
use crate::pdata::btree::{self, *};
use crate::pdata::btree_walker::*;
use crate::pdata::unpack::*;

//------------------------------------------

pub struct ArrayWalker {
    engine: Arc<dyn IoEngine + Send + Sync>,
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
    array_errs: Mutex<Vec<ArrayError>>,
}

impl<'a, V: Unpack + Copy> BlockValueVisitor<'a, V> {
    pub fn new(
        e: Arc<dyn IoEngine + Send + Sync>,
        v: &'a mut dyn ArrayVisitor<V>,
    ) -> BlockValueVisitor<'a, V> {
        BlockValueVisitor {
            engine: e,
            array_visitor: v,
            array_errs: Mutex::new(Vec::new()),
        }
    }
}

impl<'a, V: Unpack + Copy> NodeVisitor<u64> for BlockValueVisitor<'a, V> {
    // FIXME: wrap ArrayError into BTreeError, rather than mapping to value_err?
    fn visit(
        &self,
        path: &[u64],
        _kr: &KeyRange,
        _h: &NodeHeader,
        keys: &[u64],
        values: &[u64],
    ) -> btree::Result<()> {
        let mut path = path.to_vec();
        let mut errs: Vec<BTreeError> = Vec::new();

        // TODO: check index continuity
        match self.engine.read_many(values) {
            Err(_) => {
                // IO completely failed on all the child blocks
                // FIXME: count read errors on its parent (BTreeError::IoError) or on its location
                // (ArrayError::IoError)?
                for (_i, _b) in values.iter().enumerate() {
                    errs.push(btree::io_err(&path)); // FIXME: add key_context
                }
            }
            Ok(rblocks) => {
                for (i, rb) in rblocks.into_iter().enumerate() {
                    match rb {
                        Err(_) => {
                            errs.push(btree::io_err(&path)); // FIXME: add key_context
                        },
                        Ok(b) => {
                            path.push(b.loc);
                            match unpack_array_block::<V>(&path, b.get_data()) {
                                Ok(array_block) => {
                                    // FIXME: will the returned blocks be reordered?
                                    if let Err(e) = self.array_visitor.visit(keys[i], array_block) {
                                        self.array_errs.lock().unwrap().push(e);
                                    }
                                },
                                Err(e) => {
                                    self.array_errs.lock().unwrap().push(e);
                                }
                            }
                            path.pop();
                        },
                    }
                }
            }
        }

        // FIXME: duplicate to BTreeWalker::build_aggregrate()
        match errs.len() {
            0 => Ok(()),
            1 => {
                let e = errs[0].clone();
                Err(e)
            }
            _ => {
                let e = btree::aggregate_error(errs);
                Err(e)
            }
        }
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
        let r: ArrayWalker = ArrayWalker {
            engine,
            ignore_non_fatal,
        };
        r
    }

    pub fn walk<V>(&self, visitor: &mut dyn ArrayVisitor<V>, root: u64) -> array::Result<()>
    where
        V: Unpack + Copy,
    {
        let w = BTreeWalker::new(self.engine.clone(), self.ignore_non_fatal);
        let mut path = Vec::new();
        path.push(0);
        let v = BlockValueVisitor::<V>::new(self.engine.clone(), visitor);
        let btree_err = w.walk(&mut path, &v, root).map_err(|e| ArrayError::BTreeError(e));

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
