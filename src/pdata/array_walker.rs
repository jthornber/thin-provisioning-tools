use std::sync::Arc;

use crate::checksum;
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

// FIXME: define another Result type for array visiting?
pub trait ArrayVisitor<V: Unpack> {
    fn visit(&self, index: u64, v: V) -> array::Result<()>;
}

struct BlockValueVisitor<'a, V> {
    engine: Arc<dyn IoEngine + Send + Sync>,
    array_visitor: &'a mut dyn ArrayVisitor<V>,
}

impl<'a, V: Unpack + Copy> BlockValueVisitor<'a, V> {
    pub fn new(
        e: Arc<dyn IoEngine + Send + Sync>,
        v: &'a mut dyn ArrayVisitor<V>,
    ) -> BlockValueVisitor<'a, V> {
        BlockValueVisitor {
            engine: e,
            array_visitor: v,
        }
    }

    pub fn visit_array_block(&self, index: u64, array_block: ArrayBlock<V>) -> array::Result<()>{
        let mut errs: Vec<ArrayError> = Vec::new();

        let begin = index * array_block.header.max_entries as u64;
        for i in 0..array_block.header.nr_entries {
            if let Err(e) = self.array_visitor.visit(begin + i as u64, array_block.values[i as usize]) {
                errs.push(e); // TODO: add path or keys context?
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
                let e = array::aggregate_error(errs);
                Err(e)
            }
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

        for (n, index) in keys.iter().enumerate() {
            let b = self.engine.read(values[n]).map_err(|_| io_err(&path))?;

            // FIXME: move to unpack_array_block?
            let bt = checksum::metadata_block_type(b.get_data());
            if bt != checksum::BT::ARRAY {
                errs.push(btree::value_err(
                    format!("checksum failed for array block {}, {:?}", b.loc, bt)
                ));
            }

            path.push(values[n]);
            match unpack_array_block::<V>(&path, b.get_data()) {
                Ok(array_block) => {
                    if let Err(e) = self.visit_array_block(*index, array_block) {
                        errs.push(btree::value_err(format!("{}", e)));
                    }
                },
                Err(e) => {
                    errs.push(btree::value_err(format!("{}", e)));
                }
            }
            path.pop();
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

impl ArrayWalker {
    pub fn new(engine: Arc<dyn IoEngine + Send + Sync>, ignore_non_fatal: bool) -> ArrayWalker {
        let r: ArrayWalker = ArrayWalker {
            engine,
            ignore_non_fatal,
        };
        r
    }

    // FIXME: redefine the Result type for array visiting?
    pub fn walk<V>(&self, visitor: &mut dyn ArrayVisitor<V>, root: u64) -> array::Result<()>
    where
        V: Unpack + Copy,
    {
        let w = BTreeWalker::new(self.engine.clone(), self.ignore_non_fatal);
        let mut path = Vec::new();
        path.push(0);
        let v = BlockValueVisitor::<V>::new(self.engine.clone(), visitor);
        w.walk(&mut path, &v, root).map_err(|e| ArrayError::BTreeError(e))
    }
}

//------------------------------------------
