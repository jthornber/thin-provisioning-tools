use std::sync::Arc;

use crate::io_engine::*;
use crate::pdata::array::*;
use crate::pdata::array_block::*;
use crate::pdata::btree::*;
use crate::pdata::btree_walker::*;
use crate::pdata::unpack::*;

//------------------------------------------

pub struct ArrayWalker {
    engine: Arc<dyn IoEngine + Send + Sync>,
    ignore_non_fatal: bool
}

// FIXME: define another Result type for array visiting?
pub trait ArrayBlockVisitor<V: Unpack> {
    fn visit(
        &self,
        index: u64,
        v: V,
    ) -> anyhow::Result<()>;
}

struct BlockValueVisitor<V> {
    engine: Arc<dyn IoEngine + Send + Sync>,
    array_block_visitor: Box<dyn ArrayBlockVisitor<V>>,
}

impl<V: Unpack + Copy> BlockValueVisitor<V> {
    pub fn new<Visitor: 'static + ArrayBlockVisitor<V>>(
        e: Arc<dyn IoEngine + Send + Sync>,
        v: Box<Visitor>,
    ) -> BlockValueVisitor<V> {
        BlockValueVisitor {
            engine: e,
            array_block_visitor: v,
        }
    }

    pub fn visit_array_block(
        &self,
        index: u64,
        array_block: ArrayBlock<V>,
    ) {
        let begin = index * u64::from(array_block.header.nr_entries);
        for i in 0..array_block.header.nr_entries {
            self.array_block_visitor.visit(begin + u64::from(i), array_block.values[i as usize]).unwrap();
        }
    }
}

impl<V: Unpack + Copy> NodeVisitor<ArrayBlockEntry> for BlockValueVisitor<V> {
    // FIXME: return errors
    fn visit(
        &self,
        path: &[u64],
        _kr: &KeyRange,
        _h: &NodeHeader,
        keys: &[u64],
        values: &[ArrayBlockEntry],
    ) -> Result<()> {
        for n in 0..keys.len() {
            let index = keys[n];
            let b = self.engine.read(values[n].block).map_err(|_| io_err(path))?;
            let array_block = unpack_array_block::<V>(b.get_data()).map_err(|_| io_err(path))?;
            self.visit_array_block(index, array_block);
        }
        Ok(())
    }

    // FIXME: stub
    fn visit_again(&self, _path: &[u64], _b: u64) -> Result<()> {
        Ok(())
    }

    // FIXME: stub
    fn end_walk(&self) -> Result<()> {
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

    // FIXME: pass the visitor by reference?
    // FIXME: redefine the Result type for array visiting?
    pub fn walk<BV, V: Copy>(&self, visitor: Box<BV>, root: u64) -> Result<()>
    where
        BV: 'static + ArrayBlockVisitor<V>,
        V: Unpack,
    {
        let w = BTreeWalker::new(self.engine.clone(), self.ignore_non_fatal);
        let mut path = Vec::new(); // FIXME: eliminate this line?
        path.push(0);
        let v = BlockValueVisitor::<V>::new(self.engine.clone(), visitor);
        w.walk(&mut path, &v, root)
    }
}

//------------------------------------------
