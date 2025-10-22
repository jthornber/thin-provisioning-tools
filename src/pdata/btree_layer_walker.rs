use fixedbitset::FixedBitSet;
use std::collections::BTreeMap;

use crate::io_engine::*;
use crate::pdata::btree::*;
use crate::pdata::btree_utils::*;
use crate::pdata::btree_walker::{NodeVisitor, ValueCollector};
use crate::pdata::space_map::aggregator::*;
use crate::pdata::unpack::Unpack;

struct LayerHandler<'a> {
    aggregator: &'a Aggregator,
    children: FixedBitSet,
    is_root: bool,
    ignore_non_fatal: bool,
    error: Option<anyhow::Error>,
}

impl<'a> LayerHandler<'a> {
    fn new(is_root: bool, aggregator: &'a Aggregator, ignore_non_fatal: bool) -> Self {
        Self {
            aggregator,
            children: FixedBitSet::with_capacity(aggregator.get_nr_blocks()),
            is_root,
            ignore_non_fatal,
            error: None,
        }
    }

    fn get_children(self) -> FixedBitSet {
        self.children
    }
}

impl<'a> ReadHandler for LayerHandler<'a> {
    fn handle(&mut self, loc: u64, data: std::io::Result<&[u8]>) {
        use anyhow::anyhow;

        match data {
            Ok(data) => {
                match &check_and_unpack_node_::<u64>(data, loc, self.ignore_non_fatal, self.is_root)
                {
                    // TODO: check parent context of the keys
                    Ok(Node::Internal { values, .. }) => {
                        // insert the node info in pre-order fashion to better detect loops in the path
                        let seen = self.aggregator.test_and_inc(values);
                        let nr_blocks = self.aggregator.get_nr_blocks() as u64;

                        for (i, v) in values.iter().enumerate() {
                            if !seen.contains(i) && *v < nr_blocks {
                                self.children.insert(*v as usize);
                            }
                        }
                    }
                    Ok(_) => {
                        self.error
                            .get_or_insert_with(|| anyhow!("node {} is not an internal", loc));
                    }
                    Err(e) => {
                        self.error
                            .get_or_insert_with(|| anyhow!("{} at block {}", e, loc));
                    }
                }
            }
            Err(e) => {
                self.error
                    .get_or_insert_with(|| anyhow!("{} at block {}", e, loc));
            }
        }
    }

    fn complete(&mut self) {}
}

fn read_internal_nodes<V: Unpack>(
    engine: &dyn IoEngine,
    io_buffers: &mut BufferPool,
    aggregator: &Aggregator,
    root: u64,
    ignore_non_fatal: bool,
) -> anyhow::Result<(usize, FixedBitSet)> {
    let nr_blocks = aggregator.get_nr_blocks();
    if root >= nr_blocks as u64 {
        return Err(anyhow::anyhow!("block {} out of space map boundary", root));
    }

    let seen = aggregator.test_and_inc(&[root]);
    if seen.contains(0) {
        return Ok((0, FixedBitSet::new()));
    }

    let mut current_layer = FixedBitSet::with_capacity(nr_blocks);
    current_layer.insert(root as usize);

    let depth = get_depth::<V>(engine, root)?;
    if depth == 0 {
        return Ok((0, current_layer)); // TODO: avoid allocating the bitset in this situation
    }

    // Read the internal nodes, layer by layer.
    let mut is_root = true;
    for _d in (0..depth).rev() {
        let mut handler = LayerHandler::new(is_root, aggregator, ignore_non_fatal);
        is_root = false;

        engine.read_blocks(
            io_buffers,
            &mut current_layer.ones().map(|n| n as u64),
            &mut handler,
        )?;

        if let Some(e) = handler.error {
            return Err(e);
        }

        current_layer = handler.get_children();
    }

    Ok((depth, current_layer))
}

struct LeafHandler<'a, V: Unpack> {
    visitor: &'a dyn NodeVisitor<V>,
    is_root: bool,
    ignore_non_fatal: bool,
    error: Option<anyhow::Error>,
    dummy: std::marker::PhantomData<V>,
}

impl<'a, V: Unpack> LeafHandler<'a, V> {
    fn new(nv: &'a dyn NodeVisitor<V>, is_root: bool, ignore_non_fatal: bool) -> Self {
        Self {
            visitor: nv,
            is_root,
            ignore_non_fatal,
            error: None,
            dummy: std::marker::PhantomData,
        }
    }
}

impl<'a, V: Unpack> ReadHandler for LeafHandler<'a, V> {
    fn handle(&mut self, loc: u64, data: std::io::Result<&[u8]>) {
        use anyhow::anyhow;

        match data {
            Ok(data) => {
                match &check_and_unpack_node_(data, loc, self.ignore_non_fatal, self.is_root) {
                    Ok(Node::Leaf {
                        header,
                        keys,
                        values,
                    }) => {
                        // FIXME: avoid unpacking the node, which copies the key-values
                        let _ = self
                            .visitor
                            .visit(&[], &KeyRange::default(), header, keys, values);
                    }
                    Ok(_) => {
                        self.error
                            .get_or_insert_with(|| anyhow!("node {} is not a leaf", loc));
                    }
                    Err(e) => {
                        self.error
                            .get_or_insert_with(|| anyhow!("{} at block {}", e, loc));
                    }
                }
            }
            Err(e) => {
                self.error
                    .get_or_insert_with(|| anyhow!("{} at block {}", e, loc));
            }
        }
    }

    fn complete(&mut self) {}
}

fn unpacker<V: Unpack>(
    engine: &dyn IoEngine,
    nv: &dyn NodeVisitor<V>,
    leaves: &mut dyn Iterator<Item = u64>,
    is_root: bool,
    ignore_non_fatal: bool,
) -> anyhow::Result<()> {
    let io_block_size = 64 * 1024;
    let buffer_size = 16 * 1024 * 1024; // 16m
    let nr_io_blocks = buffer_size / io_block_size;
    let mut pool = BufferPool::new(nr_io_blocks, io_block_size);

    let mut handler = LeafHandler::<V>::new(nv, is_root, ignore_non_fatal);
    engine.read_blocks(&mut pool, leaves, &mut handler)?;

    if let Some(e) = handler.error {
        return Err(e);
    }

    Ok(())
}

fn read_leaf_nodes<V: Unpack>(
    engine: &dyn IoEngine,
    nv: &dyn NodeVisitor<V>,
    leaves: &FixedBitSet,
    depth: usize,
    ignore_non_fatal: bool,
) -> anyhow::Result<()> {
    unpacker::<V>(
        engine,
        nv,
        &mut leaves.ones().map(|b| b as u64),
        depth == 0,
        ignore_non_fatal,
    )
}

pub fn read_nodes<V: Unpack>(
    engine: &dyn IoEngine,
    nv: &dyn NodeVisitor<V>,
    aggregator: &Aggregator,
    root: u64,
    ignore_non_fatal: bool,
) -> anyhow::Result<()> {
    let buffer_size = 16 * 1024 * 1024;
    let nr_io_blocks = buffer_size / BLOCK_SIZE;
    let mut pool = BufferPool::new(nr_io_blocks, BLOCK_SIZE);

    let (depth, leaves) =
        read_internal_nodes::<V>(engine, &mut pool, aggregator, root, ignore_non_fatal)?;
    read_leaf_nodes(engine, nv, &leaves, depth, ignore_non_fatal)
}

//------------------------------------------

pub fn btree_to_map_with_aggregator<V: Unpack + Copy + Send + Sync + 'static>(
    engine: &dyn IoEngine,
    aggregator: &Aggregator,
    root: u64,
    ignore_non_fatal: bool,
) -> anyhow::Result<BTreeMap<u64, V>> {
    // FIXME: the mutex lock inside ValueCollector might slow down the ReadHandler
    let visitor = ValueCollector::new();
    read_nodes(engine, &visitor, aggregator, root, ignore_non_fatal)?;

    let mut results = BTreeMap::new();
    {
        let mut r = visitor.values.lock().unwrap();
        std::mem::swap(&mut results, &mut r);
    }
    Ok(results)
}

//------------------------------------------
