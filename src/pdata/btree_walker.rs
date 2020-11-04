use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use threadpool::ThreadPool;

use crate::checksum;
use crate::io_engine::*;
use crate::pdata::btree::*;
use crate::pdata::space_map::*;
use crate::pdata::unpack::*;

//------------------------------------------

pub trait NodeVisitor<V: Unpack> {
    // &self is deliberately non mut to allow the walker to use multiple threads.
    fn visit(
        &self,
        path: &Vec<u64>,
        kr: &KeyRange,
        header: &NodeHeader,
        keys: &[u64],
        values: &[V],
    ) -> Result<()>;

    // Nodes may be shared and thus visited multiple times.  The walker avoids
    // doing repeated IO, but it does call this method to keep the visitor up to
    // date.
    fn visit_again(&self, path: &Vec<u64>, b: u64) -> Result<()>;

    fn end_walk(&self) -> Result<()>;
}

#[derive(Clone)]
pub struct BTreeWalker {
    engine: Arc<dyn IoEngine + Send + Sync>,
    sm: Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    fails: Arc<Mutex<BTreeMap<u64, BTreeError>>>,
    ignore_non_fatal: bool,
}

impl BTreeWalker {
    pub fn new(engine: Arc<dyn IoEngine + Send + Sync>, ignore_non_fatal: bool) -> BTreeWalker {
        let nr_blocks = engine.get_nr_blocks() as usize;
        let r: BTreeWalker = BTreeWalker {
            engine,
            sm: Arc::new(Mutex::new(RestrictedSpaceMap::new(nr_blocks as u64))),
            fails: Arc::new(Mutex::new(BTreeMap::new())),
            ignore_non_fatal,
        };
        r
    }

    pub fn new_with_sm(
        engine: Arc<dyn IoEngine + Send + Sync>,
        sm: Arc<Mutex<dyn SpaceMap + Send + Sync>>,
        ignore_non_fatal: bool,
    ) -> Result<BTreeWalker> {
        {
            let sm = sm.lock().unwrap();
            assert_eq!(sm.get_nr_blocks().unwrap(), engine.get_nr_blocks());
        }

        Ok(BTreeWalker {
            engine,
            sm,
            fails: Arc::new(Mutex::new(BTreeMap::new())),
            ignore_non_fatal,
        })
    }

    fn failed(&self, b: u64) -> Option<BTreeError> {
        let fails = self.fails.lock().unwrap();
        match fails.get(&b) {
            None => None,
            Some(e) => Some(e.clone()),
        }
    }

    fn set_fail(&self, b: u64, err: BTreeError) {
        // FIXME: should we monitor the size of fails, and abort if too many errors?
        let mut fails = self.fails.lock().unwrap();
        fails.insert(b, err);
    }

    // Atomically increments the ref count, and returns the _old_ count.
    fn sm_inc(&self, b: u64) -> u32 {
        let mut sm = self.sm.lock().unwrap();
        let count = sm.get(b).unwrap();
        sm.inc(b, 1).unwrap();
        count
    }

    fn build_aggregate(&self, b: u64, errs: Vec<BTreeError>) -> Result<()> {
        match errs.len() {
            0 => Ok(()),
            1 => {
                let e = errs[0].clone();
                self.set_fail(b, e.clone());
                Err(e)
            }
            _ => {
                let e = aggregate_error(errs);
                self.set_fail(b, e.clone());
                Err(e)
            }
        }
    }

    fn walk_nodes<NV, V>(
        &self,
        path: &mut Vec<u64>,
        visitor: &NV,
        krs: &[KeyRange],
        bs: &[u64],
    ) -> Vec<BTreeError>
    where
        NV: NodeVisitor<V>,
        V: Unpack,
    {
        assert_eq!(krs.len(), bs.len());
        let mut errs: Vec<BTreeError> = Vec::new();

        let mut blocks = Vec::with_capacity(bs.len());
        let mut filtered_krs = Vec::with_capacity(krs.len());
        for i in 0..bs.len() {
            if self.sm_inc(bs[i]) == 0 {
                // Node not yet seen
                blocks.push(bs[i]);
                filtered_krs.push(krs[i].clone());
            } else {
                // This node has already been checked ...
                match self.failed(bs[i]) {
                    None => {
                        // ... it was clean.
                        if let Err(e) = visitor.visit_again(path, bs[i]) {
                            // ... but the visitor isn't happy
                            errs.push(e.clone());
                        }
                    }
                    Some(e) => {
                        // ... there was an error
                        errs.push(e.clone());
                    }
                }
            }
        }

        match self.engine.read_many(&blocks[0..]) {
            Err(_) => {
                // IO completely failed, error every block
                for (i, b) in blocks.iter().enumerate() {
                    let e = io_err(path).keys_context(&filtered_krs[i]);
                    errs.push(e.clone());
                    self.set_fail(*b, e);
                }
            }
            Ok(rblocks) => {
                let mut i = 0;
                for rb in rblocks {
                    match rb {
                        Err(_) => {
                            let e = io_err(path).keys_context(&filtered_krs[i]);
                            errs.push(e.clone());
                            self.set_fail(blocks[i], e);
                        }
                        Ok(b) => match self.walk_node(path, visitor, &filtered_krs[i], &b, false) {
                            Err(e) => {
                                errs.push(e);
                            }
                            Ok(()) => {}
                        },
                    }

                    i += 1;
                }
            }
        }

        errs
    }

    fn walk_node_<NV, V>(
        &self,
        path: &mut Vec<u64>,
        visitor: &NV,
        kr: &KeyRange,
        b: &Block,
        is_root: bool,
    ) -> Result<()>
    where
        NV: NodeVisitor<V>,
        V: Unpack,
    {
        use Node::*;

        let bt = checksum::metadata_block_type(b.get_data());
        if bt != checksum::BT::NODE {
            return Err(node_err_s(
                path,
                format!("checksum failed for node {}, {:?}", b.loc, bt),
            )
            .keys_context(kr));
        }

        let node = unpack_node::<V>(path, &b.get_data(), self.ignore_non_fatal, is_root)?;

        match node {
            Internal { keys, values, .. } => {
                let krs = split_key_ranges(path, &kr, &keys)?;
                let errs = self.walk_nodes(path, visitor, &krs, &values);
                return self.build_aggregate(b.loc, errs);
            }
            Leaf {
                header,
                keys,
                values,
            } => {
                if let Err(e) = visitor.visit(path, &kr, &header, &keys, &values) {
                    let e = BTreeError::Path(path.clone(), Box::new(e.clone()));
                    self.set_fail(b.loc, e.clone());
                    return Err(e);
                }
            }
        }

        Ok(())
    }

    fn walk_node<NV, V>(
        &self,
        path: &mut Vec<u64>,
        visitor: &NV,
        kr: &KeyRange,
        b: &Block,
        is_root: bool,
    ) -> Result<()>
    where
        NV: NodeVisitor<V>,
        V: Unpack,
    {
        path.push(b.loc);
        let r = self.walk_node_(path, visitor, kr, b, is_root);
        path.pop();
        visitor.end_walk()?;
        r
    }

    pub fn walk<NV, V>(&self, path: &mut Vec<u64>, visitor: &NV, root: u64) -> Result<()>
    where
        NV: NodeVisitor<V>,
        V: Unpack,
    {
        if self.sm_inc(root) > 0 {
            if let Some(e) = self.failed(root) {
                Err(e.clone())
            } else {
                visitor.visit_again(path, root)
            }
        } else {
            let root = self.engine.read(root).map_err(|_| io_err(path))?;
            let kr = KeyRange {
                start: None,
                end: None,
            };
            self.walk_node(path, visitor, &kr, &root, true)
        }
    }
}

//--------------------------------

fn walk_node_threaded_<NV, V>(
    w: Arc<BTreeWalker>,
    path: &mut Vec<u64>,
    pool: &ThreadPool,
    visitor: Arc<NV>,
    kr: &KeyRange,
    b: &Block,
    is_root: bool,
) -> Result<()>
where
    NV: NodeVisitor<V> + Send + Sync + 'static,
    V: Unpack,
{
    use Node::*;

    let bt = checksum::metadata_block_type(b.get_data());
    if bt != checksum::BT::NODE {
        return Err(node_err_s(
            path,
            format!("checksum failed for node {}, {:?}", b.loc, bt),
        )
        .keys_context(kr));
    }

    let node = unpack_node::<V>(path, &b.get_data(), w.ignore_non_fatal, is_root)?;

    match node {
        Internal { keys, values, .. } => {
            let krs = split_key_ranges(path, &kr, &keys)?;
            let errs = walk_nodes_threaded(w.clone(), path, pool, visitor, &krs, &values);
            return w.build_aggregate(b.loc, errs);
        }
        Leaf {
            header,
            keys,
            values,
        } => {
            visitor.visit(path, kr, &header, &keys, &values)?;
        }
    }

    Ok(())
}

fn walk_node_threaded<NV, V>(
    w: Arc<BTreeWalker>,
    path: &mut Vec<u64>,
    pool: &ThreadPool,
    visitor: Arc<NV>,
    kr: &KeyRange,
    b: &Block,
    is_root: bool,
) -> Result<()>
where
    NV: NodeVisitor<V> + Send + Sync + 'static,
    V: Unpack,
{
    path.push(b.loc);
    let r = walk_node_threaded_(w, path, pool, visitor.clone(), kr, b, is_root);
    path.pop();
    visitor.end_walk()?;
    r
}

fn walk_nodes_threaded<NV, V>(
    w: Arc<BTreeWalker>,
    path: &mut Vec<u64>,
    pool: &ThreadPool,
    visitor: Arc<NV>,
    krs: &[KeyRange],
    bs: &[u64],
) -> Vec<BTreeError>
where
    NV: NodeVisitor<V> + Send + Sync + 'static,
    V: Unpack,
{
    assert_eq!(krs.len(), bs.len());
    let mut errs: Vec<BTreeError> = Vec::new();

    let mut blocks = Vec::with_capacity(bs.len());
    let mut filtered_krs = Vec::with_capacity(krs.len());
    for i in 0..bs.len() {
        if w.sm_inc(bs[i]) == 0 {
            // Node not yet seen
            blocks.push(bs[i]);
            filtered_krs.push(krs[i].clone());
        } else {
            // This node has already been checked ...
            match w.failed(bs[i]) {
                None => {
                    // ... it was clean.
                    if let Err(e) = visitor.visit_again(path, bs[i]) {
                        // ... but the visitor isn't happy
                        errs.push(e.clone());
                    }
                }
                Some(e) => {
                    // ... there was an error
                    errs.push(e.clone());
                }
            }
        }
    }

    match w.engine.read_many(&blocks[0..]) {
        Err(_) => {
            // IO completely failed error every block
            for (i, b) in blocks.iter().enumerate() {
                let e = io_err(path).keys_context(&filtered_krs[i]);
                errs.push(e.clone());
                w.set_fail(*b, e);
            }
        }
        Ok(rblocks) => {
            let mut i = 0;
            let errs = Arc::new(Mutex::new(Vec::new()));

            for rb in rblocks {
                match rb {
                    Err(_) => {
                        let e = io_err(path).keys_context(&filtered_krs[i]);
                        let mut errs = errs.lock().unwrap();
                        errs.push(e.clone());
                        w.set_fail(blocks[i], e);
                    }
                    Ok(b) => {
                        let w = w.clone();
                        let visitor = visitor.clone();
                        let kr = filtered_krs[i].clone();
                        let errs = errs.clone();
                        let mut path = path.clone();

                        pool.execute(move || {
                            match w.walk_node(&mut path, visitor.as_ref(), &kr, &b, false) {
                                Err(e) => {
                                    let mut errs = errs.lock().unwrap();
                                    errs.push(e);
                                }
                                Ok(()) => {}
                            }
                        });
                    }
                }

                i += 1;
            }

            pool.join();
        }
    }

    errs
}

pub fn walk_threaded<NV, V>(
    path: &mut Vec<u64>,
    w: Arc<BTreeWalker>,
    pool: &ThreadPool,
    visitor: Arc<NV>,
    root: u64,
) -> Result<()>
where
    NV: NodeVisitor<V> + Send + Sync + 'static,
    V: Unpack,
{
    if w.sm_inc(root) > 0 {
        if let Some(e) = w.failed(root) {
            Err(e.clone())
        } else {
            visitor.visit_again(path, root)
        }
    } else {
        let root = w.engine.read(root).map_err(|_| io_err(path))?;
        let kr = KeyRange {
            start: None,
            end: None,
        };
        walk_node_threaded(w, path, pool, visitor, &kr, &root, true)
    }
}

//------------------------------------------

struct ValueCollector<V> {
    values: Mutex<BTreeMap<u64, V>>,
}

impl<V> ValueCollector<V> {
    fn new() -> ValueCollector<V> {
        ValueCollector {
            values: Mutex::new(BTreeMap::new()),
        }
    }
}

// FIXME: should we be using Copy rather than clone? (Yes)
impl<V: Unpack + Copy> NodeVisitor<V> for ValueCollector<V> {
    fn visit(
        &self,
        _path: &Vec<u64>,
        _kr: &KeyRange,
        _h: &NodeHeader,
        keys: &[u64],
        values: &[V],
    ) -> Result<()> {
        let mut vals = self.values.lock().unwrap();
        for n in 0..keys.len() {
            vals.insert(keys[n], values[n].clone());
        }

        Ok(())
    }

    fn visit_again(&self, _path: &Vec<u64>, _b: u64) -> Result<()> {
        Ok(())
    }

    fn end_walk(&self) -> Result<()> {
        Ok(())
    }
}

pub fn btree_to_map<V: Unpack + Copy>(
    path: &mut Vec<u64>,
    engine: Arc<dyn IoEngine + Send + Sync>,
    ignore_non_fatal: bool,
    root: u64,
) -> Result<BTreeMap<u64, V>> {
    let walker = BTreeWalker::new(engine, ignore_non_fatal);
    let visitor = ValueCollector::<V>::new();
    walker.walk(path, &visitor, root)?;
    Ok(visitor.values.into_inner().unwrap())
}

pub fn btree_to_map_with_sm<V: Unpack + Copy>(
    path: &mut Vec<u64>,
    engine: Arc<dyn IoEngine + Send + Sync>,
    sm: Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    ignore_non_fatal: bool,
    root: u64,
) -> Result<BTreeMap<u64, V>> {
    let walker = BTreeWalker::new_with_sm(engine, sm, ignore_non_fatal)?;
    let visitor = ValueCollector::<V>::new();

    walker.walk(path, &visitor, root)?;
    Ok(visitor.values.into_inner().unwrap())
}

//------------------------------------------

struct ValuePathCollector<V> {
    values: Mutex<BTreeMap<u64, (Vec<u64>, V)>>,
}

impl<V> ValuePathCollector<V> {
    fn new() -> ValuePathCollector<V> {
        ValuePathCollector {
            values: Mutex::new(BTreeMap::new()),
        }
    }
}

impl<V: Unpack + Clone> NodeVisitor<V> for ValuePathCollector<V> {
    fn visit(
        &self,
        path: &Vec<u64>,
        _kr: &KeyRange,
        _h: &NodeHeader,
        keys: &[u64],
        values: &[V],
    ) -> Result<()> {
        let mut vals = self.values.lock().unwrap();
        for n in 0..keys.len() {
            vals.insert(keys[n], (path.clone(), values[n].clone()));
        }

        Ok(())
    }

    fn visit_again(&self, _path: &Vec<u64>, _b: u64) -> Result<()> {
        Ok(())
    }

    fn end_walk(&self) -> Result<()> {
        Ok(())
    }
}

pub fn btree_to_map_with_path<V: Unpack + Copy>(
    path: &mut Vec<u64>,
    engine: Arc<dyn IoEngine + Send + Sync>,
    sm: Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    ignore_non_fatal: bool,
    root: u64,
) -> Result<BTreeMap<u64, (Vec<u64>, V)>> {
    let walker = BTreeWalker::new_with_sm(engine, sm, ignore_non_fatal)?;
    let visitor = ValuePathCollector::<V>::new();

    walker.walk(path, &visitor, root)?;
    Ok(visitor.values.into_inner().unwrap())
}

//------------------------------------------
