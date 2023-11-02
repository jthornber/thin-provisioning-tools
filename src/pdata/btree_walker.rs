use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use threadpool::ThreadPool;

use crate::io_engine::*;
use crate::pdata::btree::*;
use crate::pdata::space_map::*;
use crate::pdata::unpack::*;

#[cfg(test)]
mod tests;

//------------------------------------------

pub trait NodeVisitor<V: Unpack> {
    // &self is deliberately non mut to allow the walker to use multiple threads.
    fn visit(
        &self,
        path: &[u64],
        kr: &KeyRange,
        header: &NodeHeader,
        keys: &[u64],
        values: &[V],
    ) -> Result<()>;

    // Nodes may be shared and thus visited multiple times.  The walker avoids
    // doing repeated IO, but it does call this method to keep the visitor up to
    // date.
    fn visit_again(&self, path: &[u64], b: u64) -> Result<()>;

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
        let nr_blocks = engine.get_nr_blocks();
        let r: BTreeWalker = BTreeWalker {
            engine,
            sm: Arc::new(Mutex::new(RestrictedSpaceMap::new(nr_blocks))),
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
        fails.get(&b).cloned()
    }

    fn set_fail(&self, b: u64, err: BTreeError) {
        // FIXME: should we monitor the size of fails, and abort if too many errors?
        let mut fails = self.fails.lock().unwrap();
        fails.insert(b, err);
    }

    // Atomically increments the ref count, and returns the _old_ count.
    fn sm_inc(&self, b: u64) -> Result<u32> {
        let mut sm = self.sm.lock().unwrap();
        let count = sm.get(b).map_err(|e| value_err(e.to_string()))?;
        sm.inc(b, 1).map_err(|e| value_err(e.to_string()))?;
        Ok(count)
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
            let rc = match self.sm_inc(bs[i]) {
                Ok(n) => n,
                Err(_) => {
                    continue;
                }
            };

            if rc == 0 {
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
                        // TODO: revisit the node if the key context is different
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
                for (i, rb) in rblocks.into_iter().enumerate() {
                    match rb {
                        Err(_) => {
                            let e = io_err(path).keys_context(&filtered_krs[i]);
                            errs.push(e.clone());
                            self.set_fail(blocks[i], e);
                        }
                        Ok(b) => {
                            if let Err(e) =
                                self.walk_node(path, visitor, &filtered_krs[i], &b, false)
                            {
                                errs.push(e);
                            }
                        }
                    }
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

        let node = match check_and_unpack_node::<V>(b, self.ignore_non_fatal, is_root) {
            Ok(n) => n,
            Err(err) => {
                let e = node_err(path, err).keys_context(kr);
                self.set_fail(b.loc, e.clone());
                return Err(e);
            }
        };

        match node {
            Internal { keys, values, .. } => {
                let krs = split_key_ranges(path, kr, &keys)?;
                let errs = self.walk_nodes(path, visitor, &krs, &values);
                return self.build_aggregate(b.loc, errs); // implicitly calls set_fail()
            }
            Leaf {
                header,
                keys,
                values,
            } => {
                if let Err(e) = visitor.visit(path, kr, &header, &keys, &values) {
                    let e = BTreeError::Path(path.clone(), Box::new(e)).keys_context(kr);
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
        r
    }

    pub fn walk<NV, V>(&self, path: &mut Vec<u64>, visitor: &NV, root: u64) -> Result<()>
    where
        NV: NodeVisitor<V>,
        V: Unpack,
    {
        let result = if self.sm_inc(root)? > 0 {
            if let Some(e) = self.failed(root) {
                Err(e)
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
        };

        if let Err(e) = visitor.end_walk() {
            if let Err(tree_err) = result {
                return Err(BTreeError::Aggregate(vec![tree_err, e]));
            }
        }

        result
    }
}

//--------------------------------

fn walk_node_threaded_<NV, V>(
    w: Arc<BTreeWalker>,
    path: &[u64],
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

    let node = match check_and_unpack_node::<V>(b, w.ignore_non_fatal, is_root) {
        Ok(n) => n,
        Err(err) => {
            let e = node_err(path, err).keys_context(kr);
            w.set_fail(b.loc, e.clone());
            return Err(e);
        }
    };

    match node {
        Internal { keys, values, .. } => {
            let krs = split_key_ranges(path, kr, &keys)?;
            let errs = walk_nodes_threaded(w.clone(), path, pool, visitor, &krs, &values);
            return w.build_aggregate(b.loc, errs); // implicitly calls set_fail()
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
    let r = walk_node_threaded_(w, path, pool, visitor, kr, b, is_root);
    path.pop();
    r
}

fn walk_nodes_threaded<NV, V>(
    w: Arc<BTreeWalker>,
    path: &[u64],
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
        let rc = match w.sm_inc(bs[i]) {
            Ok(n) => n,
            Err(_) => {
                continue;
            }
        };

        if rc == 0 {
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
                    // TODO: revisit the node if the key context is different
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
            let child_errs = Arc::new(Mutex::new(Vec::new()));

            for (i, rb) in rblocks.into_iter().enumerate() {
                match rb {
                    Err(_) => {
                        let e = io_err(path).keys_context(&filtered_krs[i]);
                        let mut errs = child_errs.lock().unwrap();
                        errs.push(e.clone());
                        w.set_fail(blocks[i], e);
                    }
                    Ok(b) => {
                        let w = w.clone();
                        let visitor = visitor.clone();
                        let kr = filtered_krs[i].clone();
                        let errs = child_errs.clone();
                        let mut path = path.to_vec();

                        pool.execute(move || {
                            if let Err(e) = w.walk_node(&mut path, visitor.as_ref(), &kr, &b, false)
                            {
                                let mut errs = errs.lock().unwrap();
                                errs.push(e);
                            }
                        });
                    }
                }
            }

            pool.join();
            let mut child_errs = Arc::try_unwrap(child_errs).unwrap().into_inner().unwrap();
            errs.append(&mut child_errs);
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
    let result = if w.sm_inc(root)? > 0 {
        if let Some(e) = w.failed(root) {
            Err(e)
        } else {
            visitor.visit_again(path, root)
        }
    } else {
        let root = w.engine.read(root).map_err(|_| io_err(path))?;
        let kr = KeyRange {
            start: None,
            end: None,
        };
        walk_node_threaded(w, path, pool, visitor.clone(), &kr, &root, true)
    };

    if let Err(e) = visitor.end_walk() {
        if let Err(tree_err) = result {
            return Err(BTreeError::Aggregate(vec![tree_err, e]));
        }
    }

    result
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
        _path: &[u64],
        _kr: &KeyRange,
        _h: &NodeHeader,
        keys: &[u64],
        values: &[V],
    ) -> Result<()> {
        let mut vals = self.values.lock().unwrap();
        for n in 0..keys.len() {
            vals.insert(keys[n], values[n]);
        }

        Ok(())
    }

    fn visit_again(&self, _path: &[u64], _b: u64) -> Result<()> {
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
        path: &[u64],
        _kr: &KeyRange,
        _h: &NodeHeader,
        keys: &[u64],
        values: &[V],
    ) -> Result<()> {
        let mut vals = self.values.lock().unwrap();
        for n in 0..keys.len() {
            vals.insert(keys[n], (path.to_vec(), values[n].clone()));
        }

        Ok(())
    }

    fn visit_again(&self, _path: &[u64], _b: u64) -> Result<()> {
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

struct KeyCollector {
    keys: Mutex<BTreeSet<u64>>,
}

impl KeyCollector {
    fn new() -> KeyCollector {
        KeyCollector {
            keys: Mutex::new(BTreeSet::new()),
        }
    }
}

impl<V: Unpack + Copy> NodeVisitor<V> for KeyCollector {
    fn visit(
        &self,
        _path: &[u64],
        _kr: &KeyRange,
        _h: &NodeHeader,
        keys: &[u64],
        _values: &[V],
    ) -> Result<()> {
        let mut keyset = self.keys.lock().unwrap();
        for k in keys {
            keyset.insert(*k);
        }

        Ok(())
    }

    fn visit_again(&self, _path: &[u64], _b: u64) -> Result<()> {
        Ok(())
    }

    fn end_walk(&self) -> Result<()> {
        Ok(())
    }
}

pub fn btree_to_key_set<V: Unpack + Copy>(
    path: &mut Vec<u64>,
    engine: Arc<dyn IoEngine + Send + Sync>,
    ignore_non_fatal: bool,
    root: u64,
) -> Result<BTreeSet<u64>> {
    let walker = BTreeWalker::new(engine, ignore_non_fatal);
    let visitor = KeyCollector::new();
    walker.walk::<_, V>(path, &visitor, root)?;
    Ok(visitor.keys.into_inner().unwrap())
}

//------------------------------------------

struct ValueVecCollector<V> {
    values: Mutex<Vec<V>>,
}

impl<V> ValueVecCollector<V> {
    fn new() -> ValueVecCollector<V> {
        ValueVecCollector {
            values: Mutex::new(Vec::new()),
        }
    }
}

// FIXME: should we be using Copy rather than clone? (Yes)
impl<V: Unpack + Copy> NodeVisitor<V> for ValueVecCollector<V> {
    fn visit(
        &self,
        _path: &[u64],
        _kr: &KeyRange,
        _h: &NodeHeader,
        _keys: &[u64],
        values: &[V],
    ) -> Result<()> {
        let mut vals = self.values.lock().unwrap();
        vals.extend_from_slice(values);
        Ok(())
    }

    fn visit_again(&self, _path: &[u64], _b: u64) -> Result<()> {
        Ok(())
    }

    fn end_walk(&self) -> Result<()> {
        Ok(())
    }
}

pub fn btree_to_value_vec<V: Unpack + Copy>(
    path: &mut Vec<u64>,
    engine: Arc<dyn IoEngine + Send + Sync>,
    ignore_non_fatal: bool,
    root: u64,
) -> Result<Vec<V>> {
    let walker = BTreeWalker::new(engine, ignore_non_fatal);
    let visitor = ValueVecCollector::<V>::new();
    walker.walk(path, &visitor, root)?;
    Ok(visitor.values.into_inner().unwrap())
}

//------------------------------------------

struct NoopVisitor<V> {
    dummy: std::marker::PhantomData<V>,
}

impl<V> NoopVisitor<V> {
    pub fn new() -> NoopVisitor<V> {
        NoopVisitor {
            dummy: std::marker::PhantomData,
        }
    }
}

impl<V: Unpack> NodeVisitor<V> for NoopVisitor<V> {
    fn visit(
        &self,
        _path: &[u64],
        _kr: &KeyRange,
        _header: &NodeHeader,
        _keys: &[u64],
        _values: &[V],
    ) -> Result<()> {
        Ok(())
    }

    fn visit_again(&self, _path: &[u64], _b: u64) -> Result<()> {
        Ok(())
    }

    fn end_walk(&self) -> Result<()> {
        Ok(())
    }
}

pub fn count_btree_blocks<V: Unpack>(
    engine: Arc<dyn IoEngine + Send + Sync>,
    path: &mut Vec<u64>,
    root: u64,
    metadata_sm: ASpaceMap,
    ignore_non_fatal: bool,
) -> Result<()> {
    let w = BTreeWalker::new_with_sm(engine, metadata_sm, ignore_non_fatal)?;
    let v = NoopVisitor::<V>::new();
    w.walk(path, &v, root)
}

//------------------------------------------
