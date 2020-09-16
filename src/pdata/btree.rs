use nom::{number::complete::*, IResult};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use thiserror::Error;
use threadpool::ThreadPool;

use crate::checksum;
use crate::io_engine::*;
use crate::pdata::space_map::*;
use crate::pdata::unpack::*;

// FIXME: check that keys are in ascending order between nodes.

//------------------------------------------

#[derive(Clone, Debug, PartialEq)]
pub struct KeyRange {
    start: Option<u64>,
    end: Option<u64>, // This is the one-past-the-end value
}

impl fmt::Display for KeyRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (self.start, self.end) {
            (None, None) => write!(f, "[..]"),
            (None, Some(e)) => write!(f, "[..{}]", e),
            (Some(s), None) => write!(f, "[{}..]", s),
            (Some(s), Some(e)) => write!(f, "[{}..{}]", s, e),
        }
    }
}

impl KeyRange {
    // None will be returned if either range would be zero length
    fn split(&self, n: u64) -> Option<(KeyRange, KeyRange)> {
        match (self.start, self.end) {
            (None, None) => Some((
                KeyRange {
                    start: None,
                    end: Some(n),
                },
                KeyRange {
                    start: Some(n),
                    end: None,
                },
            )),
            (None, Some(e)) => {
                if n < e {
                    Some((
                        KeyRange {
                            start: None,
                            end: Some(n),
                        },
                        KeyRange {
                            start: Some(n),
                            end: Some(e),
                        },
                    ))
                } else {
                    None
                }
            }
            (Some(s), None) => {
                if s < n {
                    Some((
                        KeyRange {
                            start: Some(s),
                            end: Some(n),
                        },
                        KeyRange {
                            start: Some(n),
                            end: None,
                        },
                    ))
                } else {
                    None
                }
            }
            (Some(s), Some(e)) => {
                if s < n && n < e {
                    Some((
                        KeyRange {
                            start: Some(s),
                            end: Some(n),
                        },
                        KeyRange {
                            start: Some(n),
                            end: Some(e),
                        },
                    ))
                } else {
                    None
                }
            }
        }
    }
}

#[test]
fn test_split_range() {
    struct Test(Option<u64>, Option<u64>, u64, Option<(KeyRange, KeyRange)>);

    let tests = vec![
        Test(
            None,
            None,
            100,
            Some((
                KeyRange {
                    start: None,
                    end: Some(100),
                },
                KeyRange {
                    start: Some(100),
                    end: None,
                },
            )),
        ),
        Test(None, Some(100), 1000, None),
        Test(
            None,
            Some(100),
            50,
            Some((
                KeyRange {
                    start: None,
                    end: Some(50),
                },
                KeyRange {
                    start: Some(50),
                    end: Some(100),
                },
            )),
        ),
        Test(None, Some(100), 100, None),
        Test(Some(100), None, 50, None),
        Test(
            Some(100),
            None,
            150,
            Some((
                KeyRange {
                    start: Some(100),
                    end: Some(150),
                },
                KeyRange {
                    start: Some(150),
                    end: None,
                },
            )),
        ),
        Test(Some(100), Some(200), 50, None),
        Test(Some(100), Some(200), 250, None),
        Test(
            Some(100),
            Some(200),
            150,
            Some((
                KeyRange {
                    start: Some(100),
                    end: Some(150),
                },
                KeyRange {
                    start: Some(150),
                    end: Some(200),
                },
            )),
        ),
    ];

    for Test(start, end, n, expected) in tests {
        let kr = KeyRange { start, end };
        let actual = kr.split(n);
        assert_eq!(actual, expected);
    }
}

fn split_one(kr: &KeyRange, k: u64) -> Result<(KeyRange, KeyRange)> {
    match kr.split(k) {
        None => {
            return Err(node_err(&format!(
                "couldn't split key range {} at {}",
                kr, k
            )));
        }
        Some(pair) => Ok(pair),
    }
}

fn split_key_ranges_(kr: &KeyRange, keys: &[u64]) -> Result<Vec<KeyRange>> {
    let mut krs = Vec::with_capacity(keys.len());

    if keys.len() == 0 {
        return Err(node_err("split_key_ranges: no keys present"));
    }

    // The first key gives the lower bound
    let mut kr = KeyRange {
        start: Some(keys[0]),
        end: kr.end,
    };

    for i in 1..keys.len() {
        let (first, rest) = split_one(&kr, keys[i])?;
        krs.push(first);
        kr = rest;
    }

    krs.push(kr);

    Ok(krs)
}

fn split_key_ranges(kr: &KeyRange, keys: &[u64]) -> Result<Vec<KeyRange>> {
    let msg = format!("split: {:?} at {:?}", &kr, &keys);
    let r = split_key_ranges_(kr, keys);
    if r.is_err() {
        eprintln!("{} -> {:?}", msg, &r);
    }

    r
}

//------------------------------------------

const NODE_HEADER_SIZE: usize = 32;

#[derive(Error, Clone, Debug)]
pub enum BTreeError {
    // #[error("io error")]
    IoError, //   (std::io::Error), // FIXME: we can't clone an io_error

    // #[error("node error: {0}")]
    NodeError(String),

    // #[error("value error: {0}")]
    ValueError(String),

    // #[error("keys: {0:?}")]
    KeyContext(KeyRange, Box<BTreeError>),

    // #[error("aggregate: {0:?}")]
    Aggregate(Vec<BTreeError>),

    // #[error("{0:?}, {1}")]
    Path(u64, Box<BTreeError>),
}

fn extract_path(e: &BTreeError) -> (Vec<u64>, BTreeError) {
    let mut path = Vec::new();
    let mut e = e;

    loop {
        match e {
            BTreeError::Path(b, next) => {
                path.push(*b);
                e = next.as_ref();
            }
            _ => return (path, e.clone()),
        }
    }
}

impl fmt::Display for BTreeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (path, e) = extract_path(self);
        match e {
            BTreeError::IoError => write!(f, "io error, path{:?}", path),
            BTreeError::NodeError(msg) => write!(f, "node error: {}, path{:?}", msg, path),
            BTreeError::ValueError(msg) => write!(f, "value error: {}, path{:?}", msg, path),
            BTreeError::KeyContext(kr, be) => {
                write!(f, "{}, effecting keys {}, path{:?}", be, kr, path)
            }
            BTreeError::Aggregate(errs) => {
                for e in errs {
                    write!(f, "{}", e)?
                }
                Ok(())
            }
            // Can't happen
            BTreeError::Path(_, e) => write!(f, "{}", e),
        }
    }
}
pub fn node_err(msg: &str) -> BTreeError {
    BTreeError::NodeError(msg.to_string())
}

fn node_err_s(msg: String) -> BTreeError {
    BTreeError::NodeError(msg)
}

pub fn io_err() -> BTreeError {
    BTreeError::IoError
}

pub fn value_err(msg: String) -> BTreeError {
    BTreeError::ValueError(msg)
}

fn aggregate_error(rs: Vec<BTreeError>) -> BTreeError {
    BTreeError::Aggregate(rs)
}

impl BTreeError {
    pub fn keys_context(self, keys: &KeyRange) -> BTreeError {
        BTreeError::KeyContext(keys.clone(), Box::new(self))
    }
}

pub type Result<T> = std::result::Result<T, BTreeError>;

//------------------------------------------

#[derive(Debug, Clone)]
pub struct NodeHeader {
    pub block: u64,
    pub is_leaf: bool,
    pub nr_entries: u32,
    pub max_entries: u32,
    pub value_size: u32,
}

#[allow(dead_code)]
const INTERNAL_NODE: u32 = 1;
const LEAF_NODE: u32 = 2;

impl Unpack for NodeHeader {
    fn disk_size() -> u32 {
        32
    }

    fn unpack(data: &[u8]) -> IResult<&[u8], NodeHeader> {
        let (i, _csum) = le_u32(data)?;
        let (i, flags) = le_u32(i)?;
        let (i, block) = le_u64(i)?;
        let (i, nr_entries) = le_u32(i)?;
        let (i, max_entries) = le_u32(i)?;
        let (i, value_size) = le_u32(i)?;
        let (i, _padding) = le_u32(i)?;

        Ok((
            i,
            NodeHeader {
                block,
                is_leaf: flags == LEAF_NODE,
                nr_entries,
                max_entries,
                value_size,
            },
        ))
    }
}

#[derive(Clone)]
pub enum Node<V: Unpack> {
    Internal {
        header: NodeHeader,
        keys: Vec<u64>,
        values: Vec<u64>,
    },
    Leaf {
        header: NodeHeader,
        keys: Vec<u64>,
        values: Vec<V>,
    },
}

impl<V: Unpack> Node<V> {
    pub fn get_header(&self) -> &NodeHeader {
        use Node::*;
        match self {
            Internal { header, .. } => header,
            Leaf { header, .. } => header,
        }
    }
}

pub fn convert_result<'a, V>(r: IResult<&'a [u8], V>) -> Result<(&'a [u8], V)> {
    r.map_err(|_e| node_err("parse error"))
}

pub fn convert_io_err<V>(r: std::io::Result<V>) -> Result<V> {
    r.map_err(|_| io_err())
}

pub fn unpack_node<V: Unpack>(
    data: &[u8],
    ignore_non_fatal: bool,
    is_root: bool,
) -> Result<Node<V>> {
    use nom::multi::count;

    let (i, header) =
        NodeHeader::unpack(data).map_err(|_e| node_err("couldn't parse node header"))?;

    if header.is_leaf && header.value_size != V::disk_size() {
        return Err(node_err_s(format!(
            "value_size mismatch: expected {}, was {}",
            V::disk_size(),
            header.value_size
        )));
    }

    let elt_size = header.value_size + 8;
    if elt_size as usize * header.max_entries as usize + NODE_HEADER_SIZE > BLOCK_SIZE {
        return Err(node_err_s(format!(
            "max_entries is too large ({})",
            header.max_entries
        )));
    }

    if header.nr_entries > header.max_entries {
        return Err(node_err("nr_entries > max_entries"));
    }

    if !ignore_non_fatal {
        if header.max_entries % 3 != 0 {
            return Err(node_err("max_entries is not divisible by 3"));
        }

        if !is_root {
            let min = header.max_entries / 3;
            if header.nr_entries < min {
                return Err(node_err_s(format!(
                    "too few entries {}, expected at least {}",
                    header.nr_entries, min
                )));
            }
        }
    }

    let (i, keys) = convert_result(count(le_u64, header.nr_entries as usize)(i))?;

    let mut last = None;
    for k in &keys {
        if let Some(l) = last {
            if k <= l {
                return Err(node_err("keys out of order"));
            }
        }

        last = Some(k);
    }

    let nr_free = header.max_entries - header.nr_entries;
    let (i, _padding) = convert_result(count(le_u64, nr_free as usize)(i))?;

    if header.is_leaf {
        let (_i, values) = convert_result(count(V::unpack, header.nr_entries as usize)(i))?;

        Ok(Node::Leaf {
            header,
            keys,
            values,
        })
    } else {
        let (_i, values) = convert_result(count(le_u64, header.nr_entries as usize)(i))?;
        Ok(Node::Internal {
            header,
            keys,
            values,
        })
    }
}

//------------------------------------------

pub trait NodeVisitor<V: Unpack> {
    // &self is deliberately non mut to allow the walker to use multiple threads.
    fn visit(&self, keys: &KeyRange, header: &NodeHeader, keys: &[u64], values: &[V])
        -> Result<()>;
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

    fn walk_nodes<NV, V>(&self, visitor: &NV, kr: &[KeyRange], bs: &[u64]) -> Vec<BTreeError>
    where
        NV: NodeVisitor<V>,
        V: Unpack,
    {
        let mut errs: Vec<BTreeError> = Vec::new();

        let mut blocks = Vec::with_capacity(bs.len());
        for b in bs {
            if self.sm_inc(*b) == 0 {
                // Node not yet seen
                blocks.push(*b);
            } else {
                // This node has already been checked ...
                match self.failed(*b) {
                    None => {
                        // ... it was clean so we can ignore.
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
                    let e = io_err().keys_context(&kr[i]);
                    errs.push(e.clone());
                    self.set_fail(*b, e);
                }
            }
            Ok(rblocks) => {
                let mut i = 0;
                for rb in rblocks {
                    match rb {
                        Err(_) => {
                            let e = io_err().keys_context(&kr[i]);
                            errs.push(e.clone());
                            self.set_fail(blocks[i], e);
                        }
                        Ok(b) => match self.walk_node(visitor, &kr[i], &b, false) {
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

    fn walk_node_<NV, V>(&self, visitor: &NV, kr: &KeyRange, b: &Block, is_root: bool) -> Result<()>
    where
        NV: NodeVisitor<V>,
        V: Unpack,
    {
        use Node::*;

        let bt = checksum::metadata_block_type(b.get_data());
        if bt != checksum::BT::NODE {
            return Err(
                node_err_s(format!("checksum failed for node {}, {:?}", b.loc, bt))
                    .keys_context(kr),
            );
        }

        let node = unpack_node::<V>(&b.get_data(), self.ignore_non_fatal, is_root)?;

        match node {
            Internal { keys, values, .. } => {
                let krs = split_key_ranges(&kr, &keys)?;
                let errs = self.walk_nodes(visitor, &krs, &values);
                return self.build_aggregate(b.loc, errs);
            }
            Leaf {
                header,
                keys,
                values,
            } => {
                if let Err(e) = visitor.visit(&kr, &header, &keys, &values) {
                    self.set_fail(b.loc, e.clone());
                    return Err(e);
                }
            }
        }

        Ok(())
    }

    fn walk_node<NV, V>(&self, visitor: &NV, kr: &KeyRange, b: &Block, is_root: bool) -> Result<()>
    where
        NV: NodeVisitor<V>,
        V: Unpack,
    {
        let r = self.walk_node_(visitor, kr, b, is_root);
        let r = match r {
            Err(e) => Err(BTreeError::Path(b.loc, Box::new(e))),
            Ok(v) => Ok(v),
        };
        r
    }

    pub fn walk<NV, V>(&self, visitor: &NV, root: u64) -> Result<()>
    where
        NV: NodeVisitor<V>,
        V: Unpack,
    {
        if self.sm_inc(root) > 0 {
            if let Some(e) = self.failed(root) {
                Err(e.clone())
            } else {
                Ok(())
            }
        } else {
            let root = self.engine.read(root).map_err(|_| io_err())?;
            let kr = KeyRange {
                start: None,
                end: None,
            };
            self.walk_node(visitor, &kr, &root, true)
        }
    }
}

//--------------------------------

/*
fn walk_node_threaded<NV, V>(
    w: Arc<BTreeWalker>,
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
        return Err(node_err_s(format!(
            "checksum failed for node {}, {:?}",
            b.loc, bt
        )));
    }

    let node = unpack_node::<V>(&b.get_data(), w.ignore_non_fatal, is_root)?;

    match node {
        Internal { keys, values, .. } => {
            let krs = BTreeWalker::split_key_ranges(&kr, &keys)?;
            walk_nodes_threaded(w, pool, visitor, &krs, &values)?;
        }
        Leaf {
            header,
            keys,
            values,
        } => {
            visitor.visit(kr, &header, &keys, &values)?;
        }
    }

    Ok(())
}

fn walk_nodes_threaded<NV, V>(
    w: Arc<BTreeWalker>,
    pool: &ThreadPool,
    visitor: Arc<NV>,
    krs: &[KeyRange],
    bs: &[u64],
) -> Result<()>
where
    NV: NodeVisitor<V> + Send + Sync + 'static,
    V: Unpack,
{
    let mut blocks = Vec::new();
    for b in bs {
        if w.sm_inc(*b)? == 0 {
            blocks.push(*b);
        }
    }

    let rblocks = convert_io_err(w.engine.read_many(&blocks[0..]))?;

    let mut i = 0;
    for b in rblocks {
        match b {
            Err(_) => {
                // FIXME: aggregate these
                return Err(io_err());
            }
            Ok(b) => {
                let w = w.clone();
                let visitor = visitor.clone();
                let kr = krs[i].clone();

                pool.execute(move || {
                    let result = w.walk_node(visitor.as_ref(), &kr, &b, false);
                    if result.is_err() {
                        todo!();
                    }
                });
            }
        }

        i += 1;
    }
    pool.join();

    Ok(())
}

pub fn walk_threaded<NV, V>(
    w: Arc<BTreeWalker>,
    pool: &ThreadPool,
    visitor: Arc<NV>,
    root: u64,
) -> Result<()>
where
    NV: NodeVisitor<V> + Send + Sync + 'static,
    V: Unpack,
{
    if w.sm_inc(root)? > 0 {
        Ok(())
    } else {
        let root = convert_io_err(w.engine.read(root))?;
        let kr = KeyRange {
            start: None,
            end: None,
        };
        walk_node_threaded(w, pool, visitor, &kr, &root, true)
    }
}
*/

pub fn walk_threaded<NV, V>(
    w: Arc<BTreeWalker>,
    pool: &ThreadPool,
    visitor: Arc<NV>,
    root: u64,
) -> Result<()>
where
    NV: NodeVisitor<V> + Send + Sync + 'static,
    V: Unpack,
{
    w.walk(visitor.as_ref(), root)
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

impl<V: Unpack + Clone> NodeVisitor<V> for ValueCollector<V> {
    fn visit(&self, _kr: &KeyRange, _h: &NodeHeader, keys: &[u64], values: &[V]) -> Result<()> {
        let mut vals = self.values.lock().unwrap();
        for n in 0..keys.len() {
            vals.insert(keys[n], values[n].clone());
        }

        Ok(())
    }
}

pub fn btree_to_map<V: Unpack + Clone>(
    engine: Arc<dyn IoEngine + Send + Sync>,
    ignore_non_fatal: bool,
    root: u64,
) -> Result<BTreeMap<u64, V>> {
    let walker = BTreeWalker::new(engine, ignore_non_fatal);
    let visitor = ValueCollector::<V>::new();
    walker.walk(&visitor, root)?;
    Ok(visitor.values.into_inner().unwrap())
}

pub fn btree_to_map_with_sm<V: Unpack + Clone>(
    engine: Arc<dyn IoEngine + Send + Sync>,
    sm: Arc<Mutex<dyn SpaceMap + Send + Sync>>,
    ignore_non_fatal: bool,
    root: u64,
) -> Result<BTreeMap<u64, V>> {
    let walker = BTreeWalker::new_with_sm(engine, sm, ignore_non_fatal)?;
    let visitor = ValueCollector::<V>::new();

    walker.walk(&visitor, root)?;
    Ok(visitor.values.into_inner().unwrap())
}

//------------------------------------------
