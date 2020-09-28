use anyhow::anyhow;
use byteorder::{ReadBytesExt, WriteBytesExt};
use data_encoding::BASE64;
use nom::{number::complete::*, IResult};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use thiserror::Error;
use threadpool::ThreadPool;

use crate::checksum;
use crate::io_engine::*;
use crate::pack::vm;
use crate::pdata::space_map::*;
use crate::pdata::unpack::*;

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

fn split_one(path: &Vec<u64>, kr: &KeyRange, k: u64) -> Result<(KeyRange, KeyRange)> {
    match kr.split(k) {
        None => {
            return Err(node_err(
                path,
                &format!("couldn't split key range {} at {}", kr, k),
            ));
        }
        Some(pair) => Ok(pair),
    }
}

fn split_key_ranges(path: &Vec<u64>, kr: &KeyRange, keys: &[u64]) -> Result<Vec<KeyRange>> {
    let mut krs = Vec::with_capacity(keys.len());

    if keys.len() == 0 {
        return Err(node_err(path, "split_key_ranges: no keys present"));
    }

    // The first key gives the lower bound
    let mut kr = KeyRange {
        start: Some(keys[0]),
        end: kr.end,
    };

    for i in 1..keys.len() {
        let (first, rest) = split_one(path, &kr, keys[i])?;
        krs.push(first);
        kr = rest;
    }

    krs.push(kr);

    Ok(krs)
}

//------------------------------------------

// We compress and base64 encode paths to make them easy to
// cut and paste between programs (eg, thin_explore -p <path>)
pub fn encode_node_path(path: &[u64]) -> String {
    let mut buffer: Vec<u8> = Vec::with_capacity(128);
    let mut cursor = std::io::Cursor::new(&mut buffer);
    assert!(path.len() < 256);

    // The first entry is normally the superblock (0), so we
    // special case this.
    if path.len() > 0 && path[0] == 0 {
        let count = ((path.len() as u8) - 1) << 1;
        cursor.write_u8(count as u8).unwrap();
        vm::pack_u64s(&mut cursor, &path[1..]).unwrap();
    } else {
        let count = ((path.len() as u8) << 1) | 1;
        cursor.write_u8(count as u8).unwrap();
        vm::pack_u64s(&mut cursor, path).unwrap();
    }

    BASE64.encode(&buffer)
}

pub fn decode_node_path(text: &str) -> anyhow::Result<Vec<u64>> {
    let mut buffer = vec![0; 128];
    let bytes = &mut buffer[0..BASE64.decode_len(text.len()).unwrap()];
    BASE64
        .decode_mut(text.as_bytes(), &mut bytes[0..])
        .map_err(|_| anyhow!("bad node path.  Unable to base64 decode."))?;

    let mut input = std::io::Cursor::new(bytes);

    let mut count = input.read_u8()?;
    let mut prepend_zero = false;
    if (count & 0x1) == 0 {
        // Implicit 0 as first entry
        prepend_zero = true;
    }
    count >>= 1;

    let count = count as usize;
    let mut path;
    if count == 0 {
        path = vec![];
    } else {
        let mut output = Vec::with_capacity(count * 8);
        let mut cursor = std::io::Cursor::new(&mut output);

        let mut vm = vm::VM::new();
        let written = vm.exec(&mut input, &mut cursor, count * 8)?;
        assert_eq!(written, count * 8);

        let mut cursor = std::io::Cursor::new(&mut output);
        path = vm::unpack_u64s(&mut cursor, count)?;
    }

    if prepend_zero {
        let mut full_path = vec![0u64];
        full_path.append(&mut path);
        Ok(full_path)
    } else {
        Ok(path)
    }
}

#[test]
fn test_encode_path() {
    struct Test(Vec<u64>);

    let tests = vec![
        Test(vec![]),
        Test(vec![1]),
        Test(vec![1, 2]),
        Test(vec![1, 2, 3, 4]),
        Test(vec![0]),
        Test(vec![0, 0]),
        Test(vec![0, 1]),
        Test(vec![0, 1, 2]),
        Test(vec![0, 123, 201231, 3102983012]),
    ];

    for t in tests {
        let encoded = encode_node_path(&t.0[0..]);
        let decoded = decode_node_path(&encoded).unwrap();
        assert_eq!(decoded, &t.0[0..]);
    }
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
    Path(Vec<u64>, Box<BTreeError>),
}

impl fmt::Display for BTreeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BTreeError::IoError => write!(f, "io error"),
            BTreeError::NodeError(msg) => write!(f, "node error: {}", msg),
            BTreeError::ValueError(msg) => write!(f, "value error: {}", msg),
            BTreeError::KeyContext(kr, be) => write!(f, "{}, effecting keys {}", be, kr),
            BTreeError::Aggregate(errs) => {
                for e in errs {
                    write!(f, "{}", e)?
                }
                Ok(())
            }
            BTreeError::Path(path, e) => write!(f, "{} {}", e, encode_node_path(path)),
        }
    }
}
pub fn node_err(path: &Vec<u64>, msg: &str) -> BTreeError {
    BTreeError::Path(
        path.clone(),
        Box::new(BTreeError::NodeError(msg.to_string())),
    )
}

fn node_err_s(path: &Vec<u64>, msg: String) -> BTreeError {
    BTreeError::Path(path.clone(), Box::new(BTreeError::NodeError(msg)))
}

pub fn io_err(path: &Vec<u64>) -> BTreeError {
    BTreeError::Path(path.clone(), Box::new(BTreeError::IoError))
}

pub fn value_err(msg: String) -> BTreeError {
    BTreeError::ValueError(msg)
}

pub fn aggregate_error(rs: Vec<BTreeError>) -> BTreeError {
    BTreeError::Aggregate(rs)
}

impl BTreeError {
    pub fn keys_context(self, keys: &KeyRange) -> BTreeError {
        BTreeError::KeyContext(keys.clone(), Box::new(self))
    }
}

pub type Result<T> = std::result::Result<T, BTreeError>;

//------------------------------------------

#[derive(Debug, Clone, Copy)]
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

pub fn convert_result<'a, V>(path: &Vec<u64>, r: IResult<&'a [u8], V>) -> Result<(&'a [u8], V)> {
    r.map_err(|_e| node_err(path, "parse error"))
}

pub fn convert_io_err<V>(path: &Vec<u64>, r: std::io::Result<V>) -> Result<V> {
    r.map_err(|_| io_err(path))
}

pub fn unpack_node<V: Unpack>(
    path: &Vec<u64>,
    data: &[u8],
    ignore_non_fatal: bool,
    is_root: bool,
) -> Result<Node<V>> {
    use nom::multi::count;

    let (i, header) =
        NodeHeader::unpack(data).map_err(|_e| node_err(path, "couldn't parse node header"))?;

    if header.is_leaf && header.value_size != V::disk_size() {
        return Err(node_err_s(
            path,
            format!(
                "value_size mismatch: expected {}, was {}",
                V::disk_size(),
                header.value_size
            ),
        ));
    }

    let elt_size = header.value_size + 8;
    if elt_size as usize * header.max_entries as usize + NODE_HEADER_SIZE > BLOCK_SIZE {
        return Err(node_err_s(
            path,
            format!("max_entries is too large ({})", header.max_entries),
        ));
    }

    if header.nr_entries > header.max_entries {
        return Err(node_err(path, "nr_entries > max_entries"));
    }

    if !ignore_non_fatal {
        if header.max_entries % 3 != 0 {
            return Err(node_err(path, "max_entries is not divisible by 3"));
        }

        if !is_root {
            let min = header.max_entries / 3;
            if header.nr_entries < min {
                return Err(node_err_s(
                    path,
                    format!(
                        "too few entries {}, expected at least {}",
                        header.nr_entries, min
                    ),
                ));
            }
        }
    }

    let (i, keys) = convert_result(path, count(le_u64, header.nr_entries as usize)(i))?;

    let mut last = None;
    for k in &keys {
        if let Some(l) = last {
            if k <= l {
                return Err(node_err(path, "keys out of order"));
            }
        }

        last = Some(k);
    }

    let nr_free = header.max_entries - header.nr_entries;
    let (i, _padding) = convert_result(path, count(le_u64, nr_free as usize)(i))?;

    if header.is_leaf {
        let (_i, values) = convert_result(path, count(V::unpack, header.nr_entries as usize)(i))?;

        Ok(Node::Leaf {
            header,
            keys,
            values,
        })
    } else {
        let (_i, values) = convert_result(path, count(le_u64, header.nr_entries as usize)(i))?;
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
    fn visit(
        &self,
        path: &Vec<u64>,
        keys: &KeyRange,
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
    _path: &mut Vec<u64>,
    engine: Arc<dyn IoEngine + Send + Sync>,
    ignore_non_fatal: bool,
    root: u64,
) -> Result<BTreeMap<u64, V>> {
    let walker = BTreeWalker::new(engine, ignore_non_fatal);
    let visitor = ValueCollector::<V>::new();
    let mut path = Vec::new();
    walker.walk(&mut path, &visitor, root)?;
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
