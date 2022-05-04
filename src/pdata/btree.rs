use anyhow::anyhow;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use data_encoding::BASE64;
use nom::{number::complete::*, IResult};
use std::fmt;
use std::io;
use thiserror::Error;

use crate::io_engine::*;
use crate::pack::vm;
use crate::pdata::unpack::*;

#[cfg(test)]
mod tests;

//------------------------------------------

#[derive(Clone, Debug, PartialEq)]
pub struct KeyRange {
    pub start: Option<u64>,
    pub end: Option<u64>, // This is the one-past-the-end value
}

impl KeyRange {
    pub fn new() -> KeyRange {
        KeyRange {
            start: None,
            end: None,
        }
    }
}

impl Default for KeyRange {
    fn default() -> Self {
        Self::new()
    }
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

fn split_one(path: &[u64], kr: &KeyRange, k: u64) -> Result<(KeyRange, KeyRange)> {
    match kr.split(k) {
        None => Err(node_err(
            path,
            &format!("couldn't split key range {} at {}", kr, k),
        )),
        Some(pair) => Ok(pair),
    }
}

pub fn split_key_ranges(path: &[u64], kr: &KeyRange, keys: &[u64]) -> Result<Vec<KeyRange>> {
    let mut krs = Vec::with_capacity(keys.len());

    if keys.is_empty() {
        return Err(node_err(path, "split_key_ranges: no keys present"));
    }

    // The first key gives the lower bound
    let mut kr = KeyRange {
        start: Some(keys[0]),
        end: kr.end,
    };

    for k in keys.iter().skip(1) {
        let (first, rest) = split_one(path, &kr, *k)?;
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
    if !path.is_empty() && path[0] == 0 {
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
    let mut path = if count == 0 {
        vec![]
    } else {
        let mut output = Vec::with_capacity(count * 8);
        let mut cursor = std::io::Cursor::new(&mut output);

        let mut vm = vm::VM::new();
        let written = vm.exec(&mut input, &mut cursor, count * 8)?;
        assert_eq!(written, count * 8);

        let mut cursor = std::io::Cursor::new(&mut output);
        vm::unpack_u64s(&mut cursor, count)?
    };

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

pub trait AnyError: BoxedClone + fmt::Debug {}

// A helper trait that clones Boxed trait object
pub trait BoxedClone {
    fn boxed_clone(&self) -> Box<dyn AnyError + Send + Sync>;
}

impl<T: 'static + AnyError + Clone + Send + Sync> BoxedClone for T {
    fn boxed_clone(&self) -> Box<dyn AnyError + Send + Sync> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn AnyError + Send + Sync> {
    fn clone(&self) -> Box<dyn AnyError + Send + Sync> {
        self.boxed_clone()
    }
}

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

    Other(Box<dyn AnyError + Send + Sync>),
}

impl<T: 'static + AnyError + Send + Sync> From<T> for BTreeError {
    fn from(t: T) -> Self {
        BTreeError::Other(Box::new(t))
    }
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
            BTreeError::Other(e) => write!(f, "{:?}", e),
        }
    }
}

pub fn node_err(path: &[u64], msg: &str) -> BTreeError {
    BTreeError::Path(
        path.to_vec(),
        Box::new(BTreeError::NodeError(msg.to_string())),
    )
}

pub fn node_err_s(path: &[u64], msg: String) -> BTreeError {
    BTreeError::Path(path.to_vec(), Box::new(BTreeError::NodeError(msg)))
}

pub fn io_err(path: &[u64]) -> BTreeError {
    BTreeError::Path(path.to_vec(), Box::new(BTreeError::IoError))
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

#[derive(Debug, Clone, Copy, PartialEq)]
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

impl Pack for NodeHeader {
    fn pack<W: WriteBytesExt>(&self, w: &mut W) -> io::Result<()> {
        // csum needs to be calculated right for the whole metadata block.
        w.write_u32::<LittleEndian>(0)?;

        let flags = if self.is_leaf {
            LEAF_NODE
        } else {
            INTERNAL_NODE
        };
        w.write_u32::<LittleEndian>(flags)?;
        w.write_u64::<LittleEndian>(self.block)?;
        w.write_u32::<LittleEndian>(self.nr_entries)?;
        w.write_u32::<LittleEndian>(self.max_entries)?;
        w.write_u32::<LittleEndian>(self.value_size)?;
        w.write_u32::<LittleEndian>(0)
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

    fn get_mut_header(&mut self) -> &mut NodeHeader {
        use Node::*;
        match self {
            Internal { header, .. } => header,
            Leaf { header, .. } => header,
        }
    }

    pub fn get_keys(&self) -> &[u64] {
        use Node::*;
        match self {
            Internal { keys, .. } => &keys[0..],
            Leaf { keys, .. } => &keys[0..],
        }
    }

    pub fn set_block(&mut self, b: u64) {
        self.get_mut_header().block = b;
    }
}

pub fn convert_result<'a, V>(path: &[u64], r: IResult<&'a [u8], V>) -> Result<(&'a [u8], V)> {
    r.map_err(|_e| node_err(path, "parse error"))
}

pub fn convert_io_err<V>(path: &[u64], r: std::io::Result<V>) -> Result<V> {
    r.map_err(|_| io_err(path))
}

pub fn unpack_node<V: Unpack>(
    path: &[u64],
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
                return Err(node_err(
                    path,
                    &format!("keys out of order: {} <= {}", k, l),
                ));
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

/// Pack the given node ready to write to disk.
pub fn pack_node<W: WriteBytesExt, V: Pack + Unpack>(node: &Node<V>, w: &mut W) -> io::Result<()> {
    match node {
        Node::Internal {
            header,
            keys,
            values,
        } => {
            header.pack(w)?;
            for k in keys {
                w.write_u64::<LittleEndian>(*k)?;
            }

            // pad with zeroes
            for _i in keys.len()..header.max_entries as usize {
                w.write_u64::<LittleEndian>(0)?;
            }

            for v in values {
                v.pack(w)?;
            }
        }
        Node::Leaf {
            header,
            keys,
            values,
        } => {
            header.pack(w)?;
            for k in keys {
                w.write_u64::<LittleEndian>(*k)?;
            }

            // pad with zeroes
            for _i in keys.len()..header.max_entries as usize {
                w.write_u64::<LittleEndian>(0)?;
            }

            for v in values {
                v.pack(w)?;
            }
        }
    }

    Ok(())
}

//------------------------------------------

pub fn calc_max_entries<V: Unpack>() -> usize {
    let elt_size = 8 + V::disk_size() as usize;
    let total = ((BLOCK_SIZE - NodeHeader::disk_size() as usize) / elt_size) as usize;
    total / 3 * 3
}

//------------------------------------------
