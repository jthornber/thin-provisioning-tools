use anyhow::anyhow;
use byteorder::{ReadBytesExt, WriteBytesExt};
use data_encoding::BASE64;
use std::fmt;
use thiserror::Error;

use crate::pack::vm;

//------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq)]
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
        None => Err(context_err(
            path,
            &format!("couldn't split key range {} at {}", kr, k),
        )),
        Some(pair) => Ok(pair),
    }
}

pub fn split_key_ranges(path: &[u64], kr: &KeyRange, keys: &[u64]) -> Result<Vec<KeyRange>> {
    let mut krs = Vec::with_capacity(keys.len());

    if keys.is_empty() {
        return Err(context_err(path, "split_key_ranges: no keys present"));
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
        cursor.write_u8(count).unwrap();
        vm::pack_u64s(&mut cursor, &path[1..]).unwrap();
    } else {
        let count = ((path.len() as u8) << 1) | 1;
        cursor.write_u8(count).unwrap();
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

#[derive(Clone, Debug)]
pub enum NodeError {
    IoError,
    NotANode,
    ChecksumError,
    BlockNrMismatch,
    ValueSizeMismatch,
    MaxEntriesTooLarge,
    MaxEntriesNotDivisible,
    NumEntriesTooLarge,
    NumEntriesTooSmall, // i.e., underfull
    KeysOutOfOrder,
    IncompleteData,
}

impl fmt::Display for NodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use NodeError::*;

        match self {
            IoError => write!(f, "io error"),
            NotANode => write!(f, "not a btree node"),
            ChecksumError => write!(f, "checksum error"),
            BlockNrMismatch => write!(f, "blocknr mismatch"),
            ValueSizeMismatch => write!(f, "value_size mismatch"),
            MaxEntriesTooLarge => write!(f, "max_entries is too large"),
            MaxEntriesNotDivisible => write!(f, "max_entries is not divisible by 3"),
            NumEntriesTooLarge => write!(f, "nr_entries > max_entries"),
            NumEntriesTooSmall => write!(f, "nr_entries < max_entries / 3"),
            KeysOutOfOrder => write!(f, "keys out of order"),
            IncompleteData => write!(f, "incomplete data"),
        }
    }
}

#[derive(Error, Clone, Debug)]
pub enum BTreeError {
    // #[error("node error: {0}")]
    NodeError(NodeError),

    // #[error("value error: {0}")]
    ValueError(String),

    ContextError(String),

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
            BTreeError::NodeError(e) => write!(f, "node error: {}", e),
            BTreeError::ValueError(msg) => write!(f, "value error: {}", msg),
            BTreeError::ContextError(msg) => write!(f, "context error: {}", msg),
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

pub fn node_err(path: &[u64], e: NodeError) -> BTreeError {
    BTreeError::Path(path.to_vec(), Box::new(BTreeError::NodeError(e)))
}

pub fn io_err(path: &[u64]) -> BTreeError {
    BTreeError::Path(
        path.to_vec(),
        Box::new(BTreeError::NodeError(NodeError::IoError)),
    )
}

pub fn context_err(path: &[u64], msg: &str) -> BTreeError {
    BTreeError::Path(
        path.to_vec(),
        Box::new(BTreeError::ContextError(msg.to_string())),
    )
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
