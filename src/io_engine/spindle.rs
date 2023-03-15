use anyhow::{anyhow, Context, Result};
use roaring::RoaringBitmap;
use std::collections::BTreeMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{self, Read, Seek};
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::prelude::FileExt;
use std::path::Path;
use std::sync::mpsc;
use std::sync::RwLock;
use std::thread;

use crate::checksum::*;
use crate::io_engine::buffer::*;
use crate::io_engine::*;
use crate::pack::node_encode::*;
use crate::run_iter::*;

//------------------------------------------

/// Examining BTrees can lead to a lot of random io, which can be
/// very slow on spindle devices.  This io engine reads in all
/// metadata blocks of interest (we know these from the metadata
/// space map), compresses them and caches them in memory.  This
/// greatly speeds up performance but obviously uses a lot of memory.
/// Writes or reads to blocks not in the space map fall back to sync io.

//------------------------------------------

fn pack_block<W: io::Write>(w: &mut W, kind: BT, buf: &[u8]) -> Result<()> {
    match kind {
        BT::THIN_SUPERBLOCK | BT::CACHE_SUPERBLOCK | BT::ERA_SUPERBLOCK => {
            pack_superblock(w, buf).context("unable to pack superblock")?
        }
        BT::NODE => pack_btree_node(w, buf).context("unable to pack btree node")?,
        BT::INDEX => pack_index(w, buf).context("unable to pack space map index")?,
        BT::BITMAP => pack_bitmap(w, buf).context("unable to pack space map bitmap")?,
        BT::ARRAY => pack_array(w, buf).context("unable to pack array block")?,
        BT::UNKNOWN => return Err(anyhow!("asked to pack an unknown block type")),
    }

    Ok(())
}

fn unpack_block(z: &[u8], loc: u64) -> Result<Block> {
    // FIXME: remove this copy
    let b = Block::new(loc);
    let mut c = std::io::Cursor::new(z);
    let data = crate::pack::vm::unpack(&mut c, BLOCK_SIZE)?;
    unsafe {
        std::ptr::copy(data.as_ptr(), b.get_raw_ptr(), BLOCK_SIZE);
    }
    Ok(b)
}

fn pack_chunk(
    first_block: u64,
    chunk: &[u8],
    compressed: &mut BTreeMap<u32, Vec<u8>>,
) -> Result<u64> {
    let mut total_packed = 0u64;

    for b in 0..(chunk.len() / BLOCK_SIZE) {
        let block = first_block + b as u64;

        let offset = b * BLOCK_SIZE;
        let data = &chunk[offset..(offset + BLOCK_SIZE)];
        let kind = metadata_block_type(data);
        if kind != BT::UNKNOWN {
            let mut packed = Vec::with_capacity(64);
            pack_block(&mut packed, kind, data)?;
            total_packed += packed.len() as u64;

            compressed.insert(block as u32, packed);
        }
    }

    Ok(total_packed)
}

fn packer_thread(
    rx: mpsc::Receiver<(u64, Buffer)>,
    result_tx: mpsc::Sender<BTreeMap<u32, Vec<u8>>>,
) {
    let mut compressed = BTreeMap::new();
    while let Ok((first_block, buffer)) = rx.recv() {
        let chunk = buffer.get_data();
        pack_chunk(first_block, chunk, &mut compressed).expect("pack chunk failed");
    }

    result_tx
        .send(compressed)
        .expect("packer thread couldn't send result");
}

//------------------------------------------

struct SpindleIoEngine_ {
    nr_blocks: u64,
    compressed: BTreeMap<u32, Vec<u8>>,
    input: File,
}

impl SpindleIoEngine_ {
    pub fn new<P: AsRef<Path>>(path: P, blocks: RoaringBitmap, excl: bool) -> Result<Self> {
        let nr_blocks = get_nr_blocks(path.as_ref())?;
        let mut input = OpenOptions::new()
            .read(true)
            .custom_flags(if excl {
                libc::O_EXCL | libc::O_DIRECT
            } else {
                libc::O_DIRECT
            })
            .open(path.as_ref())?;

        let (tx, rx) = mpsc::channel::<(u64, Buffer)>();
        let (result_tx, result_rx) = mpsc::channel::<BTreeMap<u32, Vec<u8>>>();

        let thread = thread::spawn(move || packer_thread(rx, result_tx));

        for (present, mut range) in RunIter::new(blocks, nr_blocks as u32) {
            if !present {
                input.seek(std::io::SeekFrom::Current(
                    (range.len() * BLOCK_SIZE) as i64,
                ))?;
            } else {
                while !range.is_empty() {
                    let len = std::cmp::min(range.len(), 16 * 1024); // Max 64M buffer
                    let buffer = Buffer::new(len * BLOCK_SIZE, 4096);
                    input.read_exact(buffer.get_data())?;
                    tx.send((range.start as u64, buffer))?;
                    range.start += len as u32;
                }
            }
        }

        drop(tx);
        let compressed = result_rx.recv()?;
        thread.join().expect("chunk reader thread panicked");

        Ok(Self {
            nr_blocks,
            compressed,
            input,
        })
    }

    fn read_(&self, loc: u64) -> io::Result<Block> {
        if let Some(z) = self.compressed.get(&(loc as u32)) {
            unpack_block(z, loc).map_err(|_| io::Error::new(io::ErrorKind::Other, "unpack failed"))
        } else {
            let b = Block::new(loc);
            self.input
                .read_exact_at(b.get_data(), loc * BLOCK_SIZE as u64)?;
            Ok(b)
        }
    }

    fn write_(&mut self, b: &Block) -> io::Result<()> {
        self.compressed.remove(&(b.loc as u32));
        self.input
            .write_all_at(b.get_data(), b.loc * BLOCK_SIZE as u64)
    }
}

//------------------------------------------

pub struct SpindleIoEngine {
    inner: RwLock<SpindleIoEngine_>,
}

impl SpindleIoEngine {
    pub fn new<P: AsRef<Path>>(path: P, blocks: RoaringBitmap, excl: bool) -> Result<Self> {
        Ok(Self {
            inner: RwLock::new(SpindleIoEngine_::new(path, blocks, excl)?),
        })
    }
}

impl IoEngine for SpindleIoEngine {
    fn get_nr_blocks(&self) -> u64 {
        let inner = self.inner.read().unwrap();
        inner.nr_blocks
    }

    fn get_batch_size(&self) -> usize {
        1
    }

    fn suggest_nr_threads(&self) -> usize {
        std::cmp::min(4, num_cpus::get())
    }

    fn read(&self, loc: u64) -> io::Result<Block> {
        let inner = self.inner.read().unwrap();
        inner.read_(loc)
    }

    fn read_many(&self, blocks: &[u64]) -> io::Result<Vec<io::Result<Block>>> {
        let inner = self.inner.read().unwrap();
        let mut bs = Vec::new();
        for b in blocks {
            bs.push(inner.read_(*b));
        }
        Ok(bs)
    }

    fn write(&self, b: &Block) -> io::Result<()> {
        let mut inner = self.inner.write().unwrap();
        inner.write_(b)?;
        Ok(())
    }

    fn write_many(&self, blocks: &[Block]) -> io::Result<Vec<io::Result<()>>> {
        let mut inner = self.inner.write().unwrap();
        let mut bs = Vec::new();
        for b in blocks {
            bs.push(inner.write_(b));
        }
        Ok(bs)
    }
}

//------------------------------------------
