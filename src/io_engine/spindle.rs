use anyhow::{anyhow, Context, Result};
use roaring::RoaringBitmap;
use std::alloc::{alloc, dealloc, Layout};
use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::{self, Read, Seek};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::sync::mpsc;
use std::thread;

use crate::checksum::*;
use crate::io_engine::*;
use crate::pack::node_encode::*;
use crate::run_iter::*;

//------------------------------------------

/// Examining BTrees can lead to a lot of random io, which can be
/// very slow on spindle devices.  This io engine reads in all
/// metadata blocks of interest (we know these from the metadata
/// space map), compresses them and caches them in memory.  This
/// greatly speeds up performance but obviously uses a lot of memory.
/// Writes are not supported.

pub struct SpindleIoEngine {
    nr_blocks: u64,
    compressed: BTreeMap<u32, Vec<u8>>,
}

// Because we use O_DIRECT we need to use page aligned blocks.  Buffer
// manages allocation of this aligned memory.
struct Buffer {
    size: usize,
    align: usize,
    data: *mut u8,
}

impl Buffer {
    fn new(size: usize, align: usize) -> Self {
        let layout = Layout::from_size_align(size, align).unwrap();
        let ptr = unsafe { alloc(layout) };
        assert!(!ptr.is_null(), "out of memory");

        Self {
            size,
            align,
            data: ptr,
        }
    }

    pub fn get_data<'a>(&self) -> &'a mut [u8] {
        unsafe { std::slice::from_raw_parts_mut::<'a>(self.data, self.size) }
    }
}

impl Drop for Buffer {
    fn drop(&mut self) {
        let layout = Layout::from_size_align(self.size, self.align).unwrap();
        unsafe {
            dealloc(self.data, layout);
        }
    }
}

unsafe impl Send for Buffer {}
unsafe impl Sync for Buffer {}

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
    let data = crate::pack::vm::unpack(&mut c, BLOCK_SIZE as usize)?;
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
    loop {
        if let Ok((first_block, buffer)) = rx.recv() {
            let chunk = buffer.get_data();
            // eprintln!("processing chunk starting at block {}", first_block);
            pack_chunk(first_block, chunk, &mut compressed).expect("pack chunk failed");
        } else {
            break;
        }
    }

    result_tx
        .send(compressed)
        .expect("packer thread couldn't send result");
}

impl SpindleIoEngine {
    pub fn new(path: &Path, blocks: RoaringBitmap, excl: bool) -> Result<Self> {
        let nr_blocks = get_nr_blocks(path)?;
        let mut input = OpenOptions::new()
            .read(true)
            .custom_flags(if excl {
                libc::O_EXCL | libc::O_DIRECT
            } else {
                libc::O_DIRECT
            })
            .open(path)?;

        let (tx, rx) = mpsc::channel::<(u64, Buffer)>();
        let (result_tx, result_rx) = mpsc::channel::<BTreeMap<u32, Vec<u8>>>();

        let thread = thread::spawn(move || packer_thread(rx, result_tx));

        for (present, mut range) in RunIter::new(blocks, nr_blocks as u32) {
            if !present {
                input.seek(std::io::SeekFrom::Current((range.len() * BLOCK_SIZE) as i64))?;
            } else {
                while !range.is_empty() {
                    let len = std::cmp::min(range.len(), 16 * 1024);  // Max 64M buffer
                    let buffer = Buffer::new(len * BLOCK_SIZE, 4096);
                    let chunk = buffer.get_data();
                    input.read_exact(chunk)?;
                    tx.send((range.start as u64, buffer))?;
                    range.start += len as u32;
                }
            }
        }

        // FIXME: handle partial chunk at end

        drop(tx);
        let compressed = result_rx.recv()?;
        thread.join().expect("chunk reader thread panicked");

        Ok(Self {
            nr_blocks,
            compressed,
        })
    }

    fn read_(&self, loc: u64) -> io::Result<Block> {
        if let Some(z) = self.compressed.get(&(loc as u32)) {
            unpack_block(z, loc).map_err(|_| io::Error::new(io::ErrorKind::Other, "unpack failed"))
        } else {
            todo!();
        }
    }
}

impl IoEngine for SpindleIoEngine {
    fn get_nr_blocks(&self) -> u64 {
        self.nr_blocks
    }

    fn get_batch_size(&self) -> usize {
        1
    }

    fn read(&self, loc: u64) -> io::Result<Block> {
        self.read_(loc)
    }

    fn read_many(&self, blocks: &[u64]) -> io::Result<Vec<io::Result<Block>>> {
        let mut bs = Vec::new();
        for b in blocks {
            bs.push(self.read_(*b));
        }
        Ok(bs)
    }

    fn write(&self, _b: &Block) -> io::Result<()> {
        todo!();
    }

    fn write_many(&self, _blocks: &[Block]) -> io::Result<Vec<io::Result<()>>> {
        todo!();
    }
}

//------------------------------------------
