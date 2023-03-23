use anyhow::{anyhow, Context, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use flate2::{read::ZlibDecoder, write::ZlibEncoder, Compression};

use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::{
    fs::OpenOptions,
    io,
    io::prelude::*,
    io::Write,
    ops::DerefMut,
    path::Path,
    sync::{Arc, Mutex},
    thread::spawn,
};

use rand::prelude::*;
use std::sync::mpsc::{sync_channel, Receiver};

use crate::checksum::*;
use crate::file_utils;
use crate::pack::node_encode::*;

const BLOCK_SIZE: u64 = 4096;
const MAGIC: u64 = 0xa537a0aa6309ef77;
const PACK_VERSION: u64 = 3;

fn shuffle<T>(v: &mut Vec<T>) {
    let mut rng = rand::thread_rng();
    v.shuffle(&mut rng);
}

// Each thread processes multiple contiguous runs of blocks, called
// chunks.  Chunks are shuffled so each thread gets chunks spread
// across the dev in case there are large regions that don't contain
// metadata.
fn mk_chunk_vecs(nr_blocks: u64, nr_jobs: u64) -> Vec<Vec<(u64, u64)>> {
    use std::cmp::{max, min};

    let chunk_size = min(4 * 1024u64, max(128u64, nr_blocks / (nr_jobs * 64)));
    let nr_chunks = nr_blocks / chunk_size;
    let mut chunks = Vec::with_capacity(nr_chunks as usize);
    for i in 0..nr_chunks {
        chunks.push((i * chunk_size, (i + 1) * chunk_size));
    }

    // there may be a smaller chunk at the back of the file.
    if nr_chunks * chunk_size < nr_blocks {
        chunks.push((nr_chunks * chunk_size, nr_blocks));
    }

    shuffle(&mut chunks);

    let mut vs = Vec::with_capacity(nr_jobs as usize);
    for _ in 0..nr_jobs {
        vs.push(Vec::new());
    }

    for c in 0..nr_chunks {
        vs[(c % nr_jobs) as usize].push(chunks[c as usize]);
    }

    vs
}

pub fn pack(input_file: &Path, output_file: &Path) -> Result<()> {
    let nr_blocks = get_nr_blocks(input_file)?;
    let nr_jobs = std::cmp::max(1, std::cmp::min(num_cpus::get() as u64, nr_blocks / 128));
    let chunk_vecs = mk_chunk_vecs(nr_blocks, nr_jobs);

    let input = OpenOptions::new()
        .read(true)
        .write(false)
        .custom_flags(libc::O_EXCL)
        .open(input_file)?;

    let output = OpenOptions::new()
        .read(false)
        .write(true)
        .create(true)
        .truncate(true)
        .open(output_file)?;

    write_header(&output, nr_blocks).context("unable to write pack file header")?;

    let sync_input = Arc::new(Mutex::new(input));
    let sync_output = Arc::new(Mutex::new(output));

    let mut threads = Vec::new();
    for job in 0..nr_jobs {
        let sync_input = Arc::clone(&sync_input);
        let sync_output = Arc::clone(&sync_output);
        let chunks = chunk_vecs[job as usize].clone();
        threads.push(spawn(move || crunch(sync_input, sync_output, chunks)));
    }

    for t in threads {
        t.join().unwrap()?;
    }
    Ok(())
}

fn crunch<R, W>(input: Arc<Mutex<R>>, output: Arc<Mutex<W>>, ranges: Vec<(u64, u64)>) -> Result<()>
where
    R: Read + Seek + FileExt,
    W: Write,
{
    let mut written = 0u64;
    let mut z = ZlibEncoder::new(Vec::new(), Compression::default());
    for (lo, hi) in ranges {
        // We read multiple blocks at once to reduce contention
        // on input.
        let mut input = input.lock().unwrap();
        let big_data = read_blocks(input.deref_mut(), lo, hi - lo)?;
        drop(input);

        for b in lo..hi {
            let block_start = ((b - lo) * BLOCK_SIZE) as usize;
            let data = &big_data[block_start..(block_start + BLOCK_SIZE as usize)];
            let kind = metadata_block_type(data);
            if kind != BT::UNKNOWN {
                z.write_u64::<LittleEndian>(b)?;
                pack_block(&mut z, kind, data)?;

                written += 1;
                if written == 1024 {
                    let compressed = z.reset(Vec::new())?;

                    let mut output = output.lock().unwrap();
                    output.write_u64::<LittleEndian>(compressed.len() as u64)?;
                    output.write_all(&compressed)?;
                    written = 0;
                }
            }
        }
    }

    if written > 0 {
        let compressed = z.finish()?;
        let mut output = output.lock().unwrap();
        output.write_u64::<LittleEndian>(compressed.len() as u64)?;
        output.write_all(&compressed)?;
    }

    Ok(())
}

fn write_header<W>(mut w: W, nr_blocks: u64) -> io::Result<()>
where
    W: byteorder::WriteBytesExt,
{
    w.write_u64::<LittleEndian>(MAGIC)?;
    w.write_u64::<LittleEndian>(PACK_VERSION)?;
    w.write_u64::<LittleEndian>(4096)?;
    w.write_u64::<LittleEndian>(nr_blocks)?;

    Ok(())
}

fn read_header<R>(mut r: R) -> io::Result<u64>
where
    R: byteorder::ReadBytesExt,
{
    let magic = r.read_u64::<LittleEndian>()?;
    if magic != MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Not a pack file",
        ));
    }

    let version = r.read_u64::<LittleEndian>()?;
    if version != PACK_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported pack file version ({}).", PACK_VERSION),
        ));
    }

    let block_size = r.read_u64::<LittleEndian>()?;
    if block_size != BLOCK_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("block size is not {}", BLOCK_SIZE),
        ));
    }

    r.read_u64::<LittleEndian>()
}

fn get_nr_blocks(path: &Path) -> io::Result<u64> {
    let len = file_utils::file_size(path)?;
    Ok(len / BLOCK_SIZE)
}

fn read_blocks<R>(rdr: &mut R, b: u64, count: u64) -> io::Result<Vec<u8>>
where
    R: io::Read + io::Seek + FileExt,
{
    let mut buf: Vec<u8> = vec![0; (BLOCK_SIZE * count) as usize];
    rdr.read_exact_at(&mut buf, b * BLOCK_SIZE)?;
    Ok(buf)
}

fn pack_block<W: Write>(w: &mut W, kind: BT, buf: &[u8]) -> Result<()> {
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

fn write_zero_block<W>(w: &mut W, b: u64) -> io::Result<()>
where
    W: Write + Seek + FileExt,
{
    let zeroes: Vec<u8> = vec![0; BLOCK_SIZE as usize];
    w.write_all_at(&zeroes, b * BLOCK_SIZE)?;
    Ok(())
}

fn write_blocks<W>(w: &Arc<Mutex<W>>, blocks: &mut Vec<(u64, Vec<u8>)>) -> io::Result<()>
where
    W: Write + Seek + FileExt,
{
    let w = w.lock().unwrap();
    while let Some((b, block)) = blocks.pop() {
        w.write_all_at(&block, b * BLOCK_SIZE)?;
    }
    Ok(())
}

fn decode_worker<W>(rx: Receiver<Vec<u8>>, w: Arc<Mutex<W>>) -> io::Result<()>
where
    W: Write + Seek + FileExt,
{
    let mut blocks = Vec::new();

    while let Ok(bytes) = rx.recv() {
        let mut z = ZlibDecoder::new(&bytes[0..]);

        while let Ok(b) = z.read_u64::<LittleEndian>() {
            let block = crate::pack::vm::unpack(&mut z, BLOCK_SIZE as usize).unwrap();
            assert!(metadata_block_type(&block[0..]) != BT::UNKNOWN);
            blocks.push((b, block));

            if blocks.len() >= 32 {
                write_blocks(&w, &mut blocks)?;
            }
        }
    }

    write_blocks(&w, &mut blocks)?;
    Ok(())
}

pub fn unpack(input_file: &Path, output_file: &Path) -> Result<()> {
    let mut input = OpenOptions::new()
        .read(true)
        .write(false)
        .open(input_file)?;

    let nr_blocks = read_header(&input)?;

    let mut output = OpenOptions::new()
        .read(false)
        .write(true)
        .create(true)
        .truncate(true)
        .open(output_file)?;

    // zero the last block to size the file
    write_zero_block(&mut output, nr_blocks - 1)?;

    // Run until we hit the end
    let output = Arc::new(Mutex::new(output));

    // kick off the workers
    let nr_jobs = num_cpus::get();
    let mut senders = Vec::new();
    let mut threads = Vec::new();

    for _ in 0..nr_jobs {
        let (tx, rx) = sync_channel(1);
        let output = Arc::clone(&output);
        senders.push(tx);
        threads.push(spawn(move || decode_worker(rx, output)));
    }

    // Read z compressed chunk, and hand to worker thread.
    let mut next_worker = 0;
    while let Ok(len) = input.read_u64::<LittleEndian>() {
        let mut bytes = vec![0; len as usize];
        input.read_exact(&mut bytes)?;
        senders[next_worker].send(bytes).unwrap();
        next_worker = (next_worker + 1) % nr_jobs;
    }

    for s in senders {
        drop(s);
    }

    for t in threads {
        t.join().unwrap()?;
    }
    Ok(())
}
