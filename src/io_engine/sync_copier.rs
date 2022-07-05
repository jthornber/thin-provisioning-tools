use anyhow::{anyhow, Context, Result};
use std::fs::File;
use std::fs::OpenOptions;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::sync::mpsc;
use std::sync::{Arc, Mutex, RwLock};
use std::sync::atomic::Ordering;
use std::thread;

use crate::io_engine::buffer::*;
use crate::io_engine::copier::*;
use crate::io_engine::utils::*;

//-------------------------------------

pub struct SyncCopier {
    buffer_size: usize,
    block_size: usize,
    src: Arc<Mutex<File>>,
    src_offset: u64,
    dst: Arc<Mutex<File>>,
    dst_offset: u64,
}

#[derive(Clone)]
struct Op {
    block_begin: Block,
    block_end: Block,
    indexes: Vec<usize>,
}

fn aggregate_ops<'a>(ops: &[Op]) -> Vec<Op> {
    let mut r = Vec::with_capacity(ops.len());

    let mut last: Option<Op> = None;
    for op in ops {
        if let Some(mut l) = last.take() {
            if l.block_end == op.block_begin {
                l.block_end = op.block_end;
                l.indexes.append(&mut op.indexes.clone());
                last = Some(l);
            } else {
                r.push(l);
                last = Some(op.clone());
            }
        } else {
            last = Some(op.clone());
        }
    }

    if let Some(last) = last {
        r.push(last);
    }

    r
}

impl SyncCopier {
    pub fn new<P: AsRef<Path>>(
        buffer_size: usize,
        block_size: usize,
        src: P,
        src_offset: u64,
        dst: P,
        dst_offset: u64,
    ) -> Result<Self> {
        // must be a multiple of page size because we use O_DIRECT
        assert_eq!(buffer_size % 4096, 0);
        assert_eq!(block_size % 4096, 0);
        assert_eq!(src_offset % 4096, 0);
        assert_eq!(dst_offset % 4096, 0);
        assert!(block_size < buffer_size);

        let src = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_EXCL | libc::O_DIRECT)
            .open(src.as_ref())?;

        let dst = OpenOptions::new()
            .read(false)
            .write(true)
            .custom_flags(libc::O_EXCL | libc::O_DIRECT)
            .open(dst.as_ref())?;

        Ok(Self {
            buffer_size: buffer_size / 2,
            block_size,
            src: Arc::new(Mutex::new(src)),
            src_offset,
            dst: Arc::new(Mutex::new(dst)),
            dst_offset,
        })
    }

    // Returns (success, failed) ops.
    fn do_reads(
        src: &Arc<Mutex<File>>,
        offset: u64,
        block_size: usize,
        ops: &[CopyOp],
        buffer: &mut [u8],
    ) -> (Vec<CopyOp>, Vec<CopyOp>) {
        // Split the buffer up into block size chunks
        let mut bufs: Vec<Option<&mut [u8]>> =
            buffer.chunks_mut(block_size).map(|buf| Some(buf)).collect();

        let mut reads = Vec::with_capacity(ops.len());
        for (buffer_index, op) in ops.iter().enumerate() {
            reads.push(Op {
                block_begin: op.src,
                block_end: op.src + 1,
                indexes: vec![buffer_index],
            });
        }

        // sort by block_begin so the IO will be ordered.
        reads.sort_by(|lhs, rhs| lhs.block_begin.cmp(&rhs.block_begin));

        // Aggregate adjacent io
        let reads = aggregate_ops(&reads);

        // issue the io
        let src = src.lock().unwrap();
        let mut good = Vec::with_capacity(ops.len());
        let mut bad = Vec::new();
        for op in reads {
            // build io vec
            let mut iovec: Vec<&mut [u8]> = Vec::with_capacity(op.indexes.len());
            for i in 0..(op.block_end - op.block_begin) {
                let buf_index = op.indexes[i as usize];
                let data = bufs[buf_index].take();
                iovec.push(data.unwrap());
            }

            // issue io
            let pos = (op.block_begin * block_size as u64) + offset;
            let results = read_blocks(&*src, &mut iovec[..], pos);

            // check results
            match results {
                Ok(results) => {
                    for (i, v) in results.iter().enumerate() {
                        let index = op.indexes[i as usize];
                        let cop = ops[index].clone();
                        if v.is_err() {
                            bad.push(cop);
                        } else {
                            good.push(cop);
                        }
                    }
                }
                Err(_) => {
                    // error everything
                    for i in 0..op.indexes.len() {
                        let index = op.indexes[i];
                        let cop = ops[index].clone();
                        bad.push(cop);
                    }
                }
            }
        }
        (good, bad)
    }

    fn do_writes(
        dst: &Arc<Mutex<File>>,
        offset: u64,
        block_size: usize,
        ops: &[CopyOp],
        buffer: &[u8],
    ) -> (Vec<CopyOp>, Vec<CopyOp>) {
        // Split the buffer up into block size chunks
        let mut bufs: Vec<Option<&[u8]>> = buffer.chunks(block_size).map(|buf| Some(buf)).collect();

        let mut writes = Vec::with_capacity(ops.len());
        for (buffer_index, op) in ops.iter().enumerate() {
            writes.push(Op {
                block_begin: op.dst,
                block_end: op.dst + 1,
                indexes: vec![buffer_index],
            });
        }

        // sort by block_begin so the IO will be ordered.
        writes.sort_by(|lhs, rhs| lhs.block_begin.cmp(&rhs.block_begin));

        // Aggregate adjacent io
        let writes = aggregate_ops(&writes);

        // issue the io
        let dst = dst.lock().unwrap();
        let mut good = Vec::with_capacity(ops.len());
        let mut bad = Vec::new();
        for op in writes {
            // build io vec
            let mut iovec: Vec<&[u8]> = Vec::with_capacity(op.indexes.len());
            for i in 0..(op.block_end - op.block_begin) {
                let buf_index = op.indexes[i as usize];
                let data = bufs[buf_index].take();
                iovec.push(data.unwrap());
            }

            // issue io
            let pos = (op.block_begin * block_size as u64) + offset;
            let results = write_blocks(&*dst, &iovec[..], pos);

            // check results
            match results {
                Ok(results) => {
                    for (i, v) in results.iter().enumerate() {
                        let index = op.indexes[i as usize];
                        let cop = ops[index].clone();
                        if v.is_err() {
                            bad.push(cop);
                        } else {
                            good.push(cop);
                        }
                    }
                }
                Err(_) => {
                    // error everything
                    for i in 0..op.indexes.len() {
                        let index = op.indexes[i];
                        let cop = ops[index].clone();
                        bad.push(cop);
                    }
                }
            }
        }
        (good, bad)
    }
}

impl Copier for SyncCopier {
    fn copy(
        &mut self,
        ops: &[CopyOp],
        progress: Arc<dyn CopyProgress + Sync + Send>,
    ) -> Result<CopyStats> {
        let stats = Arc::new(RwLock::new(CopyStats::new(ops.len() as u64)));

        // kick off the writer thread.
        let (tx, rx) = mpsc::sync_channel::<(Vec<CopyOp>, Buffer)>(1);
        let write_thread = {
            let stats = stats.clone();
            let dst = self.dst.clone();
            let offset = self.dst_offset;
            let block_size = self.block_size;
            thread::spawn(move || loop {
                let msg = rx.recv();
                if msg.is_err() {
                    break;
                }

                let (ops, buffer) = msg.unwrap();
                let (_, mut bad) =
                    SyncCopier::do_writes(&dst, offset, block_size, &ops, buffer.get_data());

                if !bad.is_empty() {
                    let mut stats = stats.write().unwrap();
                    stats.write_errors.append(&mut bad);
                }

                let stats = stats.read().unwrap();
                stats.nr_copied.fetch_add(ops.len() as u64, Ordering::SeqCst);
                progress.update(&*stats);
            })
        };

        // read loop
        let chunk_size = self.buffer_size / self.block_size;
        for chunk in ops.chunks(chunk_size as usize) {
            let buffer_size = chunk.len() * self.block_size as usize;
            let ops: Vec<CopyOp> = chunk.iter().cloned().collect();
            let buffer = Buffer::new(buffer_size, 4096);
            let (good, mut bad) = SyncCopier::do_reads(
                &self.src,
                self.src_offset,
                self.block_size,
                &ops,
                buffer.get_data(),
            );

            if !bad.is_empty() {
                let mut stats = stats.write().unwrap();
                stats.read_errors.append(&mut bad);
            }

            tx.send((good, buffer))
                .context("error when sending io ops to writer thread")?;
        }

        drop(tx);
        write_thread
            .join()
            .map_err(|_| anyhow!("error joining with writer thread"))?;

        Ok(Arc::try_unwrap(stats).unwrap().into_inner().unwrap())
    }
}

//-------------------------------------
