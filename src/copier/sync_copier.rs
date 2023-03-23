use anyhow::{anyhow, Context, Result};
use roaring::RoaringBitmap;
use std::fs::File;
use std::fs::OpenOptions;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::sync::mpsc;
use std::sync::{Arc, Mutex, RwLock};
use std::thread;

use crate::copier::*;
use crate::io_engine::buffer::*;
use crate::io_engine::is_page_aligned;
use crate::io_engine::utils::*;

#[cfg(test)]
mod tests;

//-------------------------------------

pub struct SyncCopier<T: ReadBlocks + WriteBlocks + Send> {
    buffer_size: usize,
    block_size: usize,
    src: Arc<Mutex<T>>,
    src_offset: u64,
    dst: Arc<Mutex<T>>,
    dst_offset: u64,
}

#[derive(Clone)]
struct Op {
    block_begin: Block,
    block_end: Block,
    indexes: Vec<usize>,
}

fn aggregate_ops(ops: &[Op]) -> Vec<Op> {
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

impl<T: ReadBlocks + WriteBlocks + Send> SyncCopier<T> {
    pub fn new(buffer_size: usize, block_size: usize, src: T, dst: T) -> Result<SyncCopier<T>> {
        if block_size > buffer_size {
            return Err(anyhow!("buffer size too small"));
        }

        Ok(Self {
            buffer_size,
            block_size,
            src: Arc::new(Mutex::new(src)),
            src_offset: 0,
            dst: Arc::new(Mutex::new(dst)),
            dst_offset: 0,
        })
    }

    // Copying regions within a file
    pub fn in_file(buffer_size: usize, block_size: usize, in_out: T) -> Result<SyncCopier<T>> {
        if block_size > buffer_size {
            return Err(anyhow!("buffer size too small"));
        }

        let dst = Arc::new(Mutex::new(in_out));

        // src and dst are behind the same mutex, thus the worker threads
        // cannot read and write simultaneously. It could be useful while
        // dealing with spindle devices.
        Ok(Self {
            buffer_size,
            block_size,
            src: dst.clone(),
            src_offset: 0,
            dst,
            dst_offset: 0,
        })
    }

    pub fn src_offset(mut self, offset: u64) -> Result<SyncCopier<T>> {
        if !is_page_aligned(offset) {
            return Err(anyhow!("offset must be page aligned"));
        }
        self.src_offset = offset;
        Ok(self)
    }

    pub fn dest_offset(mut self, offset: u64) -> Result<SyncCopier<T>> {
        if !is_page_aligned(offset) {
            return Err(anyhow!("offset must be page aligned"));
        }
        self.dst_offset = offset;
        Ok(self)
    }

    // Returns a bitset that indicates whether the read succeeded for
    // that op (true == succeeded).
    fn do_reads(
        src: &Arc<Mutex<T>>,
        offset: u64,
        block_size: usize,
        ops: &[CopyOp],
        buffer: &mut [u8],
    ) -> RoaringBitmap {
        let mut success_bits = RoaringBitmap::new();

        // Split the buffer up into block size chunks
        let mut bufs: Vec<Option<&mut [u8]>> = buffer.chunks_mut(block_size).map(Some).collect();

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
            let results = src.read_blocks(&mut iovec[..], pos);

            // check results
            #[allow(clippy::single_match)]
            match results {
                Ok(results) => {
                    for (i, v) in results.iter().enumerate() {
                        let index = op.indexes[i];
                        if v.is_ok() {
                            success_bits.insert(index as u32);
                        }
                    }
                }
                Err(_) => {
                    // error everything, this is implicit since success_bits is empty.
                }
            }
        }
        success_bits
    }

    fn do_writes(
        dst: &Arc<Mutex<T>>,
        offset: u64,
        block_size: usize,
        ops: &[CopyOp],
        select_bits: &RoaringBitmap,
        buffer: &[u8],
    ) -> RoaringBitmap {
        let mut success_bits = RoaringBitmap::new();

        // Split the buffer up into block size chunks
        let mut bufs: Vec<Option<&[u8]>> = buffer.chunks(block_size).map(Some).collect();

        let mut writes = Vec::with_capacity(ops.len());
        for (buffer_index, op) in ops.iter().enumerate() {
            // Only write blocks where the read succeeded previously
            if select_bits.contains(buffer_index as u32) {
                writes.push(Op {
                    block_begin: op.dst,
                    block_end: op.dst + 1,
                    indexes: vec![buffer_index],
                });
            }
        }

        // sort by block_begin so the IO will be ordered.
        writes.sort_by(|lhs, rhs| lhs.block_begin.cmp(&rhs.block_begin));

        // Aggregate adjacent io
        let writes = aggregate_ops(&writes);

        // issue the io
        let dst = dst.lock().unwrap();
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
            let results = dst.write_blocks(&iovec[..], pos);

            // check results
            #[allow(clippy::single_match)]
            match results {
                Ok(results) => {
                    for (i, v) in results.iter().enumerate() {
                        let index = op.indexes[i];
                        if v.is_ok() {
                            success_bits.insert(index as u32);
                        }
                    }
                }
                Err(_) => {
                    // error everything, this is implicit since success_bits is empty.
                }
            }
        }
        success_bits
    }
}

impl<T: ReadBlocks + WriteBlocks + From<File> + Send> SyncCopier<T> {
    pub fn from_path<P: AsRef<Path>>(
        buffer_size: usize,
        block_size: usize,
        src: P,
        dst: P,
    ) -> Result<SyncCopier<T>> {
        // must be a multiple of page size because we use O_DIRECT
        if !is_page_aligned(block_size as u64) || !is_page_aligned(buffer_size as u64) {
            return Err(anyhow!("block size must be page aligned"));
        }

        let src_file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_EXCL | libc::O_DIRECT)
            .open(src)?;

        let dst_file = OpenOptions::new()
            .read(false)
            .write(true)
            .custom_flags(libc::O_EXCL | libc::O_DIRECT)
            .open(dst)?;

        SyncCopier::<T>::new(buffer_size, block_size, src_file.into(), dst_file.into())
    }
}

impl<T: ReadBlocks + WriteBlocks + Send + 'static> Copier for SyncCopier<T> {
    fn copy(
        &mut self,
        ops: &[CopyOp],
        progress: Arc<dyn CopyProgress + Sync + Send>,
    ) -> Result<CopyStats> {
        let stats = Arc::new(RwLock::new(CopyStats::new(ops.len() as u64)));

        // kick off the writer thread.
        let (tx, rx) = mpsc::sync_channel::<(Vec<CopyOp>, RoaringBitmap, Buffer)>(1);
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

                let (ops, read_success, buffer) = msg.unwrap();
                let write_success = SyncCopier::do_writes(
                    &dst,
                    offset,
                    block_size,
                    &ops,
                    &read_success,
                    buffer.get_data(),
                );

                if write_success.len() != read_success.len() {
                    let mut stats = stats.write().unwrap();
                    for (i, op) in ops.iter().enumerate() {
                        if read_success.contains(i as u32) && !write_success.contains(i as u32) {
                            stats.write_errors.push(*op);
                        }
                    }
                }

                let mut stats = stats.write().unwrap();
                stats.nr_copied += write_success.len();
                progress.update(&stats);
            })
        };

        // read loop
        let chunk_size = self.buffer_size / self.block_size;
        for chunk in ops.chunks(chunk_size) {
            let buffer_size = chunk.len() * self.block_size;
            let ops: Vec<CopyOp> = chunk.to_vec();
            let buffer = Buffer::new(buffer_size, 4096);
            let read_success = Self::do_reads(
                &self.src,
                self.src_offset,
                self.block_size,
                &ops,
                buffer.get_data(),
            );

            if read_success.len() as u32 != ops.len() as u32 {
                // We had some errors, iterate through to find which ones.
                let mut stats = stats.write().unwrap();
                for (i, op) in ops.iter().enumerate() {
                    if !read_success.contains(i as u32) {
                        stats.read_errors.push(*op);
                    }
                }
            }

            tx.send((ops, read_success, buffer))
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
