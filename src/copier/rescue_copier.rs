use anyhow::{anyhow, Result};
use fixedbitset::FixedBitSet;
use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::sync::Arc;

use crate::copier::*;
use crate::io_engine::buffer::*;
use crate::io_engine::{is_page_aligned, PAGE_SHIFT, PAGE_SIZE};

#[cfg(test)]
mod tests;

//-------------------------------------

pub struct RescueCopier<T: FileExt> {
    block_size: usize,
    src: T,
    src_offset: u64,
    dst: T,
    dst_offset: u64,
    buf: Buffer,
    read_success: FixedBitSet,
}

impl<T: FileExt> RescueCopier<T> {
    pub fn new(block_size: usize, src: T, dst: T) -> Result<RescueCopier<T>> {
        // must be a multiple of page size since we'll copy the blocks page-by-page.
        if !is_page_aligned(block_size as u64) {
            return Err(anyhow!("block size must be page aligned"));
        }

        let buf = Buffer::new(block_size, 4096);
        let read_success = FixedBitSet::with_capacity(block_size >> PAGE_SHIFT);

        Ok(Self {
            block_size,
            src,
            src_offset: 0,
            dst,
            dst_offset: 0,
            buf,
            read_success,
        })
    }

    pub fn from_path<P: AsRef<Path>>(
        block_size: usize,
        src: P,
        dst: P,
    ) -> Result<RescueCopier<File>> {
        let src_file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_EXCL | libc::O_DIRECT)
            .open(src)?;

        let dst_file = OpenOptions::new()
            .read(false)
            .write(true)
            .custom_flags(libc::O_EXCL | libc::O_DIRECT)
            .open(dst)?;

        RescueCopier::<File>::new(block_size, src_file, dst_file)
    }

    pub fn src_offset(mut self, offset: u64) -> Result<RescueCopier<T>> {
        if !is_page_aligned(offset) {
            return Err(anyhow!("offset must be page aligned"));
        }
        self.src_offset = offset;
        Ok(self)
    }

    pub fn dest_offset(mut self, offset: u64) -> Result<RescueCopier<T>> {
        if !is_page_aligned(offset) {
            return Err(anyhow!("offset must be page aligned"));
        }
        self.dst_offset = offset;
        Ok(self)
    }

    fn do_read(src: &T, pos: u64, buffer: &mut [u8], read_success: &mut FixedBitSet) -> usize {
        let mut read_fails = 0;
        for (i, chunk) in buffer.chunks_mut(PAGE_SIZE).enumerate() {
            let p = pos + (i << PAGE_SHIFT) as u64;
            if src.read_exact_at(chunk, p).is_ok() {
                read_success.insert(i);
            } else {
                read_fails += 1;
            }
        }
        read_fails
    }

    fn do_write(dst: &T, pos: u64, buffer: &mut [u8], selected_pages: &FixedBitSet) -> u32 {
        let mut write_fails = 0;
        for i in selected_pages.ones() {
            let offset = i << PAGE_SHIFT;
            let page_buf = &buffer[offset..offset + PAGE_SIZE];
            if dst.write_all_at(page_buf, pos + offset as u64).is_err() {
                write_fails += 1;
            }
        }
        write_fails
    }
}

impl<T: FileExt + 'static> Copier for RescueCopier<T> {
    fn copy(
        &mut self,
        ops: &[CopyOp],
        progress: Arc<dyn CopyProgress + Sync + Send>,
    ) -> Result<CopyStats> {
        let mut stats = CopyStats::new(ops.len() as u64);
        let mut nr_copied = 0;

        // copy loop
        for op in ops {
            self.read_success.clear();

            let pos = self.src_offset + op.src * self.block_size as u64;
            let read_fails =
                Self::do_read(&self.src, pos, self.buf.get_data(), &mut self.read_success);
            if read_fails > 0 {
                stats.read_errors.push(*op);
            }

            let pos = self.dst_offset + op.dst * self.block_size as u64;
            let write_fails =
                Self::do_write(&self.dst, pos, self.buf.get_data(), &self.read_success);
            if write_fails > 0 {
                stats.write_errors.push(*op);
                continue;
            }

            if read_fails == 0 && write_fails == 0 {
                nr_copied += 1;
            }
        }

        stats.nr_copied += nr_copied as u64;
        progress.update(&stats);

        Ok(stats)
    }
}

//-------------------------------------
