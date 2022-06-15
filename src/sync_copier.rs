use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::Path;
use thread_local::ThreadLocal;

use crate::mempool::Buffer;

//-----------------------------------------

const BATCH_SIZE: usize = 1048576; // bytes

pub struct SyncCopier {
    src: File,
    dest: File,
    buffer: ThreadLocal<Buffer>,
}

impl SyncCopier {
    pub fn new(src: &Path, dest: &Path) -> io::Result<SyncCopier> {
        let src_file = OpenOptions::new()
            .read(true)
            .write(false)
            .custom_flags(libc::O_DIRECT)
            .open(src)?;

        let dest_file = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_DIRECT | libc::O_EXCL)
            .open(dest)?;

        Ok(SyncCopier {
            src: src_file,
            dest: dest_file,
            buffer: ThreadLocal::new(),
        })
    }

    // for in-file copying
    pub fn in_file(inout: &Path) -> io::Result<SyncCopier> {
        let src_file = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_DIRECT | libc::O_EXCL)
            .open(inout)?;

        let dest_file = src_file.try_clone()?;

        Ok(SyncCopier {
            src: src_file,
            dest: dest_file,
            buffer: ThreadLocal::new(),
        })
    }

    pub fn copy(&self, src: u64, dest: u64, len: u64) -> io::Result<()> {
        let buf = self.buffer.get_or_try(|| Buffer::new(BATCH_SIZE))?;

        let mut written = 0;
        while len - written >= BATCH_SIZE as u64 {
            self.src.read_exact_at(buf.get_data(), src + written)?;
            self.dest.write_all_at(buf.get_data(), dest + written)?;
            written += BATCH_SIZE as u64;
        }

        let residual = (len - written) as usize;
        if residual > 0 {
            self.src
                .read_exact_at(&mut buf.get_data()[..residual], src + written)?;
            self.dest
                .write_all_at(&buf.get_data()[..residual], dest + written)?;
        }

        Ok(())
    }
}

//-----------------------------------------
