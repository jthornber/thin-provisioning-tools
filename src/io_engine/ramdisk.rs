use roaring::RoaringBitmap;
use std::io;
use std::ops::Range;
use std::os::unix::fs::FileExt;
use std::sync::{Arc, Mutex};

use crate::io_engine::base::{PAGE_SHIFT, PAGE_SIZE};
use crate::io_engine::buffer::Buffer;
use crate::io_engine::VectoredIo;
use crate::math::div_up;

//------------------------------------------

/// A mock ramdisk that simulates direct io behaviors and also supports
/// error injection.
/// If there's any broken sector within the range to read or write, then
/// the entire io should fail.
pub struct Ramdisk {
    data: Arc<Buffer>,
    invalid_pages: Arc<Mutex<RoaringBitmap>>,
}

impl Ramdisk {
    pub fn new(size_bytes: u32) -> Self {
        let data = Arc::new(Buffer::new(size_bytes as usize, 4096));
        let invalid_pages = Arc::new(Mutex::new(RoaringBitmap::new()));
        Self {
            data,
            invalid_pages,
        }
    }

    pub fn try_clone(&self) -> io::Result<Ramdisk> {
        Ok(Self {
            data: self.data.clone(),
            invalid_pages: self.invalid_pages.clone(),
        })
    }

    pub fn invalidate(&mut self, bytes: Range<u32>) {
        let page_begin = bytes.start >> PAGE_SHIFT;
        let page_end = div_up(bytes.end, PAGE_SIZE as u32);
        let mut invalid_pages = self.invalid_pages.lock().unwrap();

        for i in page_begin..page_end {
            invalid_pages.insert(i);
        }
    }

    fn is_valid_range(invalid_pages: &RoaringBitmap, bytes: Range<u32>) -> bool {
        let page_begin = bytes.start >> PAGE_SHIFT;
        let page_end = div_up(bytes.end, PAGE_SIZE as u32);

        for i in page_begin..page_end {
            if invalid_pages.contains(i) {
                return false;
            }
        }
        true
    }
}

impl VectoredIo for Ramdisk {
    fn read_vectored_at(&self, bufs: &mut [libc::iovec], pos: u64) -> io::Result<usize> {
        let invalid_pages = self.invalid_pages.lock().unwrap();

        let mut begin = pos as u32;
        for buf in bufs {
            if buf.iov_len & (PAGE_SIZE - 1) != 0 {
                return Err(io::Error::from(io::ErrorKind::InvalidInput));
            }

            if !Self::is_valid_range(&invalid_pages, begin..begin + buf.iov_len as u32) {
                return Err(io::Error::new(io::ErrorKind::Other, "read error"));
            }

            unsafe {
                std::ptr::copy(
                    self.data.get_data()[begin as usize..].as_ptr(),
                    buf.iov_base as *mut u8,
                    buf.iov_len,
                );
            }

            begin += buf.iov_len as u32;
        }
        Ok(begin as usize - pos as usize)
    }

    fn write_vectored_at(&self, bufs: &[libc::iovec], pos: u64) -> io::Result<usize> {
        let invalid_pages = self.invalid_pages.lock().unwrap();

        let mut begin = pos as u32;
        for buf in bufs {
            if buf.iov_len & (PAGE_SIZE - 1) != 0 {
                return Err(io::Error::from(io::ErrorKind::InvalidInput));
            }

            if !Self::is_valid_range(&invalid_pages, begin..begin + buf.iov_len as u32) {
                return Err(io::Error::new(io::ErrorKind::Other, "write error"));
            }

            unsafe {
                std::ptr::copy(
                    buf.iov_base as *const u8,
                    self.data.get_data()[begin as usize..].as_mut_ptr(),
                    buf.iov_len,
                );
            }

            begin += buf.iov_len as u32;
        }
        Ok(begin as usize - pos as usize)
    }
}

impl FileExt for Ramdisk {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        let invalid_pages = self.invalid_pages.lock().unwrap();
        if !Self::is_valid_range(
            &invalid_pages,
            offset as u32..offset as u32 + buf.len() as u32,
        ) {
            return Err(io::Error::new(io::ErrorKind::Other, "read error"));
        }
        buf.clone_from_slice(&self.data.get_data()[offset as usize..(offset as usize + buf.len())]);

        Ok(buf.len())
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        let invalid_pages = self.invalid_pages.lock().unwrap();
        if !Self::is_valid_range(
            &invalid_pages,
            offset as u32..offset as u32 + buf.len() as u32,
        ) {
            return Err(io::Error::new(io::ErrorKind::Other, "write error"));
        }
        self.data.get_data()[offset as usize..offset as usize + buf.len()].clone_from_slice(buf);

        Ok(buf.len())
    }
}

//------------------------------------------
