use std::alloc::{alloc, dealloc, Layout};
use std::fs::File;
use std::io::{self, Result};
use std::os::unix::io::AsRawFd;
use std::path::Path;

use crate::file_utils;
pub use crate::io_engine::buffer_pool::BufferPool;

//------------------------------------------

pub const PAGE_SIZE: usize = 4096;
pub const PAGE_SHIFT: usize = 12;
pub const BLOCK_SIZE: usize = 4096;
pub const SECTOR_SHIFT: usize = 9;
const ALIGN: usize = PAGE_SIZE;

#[derive(Debug)]
pub struct Block {
    pub loc: u64,
    data: *mut u8,
}

impl Block {
    // Creates a new block that corresponds to the given location.  The
    // memory is not initialised.
    pub fn new(loc: u64) -> Self {
        let layout = Layout::from_size_align(BLOCK_SIZE, ALIGN).unwrap();
        let ptr = unsafe { alloc(layout) };
        assert!(!ptr.is_null(), "out of memory");
        Block { loc, data: ptr }
    }

    pub fn zeroed(loc: u64) -> Self {
        let r = Self::new(loc);
        r.get_data().fill(0);
        r
    }

    pub fn get_data<'a>(&self) -> &'a mut [u8] {
        unsafe { std::slice::from_raw_parts_mut::<'a>(self.data, BLOCK_SIZE) }
    }

    pub fn zero(&mut self) {
        unsafe {
            std::ptr::write_bytes(self.data, 0, BLOCK_SIZE);
        }
    }

    #[allow(clippy::missing_safety_doc)]
    pub unsafe fn get_raw_ptr(&self) -> *mut u8 {
        self.data
    }
}

impl Drop for Block {
    fn drop(&mut self) {
        let layout = Layout::from_size_align(BLOCK_SIZE, ALIGN).unwrap();
        unsafe {
            dealloc(self.data, layout);
        }
    }
}

unsafe impl Send for Block {}

impl AsRef<[u8]> for Block {
    fn as_ref(&self) -> &[u8] {
        self.get_data()
    }
}

impl AsMut<[u8]> for Block {
    fn as_mut(&mut self) -> &mut [u8] {
        self.get_data()
    }
}

//------------------------------------------

pub fn is_page_aligned(v: u64) -> bool {
    v & (PAGE_SIZE as u64 - 1) == 0
}

//------------------------------------------

pub trait ReadHandler {
    fn handle(&mut self, loc: u64, data: std::io::Result<&[u8]>);
    fn complete(&mut self);
}

pub trait IoEngine: Send + Sync {
    fn get_nr_blocks(&self) -> u64;
    fn get_batch_size(&self) -> usize;

    fn read(&self, b: u64) -> Result<Block>;
    // The whole io could fail, or individual blocks
    fn read_many(&self, blocks: &[u64]) -> Result<Vec<Result<Block>>>;

    fn write(&self, block: &Block) -> Result<()>;
    // The whole io could fail, or individual blocks
    fn write_many(&self, blocks: &[Block]) -> Result<Vec<Result<()>>>;

    // FIXME: rename to stream_blocks?
    fn read_blocks(
        &self,
        io_block_pool: &mut BufferPool,
        blocks: &mut dyn Iterator<Item = u64>,
        handler: &mut dyn ReadHandler,
    ) -> io::Result<()>;
}

pub fn get_nr_blocks<P: AsRef<Path>>(path: P) -> io::Result<u64> {
    Ok(file_utils::file_size(path)? / (BLOCK_SIZE as u64))
}

//------------------------------------------

pub trait VectoredIo {
    fn read_vectored_at(&self, bufs: &mut [libc::iovec], pos: u64) -> io::Result<usize>;
    fn write_vectored_at(&self, bufs: &[libc::iovec], pos: u64) -> io::Result<usize>;
}

fn read_vectored_at(file: &File, bufs: &mut [libc::iovec], pos: u64) -> io::Result<usize> {
    let ptr = bufs.as_ptr();
    let ret = match unsafe { libc::preadv64(file.as_raw_fd(), ptr, bufs.len() as i32, pos as i64) }
    {
        -1 => return Err(io::Error::last_os_error()),
        n => n,
    };
    Ok(ret as usize)
}

fn write_vectored_at(file: &File, bufs: &[libc::iovec], pos: u64) -> io::Result<usize> {
    let ptr = bufs.as_ptr();
    let ret = match unsafe { libc::pwritev64(file.as_raw_fd(), ptr, bufs.len() as i32, pos as i64) }
    {
        -1 => return Err(io::Error::last_os_error()),
        n => n,
    };
    Ok(ret as usize)
}

impl VectoredIo for File {
    fn read_vectored_at(&self, bufs: &mut [libc::iovec], pos: u64) -> io::Result<usize> {
        read_vectored_at(self, bufs, pos)
    }

    fn write_vectored_at(&self, bufs: &[libc::iovec], pos: u64) -> io::Result<usize> {
        write_vectored_at(self, bufs, pos)
    }
}

impl VectoredIo for &File {
    fn read_vectored_at(&self, bufs: &mut [libc::iovec], pos: u64) -> io::Result<usize> {
        read_vectored_at(self, bufs, pos)
    }

    fn write_vectored_at(&self, bufs: &[libc::iovec], pos: u64) -> io::Result<usize> {
        write_vectored_at(self, bufs, pos)
    }
}

//-------------------------------------
