use io_uring::opcode::{self, types};
use io_uring::IoUring;
use safemem::write_bytes;
use std::alloc::{alloc, dealloc, Layout};
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Result;
use std::io::{self, Read, Seek, Write};
use std::ops::{Deref, DerefMut};
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::{AsRawFd, RawFd};
use std::path::Path;
use std::sync::{Arc, Condvar, Mutex};

use crate::file_utils;

//------------------------------------------

pub const BLOCK_SIZE: usize = 4096;
pub const SECTOR_SHIFT: usize = 9;
const ALIGN: usize = 4096;

#[derive(Clone, Debug)]
pub struct Block {
    pub loc: u64,
    data: *mut u8,
}

impl Block {
    // Creates a new block that corresponds to the given location.  The
    // memory is not initialised.
    pub fn new(loc: u64) -> Block {
        let layout = Layout::from_size_align(BLOCK_SIZE, ALIGN).unwrap();
        let ptr = unsafe { alloc(layout) };
        assert!(!ptr.is_null(), "out of memory");
        Block { loc, data: ptr }
    }

    pub fn zeroed(loc: u64) -> Block {
        let r = Self::new(loc);
        write_bytes(r.get_data(), 0);
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

//------------------------------------------

pub trait IoEngine {
    fn get_nr_blocks(&self) -> u64;
    fn get_batch_size(&self) -> usize;

    fn read(&self, b: u64) -> Result<Block>;
    // The whole io could fail, or individual blocks
    fn read_many(&self, blocks: &[u64]) -> Result<Vec<Result<Block>>>;

    fn write(&self, block: &Block) -> Result<()>;
    // The whole io could fail, or individual blocks
    fn write_many(&self, blocks: &[Block]) -> Result<Vec<Result<()>>>;
}

fn get_nr_blocks(path: &Path) -> io::Result<u64> {
    Ok(file_utils::file_size(path)? / (BLOCK_SIZE as u64))
}

//------------------------------------------

pub struct SyncIoEngine {
    nr_blocks: u64,
    files: Mutex<Vec<File>>,
    cvar: Condvar,
}

struct FileGuard<'a> {
    engine: &'a SyncIoEngine,
    file: Option<File>,
}

impl<'a> FileGuard<'a> {
    fn new(engine: &'a SyncIoEngine, file: File) -> FileGuard<'a> {
        FileGuard {
            engine,
            file: Some(file),
        }
    }
}

impl<'a> Deref for FileGuard<'a> {
    type Target = File;

    fn deref(&self) -> &File {
        &self.file.as_ref().expect("empty file guard")
    }
}

impl<'a> DerefMut for FileGuard<'a> {
    fn deref_mut(&mut self) -> &mut File {
        match &mut self.file {
            None => {
                todo!();
            }
            Some(f) => f,
        }
    }
}

impl<'a> Drop for FileGuard<'a> {
    fn drop(&mut self) {
        self.engine.put(self.file.take().expect("empty file guard"));
    }
}

impl SyncIoEngine {
    fn open_file(path: &Path, writeable: bool) -> Result<File> {
        let file = OpenOptions::new().read(true).write(writeable).open(path)?;

        Ok(file)
    }

    pub fn new(path: &Path, nr_files: usize, writeable: bool) -> Result<SyncIoEngine> {
        let mut files = Vec::with_capacity(nr_files);
        for _n in 0..nr_files {
            files.push(SyncIoEngine::open_file(path, writeable)?);
        }

        Ok(SyncIoEngine {
            nr_blocks: get_nr_blocks(path)?,
            files: Mutex::new(files),
            cvar: Condvar::new(),
        })
    }

    fn get(&self) -> FileGuard {
        let mut files = self.files.lock().unwrap();

        while files.len() == 0 {
            files = self.cvar.wait(files).unwrap();
        }

        FileGuard::new(self, files.pop().unwrap())
    }

    fn put(&self, f: File) {
        let mut files = self.files.lock().unwrap();
        files.push(f);
        self.cvar.notify_one();
    }

    fn read_(input: &mut File, loc: u64) -> Result<Block> {
        let b = Block::new(loc);
        input.seek(io::SeekFrom::Start(b.loc * BLOCK_SIZE as u64))?;
        input.read_exact(b.get_data())?;
        Ok(b)
    }

    fn write_(output: &mut File, b: &Block) -> Result<()> {
        output.seek(io::SeekFrom::Start(b.loc * BLOCK_SIZE as u64))?;
        output.write_all(&b.get_data())?;
        Ok(())
    }
}

impl IoEngine for SyncIoEngine {
    fn get_nr_blocks(&self) -> u64 {
        self.nr_blocks
    }

    fn get_batch_size(&self) -> usize {
        1
    }

    fn read(&self, loc: u64) -> Result<Block> {
        SyncIoEngine::read_(&mut self.get(), loc)
    }

    fn read_many(&self, blocks: &[u64]) -> Result<Vec<Result<Block>>> {
        let mut input = self.get();
        let mut bs = Vec::new();
        for b in blocks {
            bs.push(SyncIoEngine::read_(&mut input, *b));
        }
        Ok(bs)
    }

    fn write(&self, b: &Block) -> Result<()> {
        SyncIoEngine::write_(&mut self.get(), b)
    }

    fn write_many(&self, blocks: &[Block]) -> Result<Vec<Result<()>>> {
        let mut output = self.get();
        let mut bs = Vec::new();
        for b in blocks {
            bs.push(SyncIoEngine::write_(&mut output, b));
        }
        Ok(bs)
    }
}

//------------------------------------------

pub struct AsyncIoEngine_ {
    queue_len: u32,
    ring: IoUring,
    nr_blocks: u64,
    fd: RawFd,
    input: Arc<File>,
}

pub struct AsyncIoEngine {
    inner: Mutex<AsyncIoEngine_>,
}

impl AsyncIoEngine {
    pub fn new(path: &Path, queue_len: u32, writeable: bool) -> Result<AsyncIoEngine> {
        let input = OpenOptions::new()
            .read(true)
            .write(writeable)
            .custom_flags(libc::O_DIRECT)
            .open(path)?;

        Ok(AsyncIoEngine {
            inner: Mutex::new(AsyncIoEngine_ {
                queue_len,
                ring: IoUring::new(queue_len)?,
                nr_blocks: get_nr_blocks(path)?,
                fd: input.as_raw_fd(),
                input: Arc::new(input),
            }),
        })
    }

    // FIXME: refactor next two fns
    fn read_many_(&self, blocks: Vec<Block>) -> Result<Vec<Result<Block>>> {
        use std::io::*;

        let mut inner = self.inner.lock().unwrap();
        let count = blocks.len();
        let fd_inner = inner.input.as_raw_fd();

        for (i, b) in blocks.iter().enumerate() {
            let read_e = opcode::Read::new(types::Fd(fd_inner), b.data, BLOCK_SIZE as u32)
                .offset(b.loc as i64 * BLOCK_SIZE as i64);

            unsafe {
                let mut queue = inner.ring.submission().available();
                queue
                    .push(read_e.build().user_data(i as u64))
                    .ok()
                    .expect("queue is full");
            }
        }

        inner.ring.submit_and_wait(count)?;

        let mut cqes = inner.ring.completion().available().collect::<Vec<_>>();

        if cqes.len() != count {
            return Err(Error::new(
                ErrorKind::Other,
                "insufficient io_uring completions",
            ));
        }

        // reorder cqes
        cqes.sort_by(|a, b| a.user_data().partial_cmp(&b.user_data()).unwrap());

        let mut rs = Vec::new();
        for (i, b) in blocks.into_iter().enumerate() {
            let c = &cqes[i];

            let r = c.result();
            if r < 0 {
                let error = Error::from_raw_os_error(-r);
                rs.push(Err(error));
            } else if c.result() != BLOCK_SIZE as i32 {
                rs.push(Err(Error::new(ErrorKind::UnexpectedEof, "short read")));
            } else {
                rs.push(Ok(b));
            }
        }
        Ok(rs)
    }

    fn write_many_(&self, blocks: &[Block]) -> Result<Vec<Result<()>>> {
        use std::io::*;

        let mut inner = self.inner.lock().unwrap();
        let count = blocks.len();
        let fd_inner = inner.input.as_raw_fd();

        for (i, b) in blocks.iter().enumerate() {
            let write_e = opcode::Write::new(types::Fd(fd_inner), b.data, BLOCK_SIZE as u32)
                .offset(b.loc as i64 * BLOCK_SIZE as i64);

            unsafe {
                let mut queue = inner.ring.submission().available();
                queue
                    .push(write_e.build().user_data(i as u64))
                    .ok()
                    .expect("queue is full");
            }
        }

        inner.ring.submit_and_wait(count)?;

        let mut cqes = inner.ring.completion().available().collect::<Vec<_>>();

        // reorder cqes
        cqes.sort_by(|a, b| a.user_data().partial_cmp(&b.user_data()).unwrap());

        let mut rs = Vec::new();
        for c in cqes {
            let r = c.result();
            if r < 0 {
                let error = Error::from_raw_os_error(-r);
                rs.push(Err(error));
            } else if r != BLOCK_SIZE as i32 {
                rs.push(Err(Error::new(ErrorKind::UnexpectedEof, "short write")));
            } else {
                rs.push(Ok(()));
            }
        }
        Ok(rs)
    }
}

impl Clone for AsyncIoEngine {
    fn clone(&self) -> AsyncIoEngine {
        let inner = self.inner.lock().unwrap();
        eprintln!("in clone, queue_len = {}", inner.queue_len);
        AsyncIoEngine {
            inner: Mutex::new(AsyncIoEngine_ {
                queue_len: inner.queue_len,
                ring: IoUring::new(inner.queue_len).expect("couldn't create uring"),
                nr_blocks: inner.nr_blocks,
                fd: inner.fd,
                input: inner.input.clone(),
            }),
        }
    }
}

impl IoEngine for AsyncIoEngine {
    fn get_nr_blocks(&self) -> u64 {
        let inner = self.inner.lock().unwrap();
        inner.nr_blocks
    }

    fn get_batch_size(&self) -> usize {
        self.inner.lock().unwrap().queue_len as usize
    }

    fn read(&self, b: u64) -> Result<Block> {
        let mut inner = self.inner.lock().unwrap();
        let fd = types::Fd(inner.input.as_raw_fd());
        let b = Block::new(b);
        let read_e = opcode::Read::new(fd, b.data, BLOCK_SIZE as u32)
            .offset(b.loc as i64 * BLOCK_SIZE as i64);

        unsafe {
            let mut queue = inner.ring.submission().available();
            queue
                .push(read_e.build().user_data(0))
                .ok()
                .expect("queue is full");
        }

        inner.ring.submit_and_wait(1)?;

        let cqes = inner.ring.completion().available().collect::<Vec<_>>();

        let r = cqes[0].result();
        use std::io::*;
        if r < 0 {
            let error = Error::from_raw_os_error(-r);
            Err(error)
        } else if r != BLOCK_SIZE as i32 {
            Err(Error::new(ErrorKind::UnexpectedEof, "short write"))
        } else {
            Ok(b)
        }
    }

    fn read_many(&self, blocks: &[u64]) -> Result<Vec<Result<Block>>> {
        let inner = self.inner.lock().unwrap();
        let queue_len = inner.queue_len as usize;
        drop(inner);

        let mut results = Vec::new();
        for cs in blocks.chunks(queue_len) {
            let mut bs = Vec::new();
            for b in cs {
                bs.push(Block::new(*b));
            }

            results.append(&mut self.read_many_(bs)?);
        }

        Ok(results)
    }

    fn write(&self, b: &Block) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let fd = types::Fd(inner.input.as_raw_fd());
        let write_e = opcode::Write::new(fd, b.data, BLOCK_SIZE as u32)
            .offset(b.loc as i64 * BLOCK_SIZE as i64);

        unsafe {
            let mut queue = inner.ring.submission().available();
            queue
                .push(write_e.build().user_data(0))
                .ok()
                .expect("queue is full");
        }

        inner.ring.submit_and_wait(1)?;

        let cqes = inner.ring.completion().available().collect::<Vec<_>>();

        let r = cqes[0].result();
        use std::io::*;
        if r < 0 {
            let error = Error::from_raw_os_error(-r);
            Err(error)
        } else if r != BLOCK_SIZE as i32 {
            Err(Error::new(ErrorKind::UnexpectedEof, "short write"))
        } else {
            Ok(())
        }
    }

    fn write_many(&self, blocks: &[Block]) -> Result<Vec<Result<()>>> {
        let inner = self.inner.lock().unwrap();
        let queue_len = inner.queue_len as usize;
        drop(inner);

        let mut results = Vec::new();
        let mut done = 0;
        while done != blocks.len() {
            let len = usize::min(blocks.len() - done, queue_len);
            results.append(&mut self.write_many_(&blocks[done..(done + len)])?);
            done += len;
        }

        Ok(results)
    }
}

//------------------------------------------
