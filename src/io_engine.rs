use anyhow::Result;
use io_uring::opcode::{self, types};
use io_uring::IoUring;
use std::alloc::{alloc, dealloc, Layout};
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{self, Read, Seek, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::{AsRawFd, RawFd};
use std::path::Path;
use std::sync::{Arc, Condvar, Mutex};

//------------------------------------------

pub const BLOCK_SIZE: usize = 4096;
const ALIGN: usize = 4096;

#[derive(Debug)]
pub struct Block {
    pub loc: u64,
    pub data: *mut u8,
}

impl Block {
    pub fn new(loc: u64) -> Block {
        let layout = Layout::from_size_align(BLOCK_SIZE, ALIGN).unwrap();
        let ptr = unsafe { alloc(layout) };
        assert!(!ptr.is_null(), "out of memory");
        Block { loc, data: ptr }
    }

    pub fn get_data<'a>(&self) -> &'a mut [u8] {
        unsafe { std::slice::from_raw_parts_mut::<'a>(self.data, BLOCK_SIZE) }
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
    fn read(&self, block: &mut Block) -> Result<()>;
    fn read_many(&self, blocks: &mut [Block]) -> Result<()>;
    fn write(&self, block: &Block) -> Result<()>;
    fn write_many(&self, blocks: &[Block]) -> Result<()>;
}

fn get_nr_blocks(path: &Path) -> io::Result<u64> {
    let metadata = std::fs::metadata(path)?;
    Ok(metadata.len() / (BLOCK_SIZE as u64))
}

//------------------------------------------

pub struct SyncIoEngine {
    nr_blocks: u64,
    files: Mutex<Vec<File>>,
    cvar: Condvar,
}

impl SyncIoEngine {
    fn open_file(path: &Path, writeable: bool) -> Result<File> {
        let file = OpenOptions::new()
            .read(true)
            .write(writeable)
            .custom_flags(libc::O_DIRECT)
            .open(path)?;

        Ok(file)
    }

    pub fn new(path: &Path, nr_files: usize, writeable: bool) -> Result<SyncIoEngine> {
        let mut files = Vec::new();
        for _n in 0..nr_files {
            files.push(SyncIoEngine::open_file(path, writeable)?);
        }

        Ok(SyncIoEngine {
            nr_blocks: get_nr_blocks(path)?,
            files: Mutex::new(files),
            cvar: Condvar::new(),
        })
    }

    fn get(&self) -> File {
        let mut files = self.files.lock().unwrap();

        while files.len() == 0 {
            files = self.cvar.wait(files).unwrap();
        }
        files.pop().unwrap()
    }

    fn put(&self, f: File) {
        let mut files = self.files.lock().unwrap();
        files.push(f);
        self.cvar.notify_one();
    }
}

impl IoEngine for SyncIoEngine {
    fn get_nr_blocks(&self) -> u64 {
        self.nr_blocks
    }

    fn read(&self, b: &mut Block) -> Result<()> {
        let mut input = self.get();
        input.seek(io::SeekFrom::Start(b.loc * BLOCK_SIZE as u64))?;
        input.read_exact(&mut b.get_data())?;
        self.put(input);

        Ok(())
    }

    fn read_many(&self, blocks: &mut [Block]) -> Result<()> {
        let mut input = self.get();
        for b in blocks {
            input.seek(io::SeekFrom::Start(b.loc * BLOCK_SIZE as u64))?;
            input.read_exact(&mut b.get_data())?;
        }
        self.put(input);

        Ok(())
    }

    fn write(&self, b: &Block) -> Result<()> {
        let mut input = self.get();
        input.seek(io::SeekFrom::Start(b.loc * BLOCK_SIZE as u64))?;
        input.write_all(&b.get_data())?;
        self.put(input);

        Ok(())
    }

    fn write_many(&self, blocks: &[Block]) -> Result<()> {
        let mut input = self.get();
        for b in blocks {
            input.seek(io::SeekFrom::Start(b.loc * BLOCK_SIZE as u64))?;
            input.write_all(&b.get_data())?;
        }
        self.put(input);

        Ok(())
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
    fn read_many_(&self, blocks: &mut [Block]) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let count = blocks.len();
        let fd = types::Target::Fd(inner.input.as_raw_fd());

        for b in blocks.iter_mut() {
            let read_e = opcode::Read::new(fd, b.data, BLOCK_SIZE as u32)
                .offset(b.loc as i64 * BLOCK_SIZE as i64);

            unsafe {
                let mut queue = inner.ring.submission().available();
                queue
                    .push(read_e.build().user_data(1))
                    .ok()
                    .expect("queue is full");
            }
        }

        inner.ring.submit_and_wait(count)?;

        let cqes = inner.ring.completion().available().collect::<Vec<_>>();

        // FIXME: return proper errors
        assert_eq!(cqes.len(), count);
        for c in &cqes {
            assert_eq!(c.result(), BLOCK_SIZE as i32);
        }

        Ok(())
    }

    fn write_many_(&self, blocks: &[Block]) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let count = blocks.len();
        let fd = types::Target::Fd(inner.input.as_raw_fd());

        for b in blocks.iter() {
            let write_e = opcode::Write::new(fd, b.data, BLOCK_SIZE as u32)
                .offset(b.loc as i64 * BLOCK_SIZE as i64);

            unsafe {
                let mut queue = inner.ring.submission().available();
                queue
                    .push(write_e.build().user_data(1))
                    .ok()
                    .expect("queue is full");
            }
        }

        inner.ring.submit_and_wait(count)?;

        let cqes = inner.ring.completion().available().collect::<Vec<_>>();

        // FIXME: return proper errors
        assert_eq!(cqes.len(), count);
        for c in &cqes {
            assert_eq!(c.result(), BLOCK_SIZE as i32);
        }

        Ok(())
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

    fn read(&self, b: &mut Block) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let fd = types::Target::Fd(inner.input.as_raw_fd());
        let read_e = opcode::Read::new(fd, b.data, BLOCK_SIZE as u32)
            .offset(b.loc as i64 * BLOCK_SIZE as i64);

        unsafe {
            let mut queue = inner.ring.submission().available();
            queue
                .push(read_e.build().user_data(1))
                .ok()
                .expect("queue is full");
        }

        inner.ring.submit_and_wait(1)?;

        let cqes = inner.ring.completion().available().collect::<Vec<_>>();

        // FIXME: return proper errors
        assert_eq!(cqes.len(), 1);
        assert_eq!(cqes[0].user_data(), 1);
        assert_eq!(cqes[0].result(), BLOCK_SIZE as i32);

        Ok(())
    }

    fn read_many(&self, blocks: &mut [Block]) -> Result<()> {
        let inner = self.inner.lock().unwrap();
        let queue_len = inner.queue_len as usize;
        drop(inner);

        let mut done = 0;
        while done != blocks.len() {
            let len = usize::min(blocks.len() - done, queue_len);
            self.read_many_(&mut blocks[done..(done + len)])?;
            done += len;
        }
        Ok(())
    }

    fn write(&self, b: &Block) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let fd = types::Target::Fd(inner.input.as_raw_fd());
        let write_e = opcode::Write::new(fd, b.data, BLOCK_SIZE as u32)
            .offset(b.loc as i64 * BLOCK_SIZE as i64);

        unsafe {
            let mut queue = inner.ring.submission().available();
            queue
                .push(write_e.build().user_data(1))
                .ok()
                .expect("queue is full");
        }

        inner.ring.submit_and_wait(1)?;

        let cqes = inner.ring.completion().available().collect::<Vec<_>>();

        // FIXME: return proper errors
        assert_eq!(cqes.len(), 1);
        assert_eq!(cqes[0].user_data(), 1);
        assert_eq!(cqes[0].result(), BLOCK_SIZE as i32);

        Ok(())
    }

    fn write_many(&self, blocks: &[Block]) -> Result<()> {
        let inner = self.inner.lock().unwrap();
        let queue_len = inner.queue_len as usize;
        drop(inner);

        let mut done = 0;
        while done != blocks.len() {
            let len = usize::min(blocks.len() - done, queue_len);
            self.write_many_(&blocks[done..(done + len)])?;
            done += len;
        }
        Ok(())
    }
}

//------------------------------------------
