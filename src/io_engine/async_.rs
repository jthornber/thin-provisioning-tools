use io_uring::opcode;
use io_uring::types;
use io_uring::IoUring;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{Result};
use std::os::unix::fs::{OpenOptionsExt};
use std::os::unix::io::{AsRawFd, RawFd};
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::io_engine::*;

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
    pub fn new(path: &Path, queue_len: u32, writable: bool) -> Result<AsyncIoEngine> {
        AsyncIoEngine::new_with(path, queue_len, writable, true)
    }

    pub fn new_with(
        path: &Path,
        queue_len: u32,
        writable: bool,
        excl: bool,
    ) -> Result<AsyncIoEngine> {
        let nr_blocks = get_nr_blocks(path)?; // check file mode earlier
        let mut flags = libc::O_DIRECT;
        if excl {
            flags |= libc::O_EXCL;
        }
        let input = OpenOptions::new()
            .read(true)
            .write(writable)
            .custom_flags(flags)
            .open(path)?;

        Ok(AsyncIoEngine {
            inner: Mutex::new(AsyncIoEngine_ {
                queue_len,
                ring: IoUring::new(queue_len)?,
                nr_blocks,
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
            let ptr = unsafe { b.get_raw_ptr() };
            let read_e = opcode::Read::new(types::Fd(fd_inner), ptr, BLOCK_SIZE as u32)
                .offset(b.loc as i64 * BLOCK_SIZE as i64);

            unsafe {
                inner
                    .ring
                    .submission()
                    .push(&read_e.build().user_data(i as u64))
                    .expect("queue is full");
            }
        }

        inner.ring.submit_and_wait(count)?;

        let mut cqes = inner.ring.completion().collect::<Vec<_>>();

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
            let ptr = unsafe { b.get_raw_ptr() };
            let write_e = opcode::Write::new(types::Fd(fd_inner), ptr, BLOCK_SIZE as u32)
                .offset(b.loc as i64 * BLOCK_SIZE as i64);

            unsafe {
                inner
                    .ring
                    .submission()
                    .push(&write_e.build().user_data(i as u64))
                    .expect("queue is full");
            }
        }

        inner.ring.submit_and_wait(count)?;

        let mut cqes = inner.ring.completion().collect::<Vec<_>>();

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

    fn suggest_nr_threads(&self) -> usize {
        std::cmp::min(8, num_cpus::get())
    }

    fn read(&self, b: u64) -> Result<Block> {
        let mut inner = self.inner.lock().unwrap();
        let fd = types::Fd(inner.input.as_raw_fd());
        let b = Block::new(b);
        let ptr = unsafe { b.get_raw_ptr() };
        let read_e = opcode::Read::new(fd, ptr, BLOCK_SIZE as u32)
            .offset(b.loc as i64 * BLOCK_SIZE as i64);

        unsafe {
            inner
                .ring
                .submission()
                .push(&read_e.build().user_data(0))
                .expect("queue is full");
        }

        inner.ring.submit_and_wait(1)?;

        let cqes = inner.ring.completion().collect::<Vec<_>>();

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
        let ptr = unsafe { b.get_raw_ptr() };
        let write_e = opcode::Write::new(fd, ptr, BLOCK_SIZE as u32)
            .offset(b.loc as i64 * BLOCK_SIZE as i64);

        unsafe {
            inner
                .ring
                .submission()
                .push(&write_e.build().user_data(0))
                .expect("queue is full");
        }

        inner.ring.submit_and_wait(1)?;

        let cqes = inner.ring.completion().collect::<Vec<_>>();

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
