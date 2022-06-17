use io_uring::{opcode, types, IoUring};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, ErrorKind};
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::AsRawFd;
use std::path::Path;

#[cfg(test)]
mod tests;

//------------------------------------------

pub enum AioOp {
    Read,
    Write,
}

pub type Handle = std::os::unix::io::RawFd;

pub struct AioEngine {
    ring: IoUring,
    fds: Vec<File>,
    len: BTreeMap<u64, u32>,
}

impl AioEngine {
    pub fn new(queue_depth: u32) -> io::Result<AioEngine> {
        Ok(AioEngine {
            ring: IoUring::new(queue_depth)?,
            fds: Vec::new(),
            len: BTreeMap::new(),
        })
    }

    pub fn open_file(&mut self, path: &Path, writable: bool, excl: bool) -> io::Result<Handle> {
        let mut flags = libc::O_DIRECT;
        if excl {
            flags |= libc::O_EXCL;
        }
        let file = OpenOptions::new()
            .read(true)
            .write(writable)
            .custom_flags(flags)
            .open(path)?;
        let fd = file.as_raw_fd();
        self.fds.push(file);
        Ok(fd)
    }

    #[allow(dead_code)]
    fn close_file(&mut self, handle: Handle) -> io::Result<()> {
        for (idx, file) in self.fds.iter().enumerate() {
            if file.as_raw_fd() == handle {
                self.fds.swap_remove(idx);
                return Ok(());
            }
        }
        Err(io::Error::from(ErrorKind::InvalidInput))
    }

    pub fn issue(
        &mut self,
        handle: Handle,
        op: AioOp,
        offset: libc::off_t,
        len: u32,
        buf: *mut u8,
        context: u64,
    ) -> io::Result<()> {
        let entry = match op {
            AioOp::Read => opcode::Read::new(types::Fd(handle), buf, len)
                .offset(offset)
                .build()
                .user_data(context),
            AioOp::Write => opcode::Write::new(types::Fd(handle), buf, len)
                .offset(offset)
                .build()
                .user_data(context),
        };

        self.len.insert(context, len);
        unsafe {
            self.ring
                .submission()
                .push(&entry)
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "submission queue full"))?;
        }
        Ok(())
    }

    pub fn wait(&mut self) -> io::Result<Vec<Result<u64, (u64, io::Error)>>> {
        self.ring.submit_and_wait(1)?;
        let cqes = self.ring.completion().collect::<Vec<_>>();
        let mut rs = Vec::new();

        for c in cqes.iter() {
            let context = c.user_data();
            let r = c.result();
            let len = self.len.remove(&context);

            if r < 0 {
                let error = io::Error::from_raw_os_error(-r);
                rs.push(Err((context, error)));
            } else if len.is_none() || r as u32 != len.unwrap() {
                let error = io::Error::new(io::ErrorKind::UnexpectedEof, "incompleted io");
                rs.push(Err((context, error)));
            } else {
                rs.push(Ok(context));
            }
        }
        Ok(rs)
    }
}

//------------------------------------------
