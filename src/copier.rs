use io_uring::{opcode, types, IoUring};
use std::collections::BTreeMap;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, ErrorKind};
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::AsRawFd;
use std::path::Path;

use crate::mempool::*;

//------------------------------------------

enum AioOp {
    Read,
    Write,
}

type Handle = std::os::unix::io::RawFd;

struct AioEngine {
    ring: IoUring,
    fds: Vec<File>,
    len: BTreeMap<u64, u32>,
}

impl AioEngine {
    fn new(queue_depth: u32) -> io::Result<AioEngine> {
        Ok(AioEngine {
            ring: IoUring::new(queue_depth)?,
            fds: Vec::new(),
            len: BTreeMap::new(),
        })
    }

    fn open_file(&mut self, path: &Path, writable: bool, excl: bool) -> io::Result<Handle> {
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

    fn issue(
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

    fn wait(&mut self) -> io::Result<Vec<Result<u64, (u64, io::Error)>>> {
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

const SECTOR_SHIFT: u64 = 9;
const ALIGN: usize = 4096;

pub struct CopyOp {
    pub src_b: u64, // block_address
    pub dest_b: u64,
}

impl CopyOp {
    pub fn new(src_b: u64, dest_b: u64) -> CopyOp {
        CopyOp { src_b, dest_b }
    }
}

#[derive(PartialEq)]
enum JobStatus {
    Reading,
    Writing,
}

struct CopyJob {
    op: CopyOp,
    data: AllocBlock,
    status: JobStatus,
}

pub enum IoError {
    ReadError(CopyOp, io::Error),
    WriteError(CopyOp, io::Error),
    LostTracking(u64),
}

impl fmt::Display for IoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IoError::ReadError(op, _) => write!(f, "read error at block {}", op.src_b),
            IoError::WriteError(op, _) => write!(f, "write error at block {}", op.dest_b),
            IoError::LostTracking(c) => write!(f, "lost tracking with context {}", c),
        }
    }
}

pub struct Copier {
    engine: AioEngine,
    block_size: u32, // sectors
    queue_depth: u32,
    pool: MemPool,
    jobs: BTreeMap<u64, CopyJob>,
    src: Handle,
    dest: Handle,
    src_offset: u64,  // sectors
    dest_offset: u64, // sectors
    key_counter: u64,
}

impl Copier {
    pub fn new(
        src: &Path,
        dest: &Path,
        block_size: u32,
        queue_depth: u32,
        src_offset: u64,
        dest_offset: u64,
    ) -> io::Result<Copier> {
        if block_size > (u32::MAX >> SECTOR_SHIFT) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "block size out of bounds",
            ));
        }

        let pool = MemPool::new(
            (block_size << SECTOR_SHIFT) as usize,
            queue_depth as usize,
            ALIGN,
        )?;

        let mut engine = AioEngine::new(queue_depth)?;
        let src = engine.open_file(src, false, true)?;
        let dest = engine.open_file(dest, true, true)?;

        Ok(Copier {
            engine,
            block_size,
            queue_depth,
            pool,
            jobs: BTreeMap::new(),
            src,
            dest,
            src_offset,
            dest_offset,
            key_counter: 0,
        })
    }

    fn gen_key(&mut self) -> u64 {
        self.key_counter.wrapping_add(1)
    }

    pub fn queue_depth(&self) -> u32 {
        self.queue_depth
    }

    pub fn nr_pending(&self) -> usize {
        self.jobs.len()
    }

    pub fn issue(&mut self, op: CopyOp) -> io::Result<()> {
        let block = self
            .pool
            .alloc()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "no free slots"))?;

        let context = self.gen_key();
        let offset = (self.src_offset + op.src_b * self.block_size as u64) << SECTOR_SHIFT;
        if offset > i64::MAX as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "read offset out of bounds",
            ));
        }

        if let Err(e) = self.engine.issue(
            self.src,
            AioOp::Read,
            offset as libc::off_t,
            self.block_size << SECTOR_SHIFT,
            block.data,
            context,
        ) {
            self.pool.free(block);
            return Err(e);
        }

        self.jobs.insert(
            context,
            CopyJob {
                op,
                data: block,
                status: JobStatus::Reading,
            },
        );
        Ok(())
    }

    pub fn wait(&mut self) -> io::Result<Vec<Result<CopyOp, IoError>>> {
        let rets = self.engine.wait()?;

        let mut completion = Vec::new();
        for r in rets {
            let (context, cq_err) = r.map(|c| (c, None)).unwrap_or_else(|(c, e)| (c, Some(e)));

            let job;
            match self.jobs.get_mut(&context) {
                Some(j) => job = j,
                None => {
                    completion.push(Err(IoError::LostTracking(context)));
                    continue;
                }
            }

            if let Some(err) = cq_err {
                let j = self.jobs.remove(&context).unwrap();
                self.pool.free(j.data);
                let e = match j.status {
                    JobStatus::Reading => IoError::ReadError(j.op, err),
                    JobStatus::Writing => IoError::WriteError(j.op, err),
                };
                completion.push(Err(e));
                continue;
            }

            if job.status == JobStatus::Reading {
                job.status = JobStatus::Writing;

                let offset =
                    (self.dest_offset + job.op.dest_b * self.block_size as u64) << SECTOR_SHIFT;
                if offset > i64::MAX as u64 {
                    let j = self.jobs.remove(&context).unwrap();
                    self.pool.free(j.data);
                    completion.push(Err(IoError::WriteError(
                        j.op,
                        io::Error::new(io::ErrorKind::InvalidInput, "write offset out of bounds"),
                    )));
                    continue;
                }

                if let Err(e) = self.engine.issue(
                    self.dest,
                    AioOp::Write,
                    offset as i64,
                    self.block_size << SECTOR_SHIFT,
                    job.data.data,
                    context,
                ) {
                    let j = self.jobs.remove(&context).unwrap();
                    self.pool.free(j.data);
                    completion.push(Err(IoError::WriteError(j.op, e)));
                }
            } else {
                let j = self.jobs.remove(&context).unwrap();
                self.pool.free(j.data);
                completion.push(Ok(j.op));
            }
        }

        Ok(completion)
    }
}

//-----------------------------------------
