use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::path::Path;

use crate::aio_engine::*;
use crate::mempool::*;

//------------------------------------------

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

pub struct AsyncCopier {
    engine: AioEngine,
    block_size: u32, // bytes
    queue_depth: u32,
    pool: MemPool,
    jobs: BTreeMap<u64, CopyJob>,
    src: Handle,
    dest: Handle,
    src_offset: u64,  // bytes
    dest_offset: u64, // bytes
    key_counter: u64,
}

impl AsyncCopier {
    pub fn new(
        src: &Path,
        dest: &Path,
        block_size: u32,
        queue_depth: u32,
        src_offset: u64,
        dest_offset: u64,
    ) -> io::Result<AsyncCopier> {
        let pool = MemPool::new(block_size as usize, queue_depth as usize)?;

        let mut engine = AioEngine::new(queue_depth)?;
        let src = engine.open_file(src, false, true)?;
        let dest = engine.open_file(dest, true, true)?;

        Ok(AsyncCopier {
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
        self.key_counter = self.key_counter.wrapping_add(1);
        self.key_counter
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
        let offset = self.src_offset + op.src_b * self.block_size as u64;
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
            self.block_size,
            block.data,
            context,
        ) {
            self.pool.free(block)?;
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

            let job = match self.jobs.get_mut(&context) {
                Some(j) => j,
                None => {
                    completion.push(Err(IoError::LostTracking(context)));
                    continue;
                }
            };

            if let Some(err) = cq_err {
                let j = self.jobs.remove(&context).unwrap();
                self.pool.free(j.data)?;
                let e = match j.status {
                    JobStatus::Reading => IoError::ReadError(j.op, err),
                    JobStatus::Writing => IoError::WriteError(j.op, err),
                };
                completion.push(Err(e));
                continue;
            }

            if job.status == JobStatus::Reading {
                job.status = JobStatus::Writing;

                let offset = self.dest_offset + job.op.dest_b * self.block_size as u64;
                if offset > i64::MAX as u64 {
                    let j = self.jobs.remove(&context).unwrap();
                    self.pool.free(j.data)?;
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
                    self.block_size,
                    job.data.data,
                    context,
                ) {
                    let j = self.jobs.remove(&context).unwrap();
                    self.pool.free(j.data)?;
                    completion.push(Err(IoError::WriteError(j.op, e)));
                }
            } else {
                let j = self.jobs.remove(&context).unwrap();
                self.pool.free(j.data)?;
                completion.push(Ok(j.op));
            }
        }

        Ok(completion)
    }
}

//-----------------------------------------
