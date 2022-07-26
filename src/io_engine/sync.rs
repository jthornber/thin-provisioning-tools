use std::fs::File;
use std::fs::OpenOptions;
use std::io::{self, Result};
use std::ops::{Deref, DerefMut};
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::Path;
use std::sync::{Condvar, Mutex};

use crate::io_engine::utils::*;
use crate::io_engine::*;

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
    fn new(engine: &'a SyncIoEngine, file: File) -> Self {
        FileGuard {
            engine,
            file: Some(file),
        }
    }
}

impl<'a> Deref for FileGuard<'a> {
    type Target = File;

    fn deref(&self) -> &File {
        self.file.as_ref().expect("empty file guard")
    }
}

impl<'a> DerefMut for FileGuard<'a> {
    fn deref_mut(&mut self) -> &mut File {
        match &mut self.file {
            None => {
                panic!("empty file guard")
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
    fn open_file<P: AsRef<Path>>(path: P, writable: bool, excl: bool) -> Result<File> {
        let file = OpenOptions::new()
            .read(true)
            .write(writable)
            .custom_flags(if excl {
                libc::O_EXCL | libc::O_DIRECT
            } else {
                libc::O_DIRECT
            })
            .open(path)?;

        Ok(file)
    }

    pub fn new<P: AsRef<Path>>(path: P, writable: bool) -> Result<Self> {
        SyncIoEngine::new_with(path, writable, true)
    }

    pub fn new_with<P: AsRef<Path>>(path: P, writable: bool, excl: bool) -> Result<Self> {
        let nr_files = 8;
        let nr_blocks = get_nr_blocks(path.as_ref())?; // check file mode before opening it
        let mut files = Vec::with_capacity(nr_files);
        let file = SyncIoEngine::open_file(path.as_ref(), writable, excl)?;
        for _n in 0..nr_files - 1 {
            files.push(file.try_clone()?);
        }
        files.push(file);

        Ok(SyncIoEngine {
            nr_blocks,
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
        input.read_exact_at(b.get_data(), b.loc * BLOCK_SIZE as u64)?;
        Ok(b)
    }

    fn bad_read<T>() -> Result<T> {
        Err(io::Error::new(io::ErrorKind::Other, "read failed"))
    }

    fn read_many_(input: &File, blocks: &[u64]) -> Result<Vec<Result<Block>>> {
        let mut bs = blocks
            .iter()
            .map(|loc| Some(Block::new(*loc)))
            .collect::<Vec<Option<Block>>>();

        // sort by location
        // bs.sort_by(|lhs, rhs| lhs.loc.cmp(&rhs.loc));

        // Split into runs of adjacent blocks
        let mut runs: Vec<usize> = Vec::with_capacity(blocks.len());
        let mut last: Option<(u64, u64)> = None;
        for b in &bs {
            let b = b.as_ref().unwrap();
            if let Some((begin, end)) = last {
                if end == b.loc {
                    last = Some((begin, b.loc + 1));
                } else {
                    runs.push((end - begin) as usize);
                    last = Some((b.loc, b.loc + 1));
                }
            } else {
                last = Some((b.loc, b.loc + 1));
            }
        }

        if let Some((begin, end)) = last {
            runs.push((end - begin) as usize);
        }

        // Issue ios
        let mut issued = 0;
        let mut run_results = Vec::with_capacity(runs.len());
        for run_len in &runs {
            assert!(*run_len > 0);
            let mut buffers = Vec::with_capacity(*run_len);
            for i in 0..*run_len {
                buffers.push(bs[i + issued].as_ref().unwrap().get_data());
            }
            run_results.push(read_blocks(
                input,
                &mut buffers[..],
                bs[issued].as_ref().unwrap().loc * BLOCK_SIZE as u64,
            ));
            issued += run_len;
        }

        let mut results = Vec::with_capacity(bs.len());
        let mut index = 0;
        for (run_len, r) in runs.iter().zip(run_results) {
            match r {
                Err(_) => {
                    for _ in 0..*run_len {
                        results.push(Self::bad_read());
                    }
                    index += *run_len;
                }
                Ok(rs) => {
                    for r in rs {
                        match r {
                            Err(_) => {
                                results.push(Self::bad_read());
                            }
                            Ok(()) => {
                                results.push(Ok(bs[index].take().unwrap()));
                            }
                        }
                        index += 1;
                    }
                }
            }
        }

        // unsort
        // FIXME: finish

        Ok(results)
    }

    fn write_(output: &mut File, b: &Block) -> Result<()> {
        output.write_all_at(b.get_data(), b.loc * BLOCK_SIZE as u64)?;
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

    fn suggest_nr_threads(&self) -> usize {
        std::cmp::min(8, num_cpus::get())
    }

    fn read(&self, loc: u64) -> Result<Block> {
        SyncIoEngine::read_(&mut self.get(), loc)
    }

    fn read_many(&self, blocks: &[u64]) -> Result<Vec<Result<Block>>> {
        let input = self.get();
        Self::read_many_(&*input, &blocks)
        /*
        let mut bs = Vec::new();
        for b in blocks {
            bs.push(SyncIoEngine::read_(&mut input, *b));
        }
        Ok(bs)
        */
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
