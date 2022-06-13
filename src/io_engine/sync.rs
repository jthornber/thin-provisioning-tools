use std::fs::File;
use std::fs::OpenOptions;
use std::io::{Result};
use std::ops::{Deref, DerefMut};
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::Path;
use std::sync::{Condvar, Mutex};

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
        self.file.as_ref().expect("empty file guard")
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
    fn open_file(path: &Path, writable: bool, excl: bool) -> Result<File> {
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

    pub fn new(path: &Path, nr_files: usize, writable: bool) -> Result<SyncIoEngine> {
        SyncIoEngine::new_with(path, nr_files, writable, true)
    }

    pub fn new_with(
        path: &Path,
        nr_files: usize,
        writable: bool,
        excl: bool,
    ) -> Result<SyncIoEngine> {
        let nr_blocks = get_nr_blocks(path)?; // check file mode before opening it
        let mut files = Vec::with_capacity(nr_files);
        let file = SyncIoEngine::open_file(path, writable, excl)?;
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


