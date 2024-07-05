use anyhow::{anyhow, Result};
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread::*;

use crate::copier::batcher::*;
use crate::copier::sync_copier::*;
use crate::copier::*;
use crate::file_utils;
use crate::io_engine::utils::*;
use crate::io_engine::*;
use crate::report::*;
use crate::thin::metadata::*;
use crate::thin::migrate::devices::*;
use crate::thin::migrate::stream::*;

//------------------------------------------

#[derive(Debug, PartialEq)]
pub struct SourceArgs {
    pub path: PathBuf,
    pub delta_id: Option<ThinId>,
}

pub struct FileDestArgs {
    pub path: PathBuf,
    pub create: bool,
}

pub enum DestArgs {
    Dev(PathBuf),
    File(FileDestArgs),
}

pub struct ThinMigrateOptions {
    pub source: SourceArgs,
    pub dest: DestArgs,
    pub zero_dest: bool,
    pub buffer_size_meg: u64,
    pub report: Arc<Report>,
}

/*
struct Context {
    report: Arc<Report>,
    engine_in: Arc<dyn IoEngine + Send + Sync>,
    engine_out: Arc<dyn IoEngine + Send + Sync>,
}
*/

fn mk_engine<P: AsRef<Path>>(path: P) -> Result<Arc<dyn IoEngine + Send + Sync>> {
    let engine = SyncIoEngine::new_with(path, false, false)?;
    Ok(Arc::new(engine))
}

pub fn metadata_dev_from_thin(scanner: &mut DmScanner, thin: &File) -> Result<DeviceNr> {
    let thin_name = scanner.file_to_name(thin)?.clone();
    let thin_table = get_thin_table(scanner, &thin_name)?;
    let pool_name = scanner.dev_to_name(&thin_table.pool_dev)?.clone();
    let pool_table = get_pool_table(scanner, &pool_name)?;
    Ok(pool_table.metadata_dev)
}

struct Source {
    file: File,
    stream: Box<dyn Stream>,
    block_size: u64,
}

fn open_source(scanner: &mut DmScanner, src: &SourceArgs) -> Result<Source> {
    let thin = OpenOptions::new().read(true).write(false).open(&src.path)?;
    let thin_name = scanner.file_to_name(&thin)?.clone();
    let thin_table = get_thin_table(scanner, &thin_name)?;
    let pool_name = scanner.dev_to_name(&thin_table.pool_dev)?.clone();
    let pool_table = get_pool_table(scanner, &pool_name)?;
    let metadata_dev = metadata_dev_from_thin(scanner, &thin)?;
    let metadata_path = scanner.dev_to_path(&metadata_dev)?.unwrap();
    let metadata_engine = mk_engine(metadata_path)?;

    let stream = Box::new(ThinStream::new(&metadata_engine, thin_table.thin_id)?);

    Ok(Source {
        file: thin,
        stream,
        block_size: pool_table.data_block_size as u64,
    })
}

struct Dest {
    file: File,
}

fn open_dest_dev(path: &PathBuf, expected_len: u64) -> Result<Dest> {
    let out = OpenOptions::new().read(true).write(true).open(path)?;
    let actual_len = file_utils::file_size(path)?;
    if actual_len != expected_len {
        return Err(anyhow!(
            "lengths differ: input({}) != output({})",
            expected_len,
            actual_len
        ));
    }
    Ok(Dest { file: out })
}

fn open_dest_file(path: &PathBuf, create: bool, expected_len: u64) -> Result<File> {
    if create {
        let out = OpenOptions::new()
            .read(true)
            .write(true)
            .create(create)
            .open(path)?;
        out.set_len(expected_len)?;
        Ok(out)
    } else {
        let out = OpenOptions::new().read(true).write(true).open(path)?;
        let actual_len = file_utils::file_size(path)?;
        if actual_len != expected_len {
            return Err(anyhow!(
                "lengths differ input({}) != output({})",
                expected_len,
                actual_len
            ));
        }
        Ok(out)
    }
}

fn open_dest(_scanner: &mut DmScanner, dst: &DestArgs, expected_len: u64) -> Result<File> {
    match dst {
        DestArgs::Dev(path) => open_dest_dev(path, expected_len).map(|dst| dst.file),
        DestArgs::File(fdest) => open_dest_file(&fdest.path, fdest.create, expected_len),
    }
}

struct ThreadedCopier<T> {
    copier: T,
}

// unlike cache_writeback, thin_shrink doesn't allow failures
impl<T: Copier + Send + 'static> ThreadedCopier<T> {
    fn new(copier: T) -> ThreadedCopier<T> {
        ThreadedCopier { copier }
    }

    fn run(
        self,
        rx: mpsc::Receiver<Vec<CopyOp>>,
        progress: Arc<dyn CopyProgress + Send + Sync>,
    ) -> JoinHandle<Result<()>> {
        spawn(move || Self::run_(rx, self.copier, progress))
    }

    fn run_(
        rx: mpsc::Receiver<Vec<CopyOp>>,
        mut copier: T,
        progress: Arc<dyn CopyProgress + Send + Sync>,
    ) -> Result<()> {
        while let Ok(ops) = rx.recv() {
            let r = copier.copy(&ops, progress.clone());
            if r.is_err() {
                return Err(anyhow!(""));
            }

            let stats = r.unwrap();
            if !stats.read_errors.is_empty() || !stats.write_errors.is_empty() {
                return Err(anyhow!(""));
            }

            progress.update(&stats);
        }

        Ok(())
    }
}

struct MigrateProgress {}

impl CopyProgress for MigrateProgress {
    fn update(&self, _stats: &CopyStats) {}
}

fn copy_regions(
    mut stream: Box<dyn Stream>,
    in_file: File,
    out_file: File,
    block_size: u64,
    buffer_size_in_blocks: u64,
) -> Result<()> {
    let in_vio: VectoredBlockIo<File> = in_file.into();
    let out_vio: VectoredBlockIo<File> = out_file.into();
    let buffer_size = buffer_size_in_blocks * block_size;
    let copier = SyncCopier::new(
        (buffer_size as usize) << SECTOR_SHIFT,
        (block_size as usize) << SECTOR_SHIFT,
        in_vio,
        out_vio,
    )?;

    let (tx, rx) = mpsc::sync_channel::<Vec<CopyOp>>(1);
    let mut batcher = CopyOpBatcher::new(buffer_size_in_blocks as usize, tx);

    let copier = ThreadedCopier::new(copier);
    let progress = Arc::new(MigrateProgress {});
    let handle = copier.run(rx, progress);

    while let Some(chunk) = stream.next_chunk()? {
        match chunk.contents {
            ChunkContents::Skip => {
                // do nothing
            }
            ChunkContents::Copy => {
                let begin = chunk.offset / block_size;
                let end = (chunk.offset + chunk.len) / block_size;
                for b in begin..end {
                    batcher.push(CopyOp { src: b, dst: b })?;
                }
            }
            ChunkContents::Discard => {
                // Only needed when migrating a delta
                todo!();
            }
        }
    }

    batcher.complete()?;
    handle.join().unwrap()?;

    Ok(())
}

pub fn migrate(opts: ThinMigrateOptions) -> Result<()> {
    let mut scanner = DmScanner::new()?;
    let src = open_source(&mut scanner, &opts.source)?;
    let expected_len = file_utils::file_size(opts.source.path)?;
    let out_file = open_dest(&mut scanner, &opts.dest, expected_len)?;

    let buffer_size_in_blocks = (opts.buffer_size_meg * 1024) / src.block_size;

    copy_regions(
        src.stream,
        src.file,
        out_file,
        src.block_size,
        buffer_size_in_blocks as u64,
    )
}

//------------------------------------------
