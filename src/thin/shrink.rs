use anyhow::{anyhow, Result};

use rangemap::RangeSet;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{BufWriter, Read, SeekFrom};
use std::ops::Range;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::vec::Vec;

use crate::copier::batcher::CopyOpBatcher;
use crate::copier::*;
use crate::io_engine::utils::VectoredBlockIo;
use crate::io_engine::{IoEngine, SyncIoEngine, SECTOR_SHIFT};
use crate::pdata::space_map::metadata::core_metadata_sm;
use crate::report::Report;
use crate::shrink::toplevel::*;
use crate::thin::dump::dump_metadata;
use crate::thin::ir::{self, MetadataVisitor, Visit};
use crate::thin::metadata::*;
use crate::thin::restore::Restorer;
use crate::thin::superblock::{read_superblock, Superblock, SUPERBLOCK_LOCATION};
use crate::thin::xml;
use crate::write_batcher::WriteBatcher;

//---------------------------------------

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
        thread::spawn(move || Self::run_(rx, self.copier, progress))
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

fn copy_regions(
    data_dev: &Path,
    remaps: &[(BlockRange, u64)],
    block_size: usize,
    progress: Arc<dyn CopyProgress + Send + Sync>,
) -> Result<()> {
    let file = OpenOptions::new().read(true).write(true).open(data_dev)?;
    let vio: VectoredBlockIo<File> = file.into();
    let buffer_size = std::cmp::max(block_size, 64 * 1024 * 1024);
    let copier = SyncCopier::in_file(buffer_size, block_size, vio)?;

    let (tx, rx) = mpsc::sync_channel::<Vec<CopyOp>>(1);
    let mut batcher = CopyOpBatcher::new(1_000_000, tx);

    let copier = ThreadedCopier::new(copier);
    let handle = copier.run(rx, progress);

    for (from, to) in remaps {
        let len = range_len(from);
        let dst_range = Range {
            start: *to,
            end: *to + len,
        };
        for (src, dst) in from.clone().zip(dst_range) {
            batcher.push(CopyOp { src, dst })?;
        }
    }

    batcher.complete()?;
    handle.join().unwrap()?;

    Ok(())
}

//---------------------------------------

struct MappingCollector {
    nr_blocks: u64,
    below: RangeSet<u64>,
    above: RangeSet<u64>,
}

impl MappingCollector {
    fn new(nr_blocks: u64) -> MappingCollector {
        MappingCollector {
            nr_blocks,
            below: RangeSet::new(),
            above: RangeSet::new(),
        }
    }

    fn get_remaps(self) -> Result<Vec<(BlockRange, u64)>> {
        let new_range = 0..self.nr_blocks;
        let free = self.below.gaps(&new_range);
        build_remaps(self.above, free)
    }
}

impl MetadataVisitor for MappingCollector {
    fn superblock_b(&mut self, _sb: &ir::Superblock) -> Result<Visit> {
        Ok(Visit::Continue)
    }

    fn superblock_e(&mut self) -> Result<Visit> {
        Ok(Visit::Continue)
    }

    fn def_shared_b(&mut self, _name: &str) -> Result<Visit> {
        Ok(Visit::Continue)
    }

    fn def_shared_e(&mut self) -> Result<Visit> {
        Ok(Visit::Continue)
    }

    fn device_b(&mut self, _d: &ir::Device) -> Result<Visit> {
        Ok(Visit::Continue)
    }

    fn device_e(&mut self) -> Result<Visit> {
        Ok(Visit::Continue)
    }

    fn map(&mut self, m: &ir::Map) -> Result<Visit> {
        if m.data_begin >= self.nr_blocks {
            self.above.insert(std::ops::Range {
                start: m.data_begin,
                end: m.data_begin + m.len,
            });
        } else if m.data_begin + m.len <= self.nr_blocks {
            self.below.insert(std::ops::Range {
                start: m.data_begin,
                end: m.data_begin + m.len,
            });
        } else {
            self.below.insert(std::ops::Range {
                start: m.data_begin,
                end: self.nr_blocks,
            });
            self.above.insert(std::ops::Range {
                start: self.nr_blocks,
                end: m.data_begin + m.len,
            });
        }
        Ok(Visit::Continue)
    }

    fn ref_shared(&mut self, _name: &str) -> Result<Visit> {
        Ok(Visit::Continue)
    }

    fn eof(&mut self) -> Result<Visit> {
        Ok(Visit::Continue)
    }
}

//---------------------------------------

struct DataRemapper<'a> {
    writer: &'a mut dyn MetadataVisitor,
    nr_blocks: u64,
    remaps: Vec<(BlockRange, u64)>,
}

impl<'a> DataRemapper<'a> {
    fn new(
        writer: &'a mut dyn MetadataVisitor,
        nr_blocks: u64,
        remaps: Vec<(BlockRange, u64)>,
    ) -> DataRemapper<'a> {
        DataRemapper {
            writer,
            nr_blocks,
            remaps,
        }
    }
}

impl<'a> MetadataVisitor for DataRemapper<'a> {
    fn superblock_b(&mut self, sb: &ir::Superblock) -> Result<Visit> {
        self.writer.superblock_b(sb)
    }

    fn superblock_e(&mut self) -> Result<Visit> {
        self.writer.superblock_e()
    }

    fn def_shared_b(&mut self, name: &str) -> Result<Visit> {
        self.writer.def_shared_b(name)
    }

    fn def_shared_e(&mut self) -> Result<Visit> {
        self.writer.def_shared_e()
    }

    fn device_b(&mut self, d: &ir::Device) -> Result<Visit> {
        self.writer.device_b(d)
    }

    fn device_e(&mut self) -> Result<Visit> {
        self.writer.device_e()
    }

    fn map(&mut self, m: &ir::Map) -> Result<Visit> {
        if m.data_begin + m.len < self.nr_blocks {
            // no remapping needed.
            self.writer.map(m)?;
        } else {
            let r = m.data_begin..(m.data_begin + m.len);
            let remaps = remap(&r, &self.remaps);
            let mut written = 0;

            for r in remaps {
                self.writer.map(&ir::Map {
                    thin_begin: m.thin_begin + written,
                    data_begin: r.start,
                    time: m.time,
                    len: range_len(&r),
                })?;
                written += range_len(&r);
            }
        }

        Ok(Visit::Continue)
    }

    fn ref_shared(&mut self, name: &str) -> Result<Visit> {
        self.writer.ref_shared(name)
    }

    fn eof(&mut self) -> Result<Visit> {
        self.writer.eof()
    }
}

//---------------------------------------

fn build_remaps_from_metadata(
    engine: Arc<dyn IoEngine>,
    sb: &Superblock,
    md: &Metadata,
    nr_blocks: u64,
) -> Result<Vec<(BlockRange, u64)>> {
    let mut collector = MappingCollector::new(nr_blocks);
    dump_metadata(engine, &mut collector, sb, md)?;
    collector.get_remaps()
}

fn build_remaps_from_xml<R: Read>(input: R, nr_blocks: u64) -> Result<Vec<(BlockRange, u64)>> {
    let mut collector = MappingCollector::new(nr_blocks);
    xml::read(input, &mut collector)?;
    collector.get_remaps()
}

pub struct ThinShrinkOptions {
    pub input: PathBuf,
    pub output: PathBuf,
    pub data_device: PathBuf,
    pub nr_blocks: u64,
    pub do_copy: bool,
    pub binary_mode: bool,
    pub report: Arc<Report>,
}

fn rewrite_xml(opts: ThinShrinkOptions) -> Result<()> {
    use std::io::Seek;

    // 1st pass
    let mut input = OpenOptions::new()
        .read(true)
        .write(false)
        .custom_flags(libc::O_EXCL)
        .open(&opts.input)?;
    let sb = xml::read_superblock(input.try_clone()?)?;
    input.seek(SeekFrom::Start(0))?;
    let remaps = build_remaps_from_xml(input.try_clone()?, opts.nr_blocks)?;

    let progress = Arc::new(IgnoreProgress {});
    if opts.do_copy {
        let bs = (sb.data_block_size as usize) << SECTOR_SHIFT;
        copy_regions(&opts.data_device, &remaps, bs, progress)?;
    }

    // 2nd pass
    let writer = BufWriter::new(File::create(&opts.output)?);
    let mut xml_writer = xml::XmlWriter::new(writer);
    let mut remapper = DataRemapper::new(&mut xml_writer, opts.nr_blocks, remaps);
    input.seek(SeekFrom::Start(0))?;
    xml::read(input, &mut remapper)
}

fn rebuild_metadata(opts: ThinShrinkOptions) -> Result<()> {
    let input = Arc::new(SyncIoEngine::new(&opts.input, false)?);
    let sb = read_superblock(input.as_ref(), SUPERBLOCK_LOCATION)?;
    let md = build_metadata(input.clone(), &sb)?;
    let md = optimise_metadata(md)?;

    // 1st pass
    let remaps = build_remaps_from_metadata(input.clone(), &sb, &md, opts.nr_blocks)?;

    let progress = Arc::new(IgnoreProgress {});
    if opts.do_copy {
        let bs = (sb.data_block_size as usize) << SECTOR_SHIFT;
        copy_regions(&opts.data_device, &remaps, bs, progress)?;
    }

    // 2nd pass
    let output = Arc::new(SyncIoEngine::new(&opts.output, true)?);
    let sm = core_metadata_sm(output.get_nr_blocks(), u32::MAX);
    let mut w = WriteBatcher::new(output.clone(), sm, output.get_batch_size());
    let mut restorer = Restorer::new(&mut w, opts.report);
    let mut remapper = DataRemapper::new(&mut restorer, opts.nr_blocks, remaps);
    dump_metadata(input, &mut remapper, &sb, &md)
}

pub fn shrink(opts: ThinShrinkOptions) -> Result<()> {
    if opts.binary_mode {
        rebuild_metadata(opts)
    } else {
        rewrite_xml(opts)
    }
}

//---------------------------------------
