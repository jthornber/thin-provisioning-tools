use anyhow::{anyhow, Result};
use devicemapper::*;
use libc;
use nom::IResult;
use roaring::bitmap::RoaringBitmap;
use std::collections::*;
use std::fs::{File, OpenOptions};
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use udev::Enumerator;

use crate::commands::engine::*;
use crate::copier::sync_copier::*;
use crate::io_engine::*;
use crate::pdata::btree;
use crate::pdata::btree::*;
use crate::pdata::btree_lookup::*;
use crate::pdata::btree_walker::*;
use crate::pdata::unpack::*;
use crate::report::*;
use crate::thin::block_time::*;
use crate::thin::device_detail::*;
use crate::thin::metadata::*;
use crate::thin::migrate::devices::*;
use crate::thin::migrate::metadata::*;
use crate::thin::migrate::stream::*;
use crate::thin::superblock::*;

//---------------------------------

/*
fn get_table(dm: &mut DM, dev: &DevId, expected_target_type: &str) -> Result<String> {
    let (_info, table) = dm.table_status(
        dev,
        DmOptions::default().set_flags(DmFlags::DM_STATUS_TABLE),
    )?;
    if table.len() != 1 {
        return Err(anyhow!(
            "thin table has too many rows (is it really a thin/pool device?)"
        ));
    }

    let (_offset, _len, target_type, args) = &table[0];
    if target_type != expected_target_type {
        return Err(anyhow!(format!(
            "dm expected table type {}, dm actual table type {}",
            expected_target_type, target_type
        )));
    }

    Ok(args.to_string())
}

fn get_thin_details<P: AsRef<Path>>(thin: P, dm_devs: &DevMap, dm: &mut DM) -> Result<ThinDetails> {
    let thin = OpenOptions::new()
        .read(true)
        .write(false)
        .create(false)
        // .custom_flags(nix::fcntl::OFlag::O_EXCL as i32)
        .open(thin)?;

    let metadata = thin.metadata()?;
    if !metadata.file_type().is_block_device() {
        return Err(anyhow!("Thin is not a block device"));
    }

    // FIXME: factor out
    let rdev = metadata.rdev();
    let (major, minor) = split_device_number(metadata.rdev());
    let thin_name = dm_devs.get(&(major, minor)).unwrap().clone();
    let thin_id = DevId::Name(&thin_name);

    let thin_args = get_table(dm, &thin_id, "thin")?;
    let (_, thin_details) =
        parse_thin_table(&thin_args).map_err(|_| anyhow!("couldn't parse thin table"))?;

    Ok(thin_details)
}

fn find_device(major: u32, minor: u32) -> Option<PathBuf> {
    let mut enumerator = Enumerator::new().unwrap();

    for device in enumerator.scan_devices().unwrap() {
        if let Some(devnum) = device.devnum() {
            let found_major = unsafe { libc::major(devnum) };
            let found_minor = unsafe { libc::minor(devnum) };

            if found_major == major && found_minor == minor {
                return device.devnode().map(PathBuf::from);
            }
        }
    }

    None
}

pub fn read_thin_mappings<P: AsRef<Path>>(thin: P) -> Result<ThinInfo> {
    let mut dm = DM::new()?;

    let dm_devs = collect_dm_devs(&mut dm)?;

    let thin_details = get_thin_details(thin, &dm_devs, &mut dm)?;

    let pool_name = dm_devs
        .get(&(thin_details.pool_major, thin_details.pool_minor))
        .ok_or_else(|| anyhow!("Pool device not found"))?
        .clone();
    let pool_id = DevId::Name(&pool_name);
    let pool_args = get_table(&mut dm, &pool_id, "thin-pool")?;
    let (_, pool_details) =
        parse_pool_table(&pool_args).map_err(|_| anyhow!("couldn't parse pool table"))?;

    // Find the metadata dev
    let metadata_path = find_device(pool_details.metadata_major, pool_details.metadata_minor)
        .ok_or_else(|| anyhow!("Couldn't find pool metadata device"))?;

    // Parse thin metadata
    dm.target_msg(&pool_id, None, "reserve_metadata_snap")?;
    let engine = Arc::new(SyncIoEngine::new_with(metadata_path, false, false)?);
    let r = read_info(&engine, thin_details.id);
    dm.target_msg(&pool_id, None, "release_metadata_snap")?;

    r
}

//---------------------------------

fn get_thin_name<P: AsRef<Path>>(thin: P, dm_devs: &DevMap) -> Result<DmNameBuf> {
    let thin = OpenOptions::new()
        .read(true)
        .write(false)
        .create(false)
        // .custom_flags(nix::fcntl::OFlag::O_EXCL as i32)
        .open(thin)?;

    let metadata = thin.metadata()?;

    if !metadata.file_type().is_block_device() {
        return Err(anyhow!("Old thin is not a block device"));
    }

    // Get the major:minor of the device at the given path
    let rdev = metadata.rdev();
    let thin_major = (rdev >> 8) as u32;
    let thin_minor = (rdev & 0xff) as u32;
    Ok(dm_devs.get(&(thin_major, thin_minor)).unwrap().clone())
}

fn get_thin_details_(thin_id: &DevId, dm: &mut DM) -> Result<ThinDetails> {
    let thin_args = get_table(dm, thin_id, "thin")?;
    let (_, thin_details) =
        parse_thin_table(&thin_args).map_err(|_| anyhow!("couldn't parse thin table"))?;
    Ok(thin_details)
}

pub fn read_thin_delta<P: AsRef<Path>>(old_thin: P, new_thin: P) -> Result<DeltaInfo> {
    use anyhow::Context;

    let mut dm = DM::new()?;
    let dm_devs = collect_dm_devs(&mut dm)?;

    let old_name =
        get_thin_name(old_thin, &dm_devs).context("unable to identify --delta-device")?;
    let new_name = get_thin_name(new_thin, &dm_devs).context("unable to identify input file")?;

    let old_thin_details = get_thin_details_(&DevId::Name(&old_name), &mut dm)?;
    let new_thin_details = get_thin_details_(&DevId::Name(&new_name), &mut dm)?;

    if old_thin_details.pool_minor != new_thin_details.pool_minor {
        return Err(anyhow!("thin devices are not from the same pool"));
    }

    let pool_name = dm_devs
        .get(&(old_thin_details.pool_major, old_thin_details.pool_minor))
        .ok_or_else(|| anyhow!("Pool device not found"))?
        .clone();
    let pool_id = DevId::Name(&pool_name);
    let pool_args = get_table(&mut dm, &pool_id, "thin-pool")?;
    let (_, pool_details) =
        parse_pool_table(&pool_args).map_err(|_| anyhow!("couldn't parse pool table"))?;

    // Find the metadata dev
    let metadata_path = find_device(pool_details.metadata_major, pool_details.metadata_minor)
        .ok_or_else(|| anyhow!("Couldn't find pool metadata device"))?;

    // Parse thin metadata
    dm.target_msg(&pool_id, None, "reserve_metadata_snap")?;
    let engine = Arc::new(SyncIoEngine::new_with(metadata_path, false, false)?);
    let r = read_delta_info(&engine, old_thin_details.id, new_thin_details.id);
    dm.target_msg(&pool_id, None, "release_metadata_snap")?;

    r
}
*/

//------------------------------------------

#[derive(Debug, PartialEq)]
pub struct Source {
    pub path: PathBuf,
    pub delta_id: Option<ThinId>,
}

#[derive(Debug, PartialEq)]
pub struct ThinDest {
    pub pool: PathBuf,
    pub thin_id: ThinId,
}

pub struct FileDest {
    pub path: PathBuf,
    pub create: bool,
}

pub enum Dest {
    Thin(ThinDest),
    Dev(PathBuf),
    File(FileDest),
}

pub struct ThinMigrateOptions {
    pub source: Source,
    pub dest: Dest,
    pub zero_dest: bool,
    pub engine_opts: EngineOptions,
    pub report: Arc<Report>,
}

struct Context {
    report: Arc<Report>,
    engine_in: Arc<dyn IoEngine + Send + Sync>,
    engine_out: Arc<dyn IoEngine + Send + Sync>,
}

/*
fn new_context(opts: &ThinMigrateOptions) -> Result<Context> {
    let engine_in = EngineBuilder::new(opts.input, &opts.engine_opts).build()?;
    let engine_out = EngineBuilder::new(opts.output, &opts.engine_opts)
        .write(true)
        .build()?;

    Ok(Context {
        report: opts.report.clone(),
        engine_in,
        engine_out,
    })
}
*/

// FIXME: naive implementation, use SIMD
fn all_zeroes(data: &[u8]) -> bool {
    data.iter().all(|&x| x == 0)
}

fn read_vec(file: &mut File, offset: u64, count: usize) -> Result<Option<Vec<u8>>> {
    // Allocate buffer with the desired capacity without initializing it
    let mut buffer: Vec<u8> = Vec::with_capacity(count);

    // SAFETY: We are about to read over the uninitialised data.
    // The uninitialized memory is never read.
    unsafe {
        buffer.set_len(count);
    }

    match file.seek(SeekFrom::Start(offset)) {
        Ok(_) => {
            match file.read(&mut buffer) {
                Ok(bytes_read) if bytes_read == 0 => Ok(None),
                Ok(bytes_read) => {
                    // It's important to set the length of the buffer to the actual bytes read
                    // to avoid exposing uninitialized memory.
                    unsafe {
                        buffer.set_len(bytes_read);
                    }

                    Ok(Some(buffer))
                }
                Err(e) => Err(anyhow!("Error reading file: {}", e)),
            }
        }
        Err(e) => Err(anyhow!("Error seeking file: {}", e)),
    }
}

fn open_source(scanner: &mut ThinDeviceScanner, src: &Source) -> Result<(File, Stream)> {
    let file = OpenOptions::new().read(true).write(true).open(src.path)?;

    if let Some(thin_id) = scanner.is_thin_device(src.path)? {
        let stream = ThinStream::new(&mut metadata_engine, thin_id, file);
        (file, stream);
    } else {
        let stream = DevStream::new(file, chunk_size)?;
        (file, stream)
    }

    // See if it's a thin device
    // if so:
    //     see if there's a delta origin
    //     read metadata
    // else:
    //     check there's no delta origin
}

/*
fn copy_regions(stream: Stream, in_file: &mut File, out_file: &mut File) -> Result<()> {
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
*/

pub fn migrate(opts: ThinMigrateOptions) -> Result<()> {
    let scanner = ThinDeviceScanner::new()?;
    let (in_file, stream) = open_source(&mut scanner, &opts.source)?;
    let out_file = open_dest(&opts.dest)?;

    Ok(())
}

//------------------------------------------
