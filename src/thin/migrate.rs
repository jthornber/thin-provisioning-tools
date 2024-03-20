use anyhow::{anyhow, Result};
use devicemapper::*;
use libc;
use nom::IResult;
use roaring::bitmap::RoaringBitmap;
use std::collections::*;
use std::fs::OpenOptions;
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use udev::Enumerator;

use crate::commands::engine::*;
use crate::io_engine::*;
use crate::pdata::btree;
use crate::pdata::btree::*;
use crate::pdata::btree_walker::*;
use crate::pdata::space_map::metadata::*;
use crate::report::*;
use crate::thin::block_time::*;
use crate::thin::device_detail::*;
use crate::thin::dump::*;
use crate::thin::metadata::*;
use crate::thin::metadata_repair::*;
use crate::thin::restore::*;
use crate::thin::superblock::*;
use crate::thin::superblock::*;
use crate::write_batcher::*;

//---------------------------------

type DevMap = BTreeMap<(u32, u32), DmNameBuf>;

fn collect_dm_devs(dm: &mut DM) -> Result<DevMap> {
    let mut devs_by_nr = BTreeMap::new();
    for (name, dev, _) in dm.list_devices()? {
        devs_by_nr.insert((dev.major, dev.minor), name);
    }

    Ok(devs_by_nr)
}

// FIXME: this duplicates a lot of the work done when reading the mappings
pub fn is_thin_device<P: AsRef<Path>>(path: P) -> Result<bool> {
    let thin = OpenOptions::new()
        .read(true)
        .write(false)
        .create(false)
        .open(path)?;

    let metadata = thin.metadata()?;

    if !metadata.file_type().is_block_device() {
        // Not a block device
        return Ok(false);
    }

    // Get the major:minor of the device at the given path
    let mut dm = DM::new()?;
    let rdev = metadata.rdev();
    let thin_major = (rdev >> 8) as u32;
    let thin_minor = (rdev & 0xff) as u32;
    let dm_devs = collect_dm_devs(&mut dm)?;

    let dm_name = dm_devs.get(&(thin_major, thin_minor));
    if dm_name.is_none() {
        // Not a dm device
        return Ok(false);
    }

    let thin_name = dm_name.unwrap().clone();
    let thin_id = DevId::Name(&thin_name);

    // Confirm this is a thin device
    let mut dm = DM::new()?;

    match get_table(&mut dm, &thin_id, "thin") {
        Ok(thin_args) => Ok(parse_thin_table(&thin_args).is_ok()),
        Err(_e) => Ok(false),
    }
}

//---------------------------------

#[derive(Default)]
struct MappingCollector {
    provisioned: Mutex<RoaringBitmap>,
}

impl MappingCollector {
    fn provisioned(self) -> RoaringBitmap {
        self.provisioned.into_inner().unwrap()
    }
}

impl NodeVisitor<BlockTime> for MappingCollector {
    fn visit(
        &self,
        _path: &[u64],
        _kr: &KeyRange,
        _header: &NodeHeader,
        keys: &[u64],
        _values: &[BlockTime],
    ) -> btree::Result<()> {
        let mut bits = self.provisioned.lock().unwrap();
        for k in keys {
            assert!(*k <= u32::MAX as u64);
            bits.insert(*k as u32);
        }
        Ok(())
    }

    fn visit_again(&self, _path: &[u64], _b: u64) -> btree::Result<()> {
        Ok(())
    }

    fn end_walk(&self) -> btree::Result<()> {
        Ok(())
    }
}

//---------------------------------

#[allow(dead_code)]
#[derive(Debug)]
pub struct ThinInfo {
    pub thin_id: u32,
    pub data_block_size: u32,
    pub details: DeviceDetail,
    pub provisioned_blocks: RoaringBitmap,
}

fn read_info(metadata: &Path, thin_id: u32) -> Result<ThinInfo> {
    let engine = Arc::new(SyncIoEngine::new_with(metadata, false, false)?);

    // Read metadata superblock
    let sb = read_superblock_snap(&*engine)?;

    // look up dev details, we don't actually have a lookup method, but the details
    // tree is small, so we slurp the whole lot.
    let details: DeviceDetail = {
        let mut path = vec![];
        let details: BTreeMap<u64, DeviceDetail> =
            btree_to_map(&mut path, engine.clone(), true, sb.details_root)?;
        let thin_id = thin_id as u64;
        if let Some(d) = details.get(&thin_id) {
            *d
        } else {
            return Err(anyhow!("couldn't find thin device with id {}", thin_id));
        }
    };

    // Build map of the dev mapping roots.  Again a btree lookup method
    // would be helpful.
    let mut path = vec![];
    let roots: BTreeMap<u64, u64> = btree_to_map(&mut path, engine.clone(), true, sb.mapping_root)?;

    // walk mapping tree
    let ignore_non_fatal = true;
    let walker = BTreeWalker::new(engine, ignore_non_fatal);
    let collector = MappingCollector::default();
    let mut path = vec![];
    let sub_tree = roots.get(&(thin_id as u64)).unwrap();
    walker.walk(&mut path, &collector, *sub_tree)?;
    let provisioned_blocks = collector.provisioned();

    Ok(ThinInfo {
        thin_id,
        data_block_size: sb.data_block_size,
        details,
        provisioned_blocks,
    })
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct DeltaInfo {
    pub thin_id: u32,
    pub data_block_size: u32,
    pub details: DeviceDetail,
    pub additions: RoaringBitmap,
    pub removals: RoaringBitmap,
}

fn read_delta_info(metadata: &Path, old_thin_id: u32, new_thin_id: u32) -> Result<DeltaInfo> {
    let engine = Arc::new(SyncIoEngine::new_with(metadata, false, false)?);

    // Read metadata superblock
    let sb = read_superblock_snap(&*engine)?;

    // look up dev details, we don't actually have a lookup method, but the details
    // tree is small, so we slurp the whole lot.
    let details: DeviceDetail = {
        let mut path = vec![];
        let details: BTreeMap<u64, DeviceDetail> =
            btree_to_map(&mut path, engine.clone(), true, sb.details_root)?;
        let thin_id = new_thin_id as u64;
        if let Some(d) = details.get(&thin_id) {
            *d
        } else {
            return Err(anyhow!("couldn't find thin device with id {}", thin_id));
        }
    };

    // Build map of the dev mapping roots.  Again a btree lookup method
    // would be helpful.
    let mut path = vec![];
    let roots: BTreeMap<u64, u64> = btree_to_map(&mut path, engine.clone(), true, sb.mapping_root)?;

    // This is slow, we need to walk both trees simultaneously to do delta properly.
    let old_root = roots.get(&(old_thin_id as u64)).unwrap();
    let old_mappings: BTreeMap<u64, BlockTime> =
        btree_to_map(&mut path, engine.clone(), true, *old_root)?;

    let new_root = roots.get(&(new_thin_id as u64)).unwrap();
    let new_mappings: BTreeMap<u64, BlockTime> = btree_to_map(&mut path, engine, true, *new_root)?;

    let mut additions = RoaringBitmap::default();
    let mut removals = RoaringBitmap::default();
    for (k, v1) in &old_mappings {
        if let Some(v2) = new_mappings.get(k) {
            if *v2 == *v1 {
                // mapping hasn't changed
            } else {
                additions.insert(*k as u32);
            }
        } else {
            // unmapped
            removals.insert(*k as u32);
        }
    }

    for k in new_mappings.keys() {
        if old_mappings.get(k).is_none() {
            additions.insert(*k as u32);
        }
    }

    Ok(DeltaInfo {
        thin_id: new_thin_id,
        data_block_size: sb.data_block_size,
        details,
        additions,
        removals,
    })
}

//---------------------------------

#[derive(Debug)]
struct ThinDetails {
    pool_major: u32,
    pool_minor: u32,
    id: u32,
}

fn parse_dev(input: &str) -> IResult<&str, (u32, u32)> {
    use nom::character::complete::*;

    let (input, major) = u32(input)?;
    let (input, _) = char(':')(input)?;
    let (input, minor) = u32(input)?;

    Ok((input, (major, minor)))
}

fn parse_thin_table(input: &str) -> IResult<&str, ThinDetails> {
    use nom::character::complete::*;

    let (input, (pool_major, pool_minor)) = parse_dev(input)?;
    let (input, _) = multispace1(input)?;
    let (input, id) = u32(input)?;

    Ok((
        input,
        ThinDetails {
            pool_major,
            pool_minor,
            id,
        },
    ))
}

#[allow(dead_code)]
#[derive(Debug)]
struct PoolDetails {
    metadata_major: u32,
    metadata_minor: u32,
    data_block_size: u32,
}

fn parse_pool_table(input: &str) -> IResult<&str, PoolDetails> {
    use nom::character::complete::*;

    let (input, (metadata_major, metadata_minor)) = parse_dev(input)?;
    let (input, _) = multispace1(input)?;
    let (input, (_data_major, _data_minor)) = parse_dev(input)?;
    let (input, _) = multispace1(input)?;
    let (input, data_block_size) = u32(input)?;

    Ok((
        input,
        PoolDetails {
            metadata_major,
            metadata_minor,
            data_block_size,
        },
    ))
}

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

    // Get the major:minor of the device at the given path
    let rdev = metadata.rdev();
    let thin_major = (rdev >> 8) as u32;
    let thin_minor = (rdev & 0xff) as u32;
    let thin_name = dm_devs.get(&(thin_major, thin_minor)).unwrap().clone();
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
    let r = read_info(&metadata_path, thin_details.id);
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
    let r = read_delta_info(&metadata_path, old_thin_details.id, new_thin_details.id);
    dm.target_msg(&pool_id, None, "release_metadata_snap")?;

    r
}

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

pub fn migrate(opts: ThinMigrateOptions) -> Result<()> {
    todo!();
}

//------------------------------------------
