use anyhow::{anyhow, Result};
use devicemapper::*;
use nom::IResult;
use std::collections::*;
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::path::PathBuf;
use udev::Enumerator;

use std::fs::File;

//---------------------------------

const MAJOR_BITSHIFT: u64 = 8;
const MINOR_MASK: u64 = 0xff;

/// Splits into (major, minor)
fn split_device_number(rdev: u64) -> (u32, u32) {
    let major = (rdev >> MAJOR_BITSHIFT) as u32;
    let minor = (rdev & MINOR_MASK) as u32;

    (major, minor)
}

//---------------------------------

/// A struct representing the major and minor numbers of a Linux block device.
///
/// The `DeviceNr` struct provides methods to construct instances from either
/// explicit major and minor numbers or a combined device number (`rdev`).
#[derive(Debug, Ord, PartialOrd, Eq, PartialEq)]
pub struct DeviceNr {
    major: u32,
    minor: u32,
}

impl DeviceNr {
    pub fn new(major: u32, minor: u32) -> Self {
        Self { major, minor }
    }

    pub fn from_rdev(rdev: u64) -> Self {
        let (major, minor) = split_device_number(rdev);
        Self { major, minor }
    }
}

impl From<Device> for DeviceNr {
    fn from(dev: Device) -> Self {
        DeviceNr {
            major: dev.major,
            minor: dev.minor,
        }
    }
}

//---------------------------------

/// When constructed this scans udev to build a mapping from
/// device number to DmName.  It does not rescan, so the contents
/// can be out of date.
struct DmIndex {
    /// Maps (major, minor) -> dm name
    by_nr: BTreeMap<DeviceNr, DmNameBuf>,
}

impl DmIndex {
    fn new(dm: &mut DM) -> Result<Self> {
        let mut by_nr = BTreeMap::new();
        for (name, dev, _) in dm.list_devices()? {
            by_nr.insert(DeviceNr::new(dev.major, dev.minor), name);
        }

        Ok(Self { by_nr })
    }

    fn get(&self, dev: &DeviceNr) -> Option<&DmNameBuf> {
        self.by_nr.get(dev)
    }
}

//---------------------------------

/// Fields from the pool target line
#[derive(Debug)]
pub struct PoolTable {
    pub metadata_dev: DeviceNr,
    pub data_dev: DeviceNr,
    pub data_block_size: u32,
}

/// Fields from the thin target line
#[derive(Debug)]
pub struct ThinTable {
    pub pool_dev: DeviceNr,
    pub thin_id: u32,
}

fn parse_dev(input: &str) -> IResult<&str, DeviceNr> {
    use nom::character::complete::*;

    let (input, major) = u32(input)?;
    let (input, _) = char(':')(input)?;
    let (input, minor) = u32(input)?;

    Ok((input, DeviceNr { major, minor }))
}

fn parse_thin_table(input: &str) -> IResult<&str, ThinTable> {
    use nom::character::complete::*;

    let (input, pool_dev) = parse_dev(input)?;
    let (input, _) = multispace1(input)?;
    let (input, thin_id) = u32(input)?;

    Ok((input, ThinTable { pool_dev, thin_id }))
}

fn parse_pool_table(input: &str) -> IResult<&str, PoolTable> {
    use nom::character::complete::*;

    let (input, metadata_dev) = parse_dev(input)?;
    let (input, _) = multispace1(input)?;
    let (input, data_dev) = parse_dev(input)?;
    let (input, _) = multispace1(input)?;
    let (input, data_block_size) = u32(input)?;

    Ok((
        input,
        PoolTable {
            metadata_dev,
            data_dev,
            data_block_size,
        },
    ))
}

//---------------------------------

#[derive(Debug)]
pub struct DmInfo {
    pub name: String,
    pub uuid: String,

    pub dev: DeviceNr,
    pub open_count: i32,
    pub event_nr: u32,

    pub suspended: bool,
    pub read_only: bool,
    pub live_table: bool,
    pub inactive_table: bool,
    pub deferred_remove: bool,
    pub internal_suspended: bool,
}

impl From<DeviceInfo> for DmInfo {
    fn from(info: DeviceInfo) -> DmInfo {
        let name = info.name().map_or_else(Default::default, |v| v.to_string());
        let uuid = info.uuid().map_or_else(Default::default, |v| v.to_string());
        let dev = info.device().into();
        let open_count = info.open_count();
        let event_nr = info.event_nr();

        let flags = info.flags();
        let suspended = flags.contains(DmFlags::DM_SUSPEND);
        let read_only = flags.contains(DmFlags::DM_READONLY);
        let live_table = flags.contains(DmFlags::DM_ACTIVE_PRESENT);
        let inactive_table = flags.contains(DmFlags::DM_INACTIVE_PRESENT);
        let deferred_remove = flags.contains(DmFlags::DM_DEFERRED_REMOVE);
        let internal_suspended = flags.contains(DmFlags::DM_INTERNAL_SUSPEND);

        // FIXME: missing the target_count since it is private in DeviceInfo
        DmInfo {
            name,
            uuid,
            dev,
            open_count,
            event_nr,
            suspended,
            read_only,
            live_table,
            inactive_table,
            deferred_remove,
            internal_suspended,
        }
    }
}

//---------------------------------

/// Manages scanning and interaction with Device Mapper (DM) devices.
///
/// `DmScanner` initializes a connection to the DM subsystem and builds an index
/// for quick lookup of DM device names by their device numbers. It provides methods
/// to retrieve DM device tables, convert device numbers to DM names, and resolve
/// device paths.
pub struct DmScanner {
    dm: DM,
    index: DmIndex,
}

impl DmScanner {
    pub fn new() -> Result<Self> {
        let mut dm = DM::new().map_err(|e| anyhow!("Failed to initialize Device Mapper: {}", e))?;
        let index = DmIndex::new(&mut dm)?;

        Ok(Self { dm, index })
    }

    /// Read the table of the given dm device and return as a string.
    pub fn get_table(&mut self, dev_name: &DmName, expected_target_type: &str) -> Result<String> {
        let dev = DevId::Name(dev_name);
        let opts = DmOptions::default().set_flags(DmFlags::DM_STATUS_TABLE);
        let (_info, table) = self.dm.table_status(&dev, opts)?;
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

    pub fn get_info(&mut self, dev_name: &DmName) -> Result<DeviceInfo> {
        let dev = DevId::Name(dev_name);
        self.dm.device_info(&dev).map_err(|e| e.into())
    }

    /// Convert a device nr to a DmNameBuf
    pub fn dev_to_name(&self, dev: &DeviceNr) -> Result<&DmNameBuf> {
        if let Some(name) = self.index.get(dev) {
            Ok(name)
        } else {
            Err(anyhow!("not a dm device"))
        }
    }

    /// Convert a File to a DmNameBuf
    pub fn file_to_name(&self, dev: &File) -> Result<&DmNameBuf> {
        let metadata = dev.metadata()?;

        if !metadata.file_type().is_block_device() {
            return Err(anyhow!("'{:?}' is not a block device", dev));
        }

        self.dev_to_name(&DeviceNr::from_rdev(metadata.rdev()))
    }

    /// Convert a device nr to a path.
    pub fn dev_to_path(&self, dev: &DeviceNr) -> Result<Option<PathBuf>> {
        let mut enumerator = Enumerator::new()?;
        let device_path = enumerator
            .scan_devices()?
            .filter_map(|device| {
                device
                    .devnum()
                    .map(|rdev| (device, split_device_number(rdev)))
            })
            .find(|&(_, (found_major, found_minor))| {
                found_major == dev.major && found_minor == dev.minor
            })
            .and_then(|(device, _)| device.devnode().map(PathBuf::from));

        Ok(device_path)
    }
}

//---------------------------------

/// Read and parse a thin table
pub fn get_thin_table(scanner: &mut DmScanner, dev_name: &DmName) -> Result<ThinTable> {
    let thin_args = scanner.get_table(dev_name, "thin")?;
    let (_, thin_table) =
        parse_thin_table(&thin_args).map_err(|_| anyhow!("couldn't parse thin table"))?;
    Ok(thin_table)
}

/// Read and parse a pool table
pub fn get_pool_table(scanner: &mut DmScanner, dev_name: &DmName) -> Result<PoolTable> {
    let pool_args = scanner.get_table(dev_name, "thin-pool")?;
    let (_, pool_table) =
        parse_pool_table(&pool_args).map_err(|_| anyhow!("couldn't parse pool table"))?;
    Ok(pool_table)
}

pub fn get_device_info(scanner: &mut DmScanner, dev_name: &DmName) -> Result<DmInfo> {
    scanner.get_info(dev_name).map(|info| info.into())
}

//------------------------------------------
