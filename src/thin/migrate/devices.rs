use anyhow::{anyhow, Result};
use devicemapper::*;
use nom::IResult;
use std::collections::*;
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::path::PathBuf;
use udev::Enumerator;

use std::fs::File;

//---------------------------------

#[derive(Debug)]
struct PoolTable {
    metadata_major: u32,
    metadata_minor: u32,
    data_block_size: u32,
}

#[derive(Debug)]
struct ThinTable {
    pool_major: u32,
    pool_minor: u32,
    thin_id: u32,
}

fn parse_dev(input: &str) -> IResult<&str, (u32, u32)> {
    use nom::character::complete::*;

    let (input, major) = u32(input)?;
    let (input, _) = char(':')(input)?;
    let (input, minor) = u32(input)?;

    Ok((input, (major, minor)))
}

fn parse_thin_table(input: &str) -> IResult<&str, ThinTable> {
    use nom::character::complete::*;

    let (input, (pool_major, pool_minor)) = parse_dev(input)?;
    let (input, _) = multispace1(input)?;
    let (input, thin_id) = u32(input)?;

    Ok((
        input,
        ThinTable {
            pool_major,
            pool_minor,
            thin_id,
        },
    ))
}

fn parse_pool_table(input: &str) -> IResult<&str, PoolTable> {
    use nom::character::complete::*;

    let (input, (metadata_major, metadata_minor)) = parse_dev(input)?;
    let (input, _) = multispace1(input)?;
    let (input, (_data_major, _data_minor)) = parse_dev(input)?;
    let (input, _) = multispace1(input)?;
    let (input, data_block_size) = u32(input)?;

    Ok((
        input,
        PoolTable {
            metadata_major,
            metadata_minor,
            data_block_size,
        },
    ))
}

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

/// When contructed this scans udev to build a mapping from
/// device number to DmName.  It does not rescan, so the contents
/// can be out of date.
struct DmIndex {
    /// Maps (major, minor) -> dm name
    by_nr: BTreeMap<(u32, u32), DmNameBuf>,
}

impl DmIndex {
    fn new(dm: &mut DM) -> Result<Self> {
        let mut by_nr = BTreeMap::new();
        for (name, dev, _) in dm.list_devices()? {
            by_nr.insert((dev.major, dev.minor), name);
        }

        Ok(Self { by_nr })
    }

    fn get(&self, major: u32, minor: u32) -> Option<&DmNameBuf> {
        self.by_nr.get(&(major, minor))
    }

    fn get_by_rdev(&self, rdev: u64) -> Option<&DmNameBuf> {
        let (major, minor) = split_device_number(rdev);
        self.get(major, minor)
    }
}

//---------------------------------

pub struct ThinDeviceScanner {
    dm: DM,
    index: DmIndex,
}

impl ThinDeviceScanner {
    pub fn new() -> Result<Self> {
        let mut dm = DM::new().map_err(|e| anyhow!("Failed to initialize Device Mapper: {}", e))?;
        let index = DmIndex::new(&mut dm)?;

        Ok(Self { dm, index })
    }

    pub fn is_thin_device(&self, thin: &File) -> Result<u32> {
        let metadata = thin.metadata()?;

        if !metadata.file_type().is_block_device() {
            // Not a block device
            return Ok(false);
        }

        if let Some(dm_name) = self.index.get_by_rdev(metadata.rdev()) {
            match self.get_table(dm_name, "thin") {
                Ok(thin_args) => {
                    let (_, table) = parse_thin_table(&thin_args)?;
                    Ok(table.thin_id)
                }
                Err(_e) => Ok(false),
            }
        } else {
            // Not a dm device
            Ok(false)
        }
    }

    fn get_table(&mut self, dev_name: &DmName, expected_target_type: &str) -> Result<String> {
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

    fn get_by_rdev(&self, rdev: u64) -> Result<&DmNameBuf> {
        if let Some(name) = self.index.get_by_rdev(rdev) {
            Ok(name)
        } else {
            Err(anyhow!("not a dm device"))
        }
    }

    pub fn get_thin_details(&self, thin: &File) -> Result<ThinTable> {
        let metadata = thin.metadata()?;
        if !metadata.file_type().is_block_device() {
            return Err(anyhow!("Thin is not a block device"));
        }

        let thin_name = self.get_by_rdev(metadata.rdev())?;
        let thin_args = self.get_table(thin_name, "thin")?;
        let (_, thin_details) =
            parse_thin_table(&thin_args).map_err(|_| anyhow!("couldn't parse thin table"))?;

        Ok(thin_details)
    }

    pub fn get_thin_name(&self, thin: &File) -> Result<&DmNameBuf> {
        let metadata = thin.metadata()?;

        if !metadata.file_type().is_block_device() {
            return Err(anyhow!("'{:?}' is not a block device", thin));
        }

        self.get_by_rdev(metadata.rdev())
    }
}

// FIXME: see how this is being used and see if we can lose this fn
pub fn find_device(major: u32, minor: u32) -> Result<Option<PathBuf>> {
    let mut enumerator = Enumerator::new()?;
    let device_path = enumerator
        .scan_devices()?
        .filter_map(|device| {
            device
                .devnum()
                .map(|rdev| (device, split_device_number(rdev)))
        })
        .find(|&(_, (found_major, found_minor))| found_major == major && found_minor == minor)
        .map(|(device, _)| device.devnode().map(PathBuf::from))
        .flatten();

    Ok(device_path)
}

//------------------------------------------
