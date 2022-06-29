use nix::sys::stat;
use nix::sys::stat::{FileStat, SFlag};
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Seek, Write};
use std::os::unix::io::AsRawFd;
use std::path::Path;
use tempfile::tempfile;

//---------------------------------------

fn test_bit(mode: u32, flag: SFlag) -> bool {
    SFlag::from_bits_truncate(mode).contains(flag)
}

pub fn is_file_or_blk_(info: FileStat) -> bool {
    test_bit(info.st_mode, SFlag::S_IFBLK) || test_bit(info.st_mode, SFlag::S_IFREG)
}

pub fn file_exists(path: &Path) -> bool {
    matches!(stat::stat(path), Ok(_))
}

pub fn is_file_or_blk(path: &Path) -> bool {
    match stat::stat(path) {
        Ok(info) => is_file_or_blk_(info),
        _ => false,
    }
}

pub fn is_file(path: &Path) -> bool {
    match stat::stat(path) {
        Ok(info) => test_bit(info.st_mode, SFlag::S_IFREG),
        _ => false,
    }
}

//---------------------------------------

const BLKGETSIZE64_CODE: u8 = 0x12;
const BLKGETSIZE64_SEQ: u8 = 114;
ioctl_read!(ioctl_blkgetsize64, BLKGETSIZE64_CODE, BLKGETSIZE64_SEQ, u64);

pub fn fail<T>(msg: &str) -> io::Result<T> {
    let e = io::Error::new(io::ErrorKind::Other, msg);
    Err(e)
}

fn get_device_size<P: AsRef<Path>>(path: P) -> io::Result<u64> {
    let file = File::open(path.as_ref())?;
    let fd = file.as_raw_fd();
    let mut cap = 0u64;
    unsafe {
        match ioctl_blkgetsize64(fd, &mut cap) {
            Ok(_) => Ok(cap),
            _ => fail("BLKGETSIZE64 ioctl failed"),
        }
    }
}

pub fn file_size<P: AsRef<Path>>(path: P) -> io::Result<u64> {
    match stat::stat(path.as_ref()) {
        Ok(info) => {
            if test_bit(info.st_mode, SFlag::S_IFREG) {
                Ok(info.st_size as u64)
            } else if test_bit(info.st_mode, SFlag::S_IFBLK) {
                get_device_size(path)
            } else {
                fail("Not a block device or regular file")
            }
        }
        _ => fail("stat failed"),
    }
}

//---------------------------------------

fn set_size<W: Write + Seek>(w: &mut W, nr_bytes: u64) -> io::Result<()> {
    let zeroes: Vec<u8> = vec![0; 1];

    if nr_bytes > 0 {
        w.seek(io::SeekFrom::Start(nr_bytes - 1))?;
        w.write_all(&zeroes)?;
    }

    Ok(())
}

pub fn temp_file_sized(nr_bytes: u64) -> io::Result<std::fs::File> {
    let mut file = tempfile()?;
    set_size(&mut file, nr_bytes)?;
    Ok(file)
}

pub fn create_sized_file(path: &Path, nr_bytes: u64) -> io::Result<std::fs::File> {
    let mut file = OpenOptions::new()
        .read(false)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;
    set_size(&mut file, nr_bytes)?;
    Ok(file)
}

//---------------------------------------
