use nix::sys::stat;
use nix::sys::stat::{FileStat, SFlag};
use std::fs::File;
use std::io;
use std::io::{Seek, Write};
use std::os::unix::io::AsRawFd;
use tempfile::tempfile;

//---------------------------------------

fn check_bits(mode: u32, flag: &SFlag) -> bool {
    (mode & flag.bits()) != 0
}

pub fn is_file_or_blk(info: FileStat) -> bool {
    check_bits(info.st_mode, &stat::SFlag::S_IFBLK)
        || check_bits(info.st_mode, &stat::SFlag::S_IFREG)
}

pub fn file_exists(path: &str) -> bool {
    match stat::stat(path) {
        Ok(info) => is_file_or_blk(info),
        _ => {
            // FIXME: assuming all errors indicate the file doesn't
            // exist.
            false
        }
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

fn get_device_size(path: &str) -> io::Result<u64> {
    let file = File::open(path)?;
    let fd = file.as_raw_fd();
    let mut cap = 0u64;
    unsafe {
        match ioctl_blkgetsize64(fd, &mut cap) {
            Ok(_) => Ok(cap),
            _ => fail("BLKGETSIZE64 ioctl failed"),
        }
    }
}

pub fn file_size(path: &str) -> io::Result<u64> {
    match stat::stat(path) {
        Ok(info) => {
            if check_bits(info.st_mode, &SFlag::S_IFREG) {
                Ok(info.st_size as u64)
            } else if check_bits(info.st_mode, &SFlag::S_IFBLK) {
                get_device_size(path)
            } else {
                fail("not a regular file or block device")
            }
        }
        _ => fail("stat failed"),
    }
}

//---------------------------------------

pub fn temp_file_sized(nr_bytes: u64) -> io::Result<std::fs::File> {
    let mut file = tempfile()?;

    let zeroes: Vec<u8> = vec![0; 1];

    if nr_bytes > 0 {
        file.seek(io::SeekFrom::Start(nr_bytes - 1))?;
        file.write_all(&zeroes)?;
    }

    Ok(file)
}

//---------------------------------------
