use nix::sys::stat;
use nix::sys::stat::FileStat;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Seek, Write};
use std::os::unix::io::AsRawFd;
use std::path::Path;
use tempfile::tempfile;

//---------------------------------------

#[inline(always)]
pub fn s_isreg(info: &FileStat) -> bool {
    (info.st_mode & stat::SFlag::S_IFMT.bits()) == stat::SFlag::S_IFREG.bits()
}

#[inline(always)]
pub fn s_isblk(info: &FileStat) -> bool {
    (info.st_mode & stat::SFlag::S_IFMT.bits()) == stat::SFlag::S_IFBLK.bits()
}

pub fn is_file(path: &Path) -> io::Result<()> {
    match stat::stat(path) {
        Ok(info) => {
            if s_isreg(&info) {
                Ok(())
            } else {
                fail("Not a regular file")
            }
        }
        _ => {
            // FIXME: assuming all errors indicate the file doesn't
            // exist.
            fail("No such file or directory")
        }
    }
}

pub fn is_file_or_blk(path: &Path) -> io::Result<()> {
    match stat::stat(path) {
        Ok(info) => {
            if s_isreg(&info) || s_isblk(&info) {
                Ok(())
            } else {
                fail("Not a block device or regular file")
            }
        }
        _ => {
            // FIXME: assuming all errors indicate the file doesn't
            // exist.
            fail("No such file or directory")
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

fn get_device_size(path: &Path) -> io::Result<u64> {
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

pub fn file_size(path: &Path) -> io::Result<u64> {
    match stat::stat(path) {
        Ok(info) => {
            if s_isreg(&info) {
                Ok(info.st_size as u64)
            } else if s_isblk(&info) {
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

pub fn check_output_file_requirements(path: &Path) -> io::Result<()> {
    // minimal thin metadata size is 10 blocks, with one device
    if file_size(path)? < 40960 {
        return fail("Output file too small.");
    }
    Ok(())
}

//---------------------------------------
