use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Seek, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::io::AsRawFd;
use std::path::Path;

use crate::ioctl::{self, *};

//---------------------------------------

fn test_bit(mode: u32, flag: u32) -> bool {
    (mode & libc::S_IFMT) == flag
}

fn is_file_or_blk_(info: &libc::stat64) -> bool {
    test_bit(info.st_mode, libc::S_IFBLK) || test_bit(info.st_mode, libc::S_IFREG)
}

// wrapper of libc::stat64
fn libc_stat64(path: &Path) -> io::Result<libc::stat64> {
    let c_path = std::ffi::CString::new(path.as_os_str().as_bytes())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;

    unsafe {
        let mut st: libc::stat64 = std::mem::zeroed();
        let r = libc::stat64(c_path.as_ptr(), &mut st);
        if r == 0 {
            Ok(st)
        } else {
            Err(io::Error::last_os_error())
        }
    }
}

pub fn is_file_or_blk(path: &Path) -> io::Result<bool> {
    libc_stat64(path).map(|info| is_file_or_blk_(&info))
}

pub fn is_file(path: &Path) -> io::Result<bool> {
    libc_stat64(path).map(|info| test_bit(info.st_mode, libc::S_IFREG))
}

//---------------------------------------

const BLKGETSIZE64: ioctl::RequestType = crate::request_code_read!(0x12, 114, usize);

pub fn fail<T>(msg: &str) -> io::Result<T> {
    let e = io::Error::new(io::ErrorKind::Other, msg);
    Err(e)
}

fn get_device_size<P: AsRef<Path>>(path: P) -> io::Result<u64> {
    let file = File::open(path.as_ref())?;
    let fd = file.as_raw_fd();
    let mut cap = 0u64;

    unsafe {
        if libc::ioctl(fd, BLKGETSIZE64, &mut cap) == 0 {
            Ok(cap)
        } else {
            Err(io::Error::last_os_error())
        }
    }
}

pub fn file_size<P: AsRef<Path>>(path: P) -> io::Result<u64> {
    libc_stat64(path.as_ref()).and_then(|info| {
        if test_bit(info.st_mode, libc::S_IFREG) {
            Ok(info.st_size as u64)
        } else if test_bit(info.st_mode, libc::S_IFBLK) {
            get_device_size(path)
        } else {
            fail("Not a block device or regular file")
        }
    })
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
