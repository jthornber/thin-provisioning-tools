use nix::sys::stat;
use nix::sys::stat::{FileStat, SFlag};
use std::io;
use std::fs::File;
use std::os::unix::io::AsRawFd;

//---------------------------------------

fn check_bits(mode: u32, flag: &SFlag) -> bool {
    (mode & flag.bits()) != 0
}

pub fn is_file_or_blk(info: FileStat) -> bool {
    check_bits(info.st_mode, &stat::SFlag::S_IFBLK) ||
        check_bits(info.st_mode, &stat::SFlag::S_IFREG)
}

pub fn file_exists(path: &str) -> bool {
    match stat::stat(path) {
        Ok(info) => {
            return is_file_or_blk(info);
        }
        _ => {
            // FIXME: assuming all errors indicate the file doesn't
            // exist.
            eprintln!("couldn't stat '{}'", path);
            return false;
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
    let _file = File::open(path)?; 
    let fd = File::open(path).unwrap().as_raw_fd();
    let mut cap = 0u64;
    unsafe {
       match ioctl_blkgetsize64(fd, &mut cap) {
           Ok(_) => {return Ok(cap);}
           _ => {return fail("BLKGETSIZE64 ioctl failed");}
       }
    }
}

pub fn file_size(path: &str) -> io::Result<u64> {
    match stat::stat(path) {
        Ok(info) => {
            if check_bits(info.st_mode, &SFlag::S_IFREG) {
                return Ok(info.st_size as u64);
            } else if check_bits(info.st_mode, &SFlag::S_IFBLK) {
                return get_device_size(path);
            } else {
                return fail("not a regular file or block device");
            } 
        }
        _ => {
            return fail("stat failed");
        }
    }    
}

//---------------------------------------
