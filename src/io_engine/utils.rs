use anyhow::{anyhow, Result};
use iovec::{unix, IoVec};
use std::fs::File;
use std::os::unix::io::AsRawFd;

///-------------------------------------

// All the individual buffers are assumed to be the same size.  If io fails, the first block
// will be marked as errored, and the io will be retried from the subsequent block.
pub fn read_blocks<'a>(
    src: &File,
    buffers: &'a mut [&'a mut [u8]],
    mut pos: u64,
) -> Result<Vec<Result<()>>> {
    let block_size = buffers[0].len();
    let mut remaining = 0;
    let mut bufs: Vec<&'a mut IoVec> = Vec::with_capacity(buffers.len());
    for b in buffers.iter_mut() {
        assert_eq!(b.len(), block_size);
        remaining += b.len();
        bufs.push((*b).into());
    }
    let mut os_bufs = unix::as_os_slice_mut(&mut bufs[..]);
    let mut results = Vec::with_capacity(os_bufs.len());

    while remaining > 0 {
        let ptr = os_bufs.as_ptr();
        let n = unsafe { libc::preadv64(src.as_raw_fd(), ptr, os_bufs.len() as i32, pos as i64) };

        if n > 0 {
            remaining -= n as usize;
            pos += n as u64;
            assert_eq!(n as usize % block_size, 0);
            os_bufs = &mut os_bufs[(n as usize / block_size)..];
            for _ in 0..(n as usize / block_size) {
                results.push(Ok(()));
            }
        } else {
            // Skip to the next iovec
            remaining -= block_size;
            pos += block_size as u64;
            os_bufs = &mut os_bufs[1..];
            results.push(Err(anyhow!("read failed")));
        }
    }

    Ok(results)
}

pub fn write_blocks<'a>(
    src: &File,
    buffers: &'a [&'a [u8]],
    mut pos: u64,
) -> Result<Vec<Result<()>>> {
    let block_size = buffers[0].len();
    let mut remaining = 0;
    let mut bufs: Vec<&'a IoVec> = Vec::with_capacity(buffers.len());
    for b in buffers.iter() {
        assert_eq!(b.len(), block_size);
        remaining += b.len();
        bufs.push((*b).into());
    }
    let mut os_bufs = unix::as_os_slice(&bufs[..]);
    let mut results = Vec::with_capacity(os_bufs.len());

    while remaining > 0 {
        let ptr = os_bufs.as_ptr();
        let n = unsafe { libc::pwritev64(src.as_raw_fd(), ptr, os_bufs.len() as i32, pos as i64) };

        if n > 0 {
            remaining -= n as usize;
            pos += n as u64;
            assert_eq!(n as usize % block_size, 0);
            os_bufs = &os_bufs[(n as usize / block_size)..];
            for _ in 0..(n as usize / block_size) {
                results.push(Ok(()));
            }
        } else {
            // Skip to the next iovec
            remaining -= block_size;
            pos += block_size as u64;
            os_bufs = &os_bufs[1..];
            results.push(Err(anyhow!("read failed")));
        }
    }

    Ok(results)
}

//-------------------------------------
