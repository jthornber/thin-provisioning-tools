use anyhow::Result;
use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write, Read};
use std::os::unix::fs::OpenOptionsExt;

pub type Sector = u64;

#[derive(Debug)]
pub struct Region {
    pub src: Sector,
    pub dest: Sector,
    pub len: Sector,
}


fn copy_step<W>(file: &mut W, src_byte: u64, dest_byte: u64, len: usize) -> Result<()>
where
    W: Write + Seek + Read,
{
    let mut buf = vec![0; len];
    file.seek(SeekFrom::Start(src_byte))?;
    file.read_exact(&mut buf[0..])?;
    file.seek(SeekFrom::Start(dest_byte))?;
    file.write_all(&buf)?;
    Ok(())
}

fn copy_region<W>(file: &mut W, r: &Region) -> Result<()>
where
    W: Write + Seek + Read,
{
    const MAX_BYTES: Sector = 1024 * 1024 * 64;

    let src_bytes = r.src * 512;
    let dest_bytes = r.dest * 512;
    let len_bytes = r.len * 512;
    let mut written = 0;
    while written != len_bytes {
        let step = u64::min(len_bytes - written, MAX_BYTES);
        copy_step(file, src_bytes + written, dest_bytes + written, step as usize)?;
        written += step;
    }
    Ok(())
}

pub fn copy(path: &str, regions: &Vec<Region>) -> Result<()> {
    let mut input = OpenOptions::new()
        .read(true)
        .write(true)
        //.custom_flags(libc::O_DIRECT)
        .open(path)?;

    for r in regions {
        eprintln!("copying {:?}", r);
        copy_region(&mut input, r)?;
    }

    Ok(())
}
