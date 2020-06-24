use anyhow::Result;
use std::fs::OpenOptions;
use std::os::unix::fs::OpenOptionsExt;

use crate::shrink::xml;

//---------------------------------------

pub fn shrink(input_file: &str, _output_file: &str, _map_file: &str) -> Result<()> {
    let input = OpenOptions::new()
        .read(true)
        .write(false)
        .custom_flags(libc::O_EXCL)
        .open(input_file)?;

    let mut visitor = xml::XmlWriter::new(std::io::stdout());
    xml::read(input, &mut visitor)?;

    Ok(())
}

//---------------------------------------
