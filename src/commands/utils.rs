use anyhow::{anyhow, Result};
use atty::Stream;
use clap::ArgMatches;
use std::fs::OpenOptions;
use std::io::Read;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use crate::checksum::{metadata_block_type, BT};
use crate::file_utils;
use crate::report::*;

pub fn check_input_file(input_file: &Path) -> Result<&Path> {
    match file_utils::is_file_or_blk(input_file) {
        Ok(true) => Ok(input_file),
        Ok(false) => Err(anyhow!(
            "Not a block device or regular file '{}'.",
            input_file.display()
        )),
        Err(e) => {
            if let Some(libc::ENOENT) = e.raw_os_error() {
                Err(anyhow!(
                    "Couldn't find input file '{}'",
                    input_file.display()
                ))
            } else {
                Err(anyhow!("Invalid output file: {}", e))
            }
        }
    }
}

pub fn check_file_not_tiny(input_file: &Path) -> Result<&Path> {
    match file_utils::file_size(input_file) {
        Ok(0..=4095) => Err(anyhow!(
            "Metadata device/file too small.  Is this binary metadata?"
        )),
        Ok(4096..) => Ok(input_file),
        Err(e) => Err(anyhow!("Couldn't get file size: {}", e)),
    }
}

pub fn check_output_file(path: &Path) -> Result<&Path> {
    // minimal thin metadata size is 10 blocks, with one device
    match file_utils::file_size(path) {
        Ok(0..=40959) => Err(anyhow!("Output file too small.")),
        Ok(40960..) => Ok(path),
        Err(e) => {
            if let Some(libc::ENOENT) = e.raw_os_error() {
                Err(anyhow!("Couldn't find output file '{}'", path.display()))
            } else {
                Err(anyhow!("Invalid output file: {}", e))
            }
        }
    }
}

pub fn mk_report(quiet: bool) -> std::sync::Arc<Report> {
    if quiet {
        Arc::new(mk_quiet_report())
    } else if atty::is(Stream::Stderr) {
        Arc::new(mk_progress_bar_report())
    } else {
        Arc::new(mk_simple_report())
    }
}

fn is_xml(line: &[u8]) -> bool {
    line.starts_with(b"<superblock") || line.starts_with(b"?xml") || line.starts_with(b"<!DOCTYPE")
}

pub fn is_xml_file(input_file: &Path) -> Result<bool> {
    let mut file = OpenOptions::new().read(true).open(input_file)?;
    let mut data = vec![0; 16];
    file.read_exact(&mut data)?;
    Ok(is_xml(&data))
}

/// This tries to read the start of input_path to see
/// if it's xml.  If there are any problems reading the file
/// then it fails silently.
pub fn check_not_xml(input_file: &Path) -> Result<&Path> {
    match is_xml_file(input_file) {
        Ok(true) => Err(anyhow!(
            "This looks like XML.  This tool only supports the binary metadata format."
        )),
        _ => Ok(input_file),
    }
}

pub fn is_metadata(path: &Path) -> Result<bool> {
    let mut file = OpenOptions::new().read(true).open(path)?;
    let mut sb = vec![0; 4096];
    file.read_exact(&mut sb)?;
    match metadata_block_type(&sb) {
        BT::THIN_SUPERBLOCK | BT::CACHE_SUPERBLOCK | BT::ERA_SUPERBLOCK => Ok(true),
        _ => Ok(false),
    }
}

pub fn yes_no_prompt(report: &Report, prompt: &str) -> Result<bool> {
    report
        .get_prompt_input(&format!("{} [y/n]: ", prompt))
        .map(|input| {
            let input = input.trim_end().to_lowercase();
            input.eq("yes") || input.eq("y")
        })
        .map_err(|e| e.into())
}

/// Reads the start of the file to see if it's a metadata.
/// Might fail silently if there are any problems reading the file,
/// e.g., permission denied or IO errors.
pub fn check_overwrite_metadata(report: &Report, path: &Path) -> Result<()> {
    let prompt = "The destintation appears to already contain metadata, are you sure?";

    if matches!(file_utils::is_file_or_blk(path), Ok(true))
        && matches!(is_metadata(path), Ok(true))
        && !matches!(yes_no_prompt(report, prompt), Ok(true))
    {
        return Err(anyhow!("Output file not overwritten"));
    }

    Ok(()) // file not found or not a metadata, or 'y' is entered
}

pub fn optional_value_or_exit<R>(matches: &ArgMatches, name: &str) -> Option<R>
where
    R: FromStr,
    <R as FromStr>::Err: std::fmt::Display,
{
    if matches.is_present(name) {
        Some(matches.value_of_t_or_exit::<R>(name))
    } else {
        None
    }
}

pub fn to_exit_code<T>(report: &Report, result: anyhow::Result<T>) -> exitcode::ExitCode {
    if let Err(e) = result {
        if e.chain().len() > 1 {
            report.fatal(&format!("{}: {}", e, e.root_cause()));
        } else {
            report.fatal(&format!("{}", e));
        }

        // FIXME: we need a way of getting more meaningful error codes
        exitcode::USAGE
    } else {
        exitcode::OK
    }
}

//---------------------------------------
