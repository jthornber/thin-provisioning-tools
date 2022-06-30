use anyhow::Result;
use atty::Stream;
use clap::ArgMatches;
use std::fs::OpenOptions;
use std::io::Read;
use std::path::Path;
use std::process::exit;
use std::str::FromStr;
use std::sync::Arc;

use crate::file_utils;
use crate::report::*;

pub fn check_input_file(input_file: &Path, report: &Report) {
    if !file_utils::file_exists(input_file) {
        report.fatal(&format!("Couldn't find input file '{:?}'.", &input_file));
        exit(1);
    }

    if !file_utils::is_file_or_blk(input_file) {
        report.fatal(&format!(
            "Not a block device or regular file '{:?}'.",
            &input_file
        ));
        exit(1);
    }
}

pub fn check_file_not_tiny(input_file: &Path, report: &Report) {
    if file_utils::file_size(input_file).expect("couldn't get input size") < 4096 {
        report.fatal("Metadata device/file too small.  Is this binary metadata?");
        exit(1);
    }
}

pub fn check_output_file(path: &Path, report: &Report) {
    // minimal thin metadata size is 10 blocks, with one device
    match file_utils::file_size(path) {
        Ok(size) => {
            if size < 40960 {
                report.fatal("Output file too small.");
                exit(1);
            }
        }
        Err(e) => {
            report.fatal(&format!("{}", e));
            exit(1);
        }
    }
}

pub fn mk_report(quiet: bool) -> std::sync::Arc<Report> {
    if quiet {
        Arc::new(mk_quiet_report())
    } else if atty::is(Stream::Stdout) {
        Arc::new(mk_progress_bar_report())
    } else {
        Arc::new(mk_simple_report())
    }
}

fn is_xml(line: &[u8]) -> bool {
    line.starts_with(b"<superblock") || line.starts_with(b"?xml") || line.starts_with(b"<!DOCTYPE")
}

pub fn check_not_xml_(input_file: &Path) -> Result<bool> {
    let mut file = OpenOptions::new().read(true).open(input_file)?;
    let mut data = vec![0; 16];
    file.read_exact(&mut data)?;
    Ok(is_xml(&data))
}

/// This trys to read the start of input_path to see
/// if it's xml.  If there are any problems reading the file
/// then it fails silently.
pub fn check_not_xml(input_file: &Path, report: &Report) {
    if let Ok(true) = check_not_xml_(input_file) {
        report.fatal("This looks like XML.  This tool only supports the binary metadata format.");
        exit(1);
    }
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

pub fn to_exit_code<T>(report: &Arc<Report>, result: anyhow::Result<T>) -> exitcode::ExitCode {
    if let Err(e) = result {
        report.fatal(&format!("{}", e));

        /*
        report.fatal(&format!("{:?}", e));
        report.fatal(
            "metadata contains errors (run cache_check for details).\n\
            perhaps you wanted to run with --repair ?",
        );
        */

        // FIXME: we need a way of getting more meaningful error codes
        exitcode::USAGE
    } else {
        exitcode::OK
    }
}

//---------------------------------------
