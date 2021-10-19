extern crate clap;

use crate::file_utils;
use clap::{App, Arg};
use std::path::Path;
use std::process;

use std::process::exit;

pub fn run(args: &[std::ffi::OsString]) {
    let parser = App::new("thin_metadata_unpack")
        .version(crate::version::tools_version())
        .about("Unpack a compressed file of thin metadata.")
        .arg(
            Arg::with_name("INPUT")
                .help("Specify thinp metadata binary device/file")
                .required(true)
                .short("i")
                .value_name("DEV")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("OUTPUT")
                .help("Specify packed output file")
                .required(true)
                .short("o")
                .value_name("FILE")
                .takes_value(true),
        );

    let matches = parser.get_matches_from(args);
    let input_file = Path::new(matches.value_of("INPUT").unwrap());
    let output_file = Path::new(matches.value_of("OUTPUT").unwrap());

    if !file_utils::is_file(input_file) {
        eprintln!("Invalid input file '{}'.", input_file.display());
        exit(1);
    }

    if let Err(reason) = crate::pack::toplevel::unpack(input_file, output_file) {
        eprintln!("Application error: {}", reason);
        process::exit(1);
    }
}
