extern crate clap;

use clap::{App, Arg};
use std::path::Path;
use std::process;

use crate::commands::utils::check_input_file;
use crate::report::mk_simple_report;

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

    let report = mk_simple_report();
    check_input_file(input_file, &report);

    if let Err(reason) = crate::pack::toplevel::unpack(input_file, output_file) {
        report.fatal(&format!("Application error: {}", reason));
        process::exit(1);
    }
}
