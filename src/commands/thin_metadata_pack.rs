extern crate clap;

use clap::{App, Arg};
use std::path::Path;
use std::process;

use crate::commands::utils::*;
use crate::report::*;

pub fn run(args: &[std::ffi::OsString]) {
    let parser = App::new("thin_metadata_pack")
	.version(crate::version::tools_version())
        .about("Produces a compressed file of thin metadata.  Only packs metadata blocks that are actually used.")
        .arg(Arg::with_name("INPUT")
            .help("Specify thinp metadata binary device/file")
            .required(true)
            .short("i")
            .value_name("DEV")
            .takes_value(true))
        .arg(Arg::with_name("OUTPUT")
            .help("Specify packed output file")
            .required(true)
            .short("o")
            .value_name("FILE")
            .takes_value(true));

    let matches = parser.get_matches_from(args);
    let input_file = Path::new(matches.value_of("INPUT").unwrap());
    let output_file = Path::new(matches.value_of("OUTPUT").unwrap());

    let report = mk_simple_report();
    check_input_file(input_file, &report);

    if let Err(reason) = crate::pack::toplevel::pack(input_file, output_file) {
        report.fatal(&format!("Application error: {}\n", reason));
        process::exit(1);
    }
}
