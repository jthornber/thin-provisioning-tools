extern crate clap;

use clap::{App, Arg};
use std::path::Path;
use std::process;

use crate::cache::restore::{restore, CacheRestoreOptions};
use crate::commands::utils::*;

pub fn run(args: &[std::ffi::OsString]) {
    let parser = App::new("cache_restore")
        .version(crate::version::tools_version())
        .about("Convert XML format metadata to binary.")
        // flags
        .arg(
            Arg::with_name("ASYNC_IO")
                .help("Force use of io_uring for synchronous io")
                .long("async-io")
                .hidden(true),
        )
        .arg(
            Arg::with_name("QUIET")
                .help("Suppress output messages, return only exit code.")
                .short("q")
                .long("quiet"),
        )
        // options
        .arg(
            Arg::with_name("INPUT")
                .help("Specify the input xml")
                .short("i")
                .long("input")
                .value_name("FILE")
                .required(true),
        )
        .arg(
            Arg::with_name("OUTPUT")
                .help("Specify the output device to check")
                .short("o")
                .long("output")
                .value_name("FILE")
                .required(true),
        );

    let matches = parser.get_matches_from(args);
    let input_file = Path::new(matches.value_of("INPUT").unwrap());
    let output_file = Path::new(matches.value_of("OUTPUT").unwrap());

    let report = mk_report(matches.is_present("QUIET"));
    check_input_file(input_file, &report);
    check_output_file(output_file, &report);

    let opts = CacheRestoreOptions {
        input: &input_file,
        output: &output_file,
        async_io: matches.is_present("ASYNC_IO"),
        report: report.clone(),
    };

    if let Err(reason) = restore(opts) {
        report.fatal(&format!("{}", reason));
        process::exit(1);
    }
}
