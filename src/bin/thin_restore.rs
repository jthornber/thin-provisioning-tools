extern crate clap;
extern crate thinp;

use atty::Stream;
use clap::{App, Arg};
use std::path::Path;
use std::process;
use std::sync::Arc;
use thinp::file_utils;
use thinp::report::*;
use thinp::thin::restore::{restore, ThinRestoreOptions};

fn main() {
    let parser = App::new("thin_restore")
        .version(thinp::version::tools_version())
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
                .help("Specify the output device")
                .short("o")
                .long("output")
                .value_name("FILE")
                .required(true),
        );

    let matches = parser.get_matches();
    let input_file = Path::new(matches.value_of("INPUT").unwrap());
    let output_file = Path::new(matches.value_of("OUTPUT").unwrap());

    if let Err(e) = file_utils::is_file(input_file) {
        eprintln!("Invalid input file '{}': {}.", input_file.display(), e);
        process::exit(1);
    }

    if let Err(e) = file_utils::check_output_file_requirements(output_file) {
        eprintln!("{}", e);
        process::exit(1);
    }

    let report;

    if matches.is_present("QUIET") {
        report = std::sync::Arc::new(mk_quiet_report());
    } else if atty::is(Stream::Stdout) {
        report = std::sync::Arc::new(mk_progress_bar_report());
    } else {
        report = Arc::new(mk_simple_report());
    }

    let opts = ThinRestoreOptions {
        input: &input_file,
        output: &output_file,
        async_io: matches.is_present("ASYNC_IO"),
        report,
    };

    if let Err(reason) = restore(opts) {
        eprintln!("{}", reason);
        process::exit(1);
    }
}
