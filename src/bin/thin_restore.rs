extern crate clap;
extern crate thinp;

use atty::Stream;
use clap::{App, Arg};
use std::path::Path;
use std::process;
use std::process::exit;
use std::sync::Arc;
use thinp::file_utils;
use thinp::report::*;
use thinp::thin::restore::{restore, ThinRestoreOptions};

fn main() {
    let parser = App::new("thin_restore")
        .version(thinp::version::tools_version())
        .about("Convert XML format metadata to binary.")
        .arg(
            Arg::with_name("ASYNC_IO")
                .help("Force use of io_uring for synchronous io")
                .long("async-io")
                .hidden(true),
        )
        .arg(
            Arg::with_name("OVERRIDE_MAPPING_ROOT")
                .help("Specify a mapping root to use")
                .long("override-mapping-root")
                .value_name("OVERRIDE_MAPPING_ROOT")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("INPUT")
                .help("Specify the input xml")
                .short("i")
                .long("input")
                .value_name("INPUT")
                .required(true),
        )
        .arg(
            Arg::with_name("OUTPUT")
                .help("Specify the output device to check")
                .short("o")
                .long("output")
                .value_name("OUTPUT")
                .required(true),
        );

    let matches = parser.get_matches();
    let input_file = Path::new(matches.value_of("INPUT").unwrap());
    let output_file = Path::new(matches.value_of("OUTPUT").unwrap());

    if !file_utils::file_exists(input_file) {
        eprintln!("Couldn't find input file '{:?}'.", &input_file);
        exit(1);
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
        println!("{}", reason);
        process::exit(1);
    }
}
