extern crate clap;

use atty::Stream;
use clap::{App, Arg};
use std::path::Path;
use std::process;
use std::sync::Arc;

use crate::commands::utils::*;
use crate::era::repair::{repair, EraRepairOptions};
use crate::report::*;

pub fn run(args: &[std::ffi::OsString]) {
    let parser = App::new("era_repair")
        .color(clap::ColorChoice::Never)
        .version(crate::version::tools_version())
        .about("Repair binary era metadata, and write it to a different device or file")
        // flags
        .arg(
            Arg::new("ASYNC_IO")
                .help("Force use of io_uring for synchronous io")
                .long("async-io")
                .hide(true),
        )
        .arg(
            Arg::new("QUIET")
                .help("Suppress output messages, return only exit code.")
                .short('q')
                .long("quiet"),
        )
        // options
        .arg(
            Arg::new("INPUT")
                .help("Specify the input device")
                .short('i')
                .long("input")
                .value_name("FILE")
                .required(true),
        )
        .arg(
            Arg::new("OUTPUT")
                .help("Specify the output device")
                .short('o')
                .long("output")
                .value_name("FILE")
                .required(true),
        );

    let matches = parser.get_matches_from(args);
    let input_file = Path::new(matches.value_of("INPUT").unwrap());
    let output_file = Path::new(matches.value_of("OUTPUT").unwrap());

    let report = if matches.is_present("QUIET") {
        std::sync::Arc::new(mk_quiet_report())
    } else if atty::is(Stream::Stdout) {
        std::sync::Arc::new(mk_progress_bar_report())
    } else {
        Arc::new(mk_simple_report())
    };

    check_input_file(input_file, &report);

    let opts = EraRepairOptions {
        input: input_file,
        output: output_file,
        async_io: matches.is_present("ASYNC_IO"),
        report: report.clone(),
    };

    if let Err(reason) = repair(opts) {
        report.fatal(&format!("{}", reason));
        process::exit(1);
    }
}
