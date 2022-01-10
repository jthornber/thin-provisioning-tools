extern crate clap;

use clap::{App, Arg};
use std::path::Path;
use std::process;

use crate::commands::utils::*;
use crate::thin::trim::{trim, ThinTrimOptions};

//------------------------------------------

pub fn run(args: &[std::ffi::OsString]) {
    let parser = App::new("thin_trim")
        .version(crate::version::tools_version())
        .about("Issue discard requests for free pool space (offline tool).")
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
            Arg::with_name("METADATA_DEV")
                .help("Specify the pool metadata device")
                .long("metadata-dev")
                .value_name("FILE")
                .required(true),
        )
        .arg(
            Arg::with_name("DATA_DEV")
                .help("Specify the pool data device")
                .long("data-dev")
                .value_name("FILE")
                .required(true),
        );

    let matches = parser.get_matches_from(args);
    let metadata_dev = Path::new(matches.value_of("METADATA_DEV").unwrap());
    let data_dev = Path::new(matches.value_of("DATA_DEV").unwrap());

    let report = mk_report(matches.is_present("QUIET"));
    check_input_file(metadata_dev, &report);
    check_input_file(data_dev, &report);

    let opts = ThinTrimOptions {
        metadata_dev,
        data_dev,
        async_io: matches.is_present("ASYNC_IO"),
        report: report.clone(),
    };

    if let Err(reason) = trim(opts) {
        report.fatal(&format!("{}", reason));
        process::exit(1);
    }
}
