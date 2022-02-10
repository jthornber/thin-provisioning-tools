extern crate clap;

use clap::{App, Arg};
use std::path::Path;
use std::process;

use crate::commands::utils::*;
use crate::era::dump::{dump, EraDumpOptions};

//------------------------------------------

pub fn run(args: &[std::ffi::OsString]) {
    let parser = App::new("era_dump")
        .color(clap::ColorChoice::Never)
        .version(crate::version::tools_version())
        .about("Dump the era metadata to stdout in XML format")
        // flags
        .arg(
            Arg::new("ASYNC_IO")
                .help("Force use of io_uring for synchronous io")
                .long("async-io")
                .hide(true),
        )
        .arg(
            Arg::new("LOGICAL")
                .help("Fold any unprocessed write sets into the final era array")
                .long("logical"),
        )
        .arg(
            Arg::new("REPAIR")
                .help("Repair the metadata whilst dumping it")
                .short('r')
                .long("repair"),
        )
        // options
        .arg(
            Arg::new("OUTPUT")
                .help("Specify the output file rather than stdout")
                .short('o')
                .long("output")
                .value_name("FILE"),
        )
        // arguments
        .arg(
            Arg::new("INPUT")
                .help("Specify the input device to dump")
                .required(true)
                .index(1),
        );

    let matches = parser.get_matches_from(args);
    let input_file = Path::new(matches.value_of("INPUT").unwrap());
    let output_file = if matches.is_present("OUTPUT") {
        Some(Path::new(matches.value_of("OUTPUT").unwrap()))
    } else {
        None
    };

    // Create a temporary report just in case these checks
    // need to report anything.
    let report = std::sync::Arc::new(crate::report::mk_simple_report());
    check_input_file(input_file, &report);
    check_file_not_tiny(input_file, &report);
    drop(report);

    let opts = EraDumpOptions {
        input: input_file,
        output: output_file,
        async_io: matches.is_present("ASYNC_IO"),
        logical: matches.is_present("LOGICAL"),
        repair: matches.is_present("REPAIR"),
    };

    if let Err(reason) = dump(opts) {
        eprintln!("{}", reason);
        process::exit(1);
    }
}

//------------------------------------------
