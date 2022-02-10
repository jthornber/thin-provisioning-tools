extern crate clap;

use clap::{App, Arg};
use std::path::Path;
use std::process;

use crate::commands::utils::*;
use crate::era::invalidate::{invalidate, EraInvalidateOptions};

//------------------------------------------

pub fn run(args: &[std::ffi::OsString]) {
    let parser = App::new("era_invalidate")
        .color(clap::ColorChoice::Never)
        .version(crate::version::tools_version())
        .about("List blocks that may have changed since a given era")
        // flags
        .arg(
            Arg::new("ASYNC_IO")
                .help("Force use of io_uring for synchronous io")
                .long("async-io")
                .hide(true),
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
        )
        .arg(
            Arg::new("WRITTEN_SINCE")
                .help("Blocks written since the given era will be listed")
                .long("written-since")
                .required(true)
                .value_name("ERA"),
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

    let threshold = matches
        .value_of("WRITTEN_SINCE")
        .map(|s| {
            s.parse::<u32>().unwrap_or_else(|_| {
                eprintln!("Couldn't parse written_since");
                process::exit(1);
            })
        })
        .unwrap_or(0);

    let opts = EraInvalidateOptions {
        input: input_file,
        output: output_file,
        async_io: matches.is_present("ASYNC_IO"),
        threshold,
        use_metadata_snap: matches.is_present("METADATA_SNAP"),
    };

    if let Err(reason) = invalidate(&opts) {
        eprintln!("{}", reason);
        process::exit(1);
    }
}

//------------------------------------------
