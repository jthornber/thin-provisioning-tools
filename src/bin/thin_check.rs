extern crate clap;
extern crate thinp;

use atty::Stream;
use clap::{App, Arg};
use std::path::Path;
use std::process;
use thinp::file_utils;
use thinp::thin::check::{check, ThinCheckOptions};
use std::sync::Arc;
use std::process::exit;
use thinp::report::*;

fn main() {
    let parser = App::new("thin_check")
        .version(thinp::version::TOOLS_VERSION)
        .about("Validates thin provisioning metadata on a device or file.")
        .arg(
            Arg::with_name("QUIET")
                .help("Suppress output messages, return only exit code.")
                .short("q")
                .long("quiet")
        )
        .arg(
            Arg::with_name("SB_ONLY")
                .help("Only check the superblock.")
                .long("super-block-only")
                .value_name("SB_ONLY"),
        )
        .arg(
            Arg::with_name("ignore-non-fatal-errors")
                .help("Only return a non-zero exit code if a fatal error is found.")
                .long("ignore-non-fatal-errors")
                .value_name("IGNORE_NON_FATAL"),
        )
        .arg(
            Arg::with_name("clear-needs-check-flag")
                .help("Clears the 'needs_check' flag in the superblock")
                .long("clear-needs-check")
                .value_name("CLEAR_NEEDS_CHECK"),
        )
        .arg(
            Arg::with_name("OVERRIDE_MAPPING_ROOT")
                .help("Specify a mapping root to use")
                .long("override-mapping-root")
                .value_name("OVERRIDE_MAPPING_ROOT")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("METADATA_SNAPSHOT")
                .help("Check the metadata snapshot on a live pool")
                .short("m")
                .long("metadata-snapshot")
                .value_name("METADATA_SNAPSHOT"),
        )
        .arg(
            Arg::with_name("INPUT")
                .help("Specify the input device to check")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::with_name("SYNC_IO")
                .help("Force use of synchronous io")
                .long("sync-io")
                .value_name("SYNC_IO")
                .takes_value(false),
        );

    let matches = parser.get_matches();
    let input_file = Path::new(matches.value_of("INPUT").unwrap());

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

    let opts = ThinCheckOptions {
        dev: &input_file,
        async_io: !matches.is_present("SYNC_IO"),
        report
    };

    if let Err(reason) = check(opts) {
        println!("Application error: {}", reason);
        process::exit(1);
    }
}
