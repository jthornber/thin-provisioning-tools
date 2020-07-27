extern crate clap;
extern crate thinp;

use clap::{App, Arg};
use std::path::Path;
use std::process;
use thinp::file_utils;

use std::process::exit;

fn main() {
    let parser = App::new("thin_check")
        .version(thinp::version::TOOLS_VERSION)
        .about("Validates thin provisioning metadata on a device or file.")
        .arg(
            Arg::with_name("QUIET")
                .help("Suppress output messages, return only exit code.")
                .short("q")
                .long("quiet")
                .value_name("QUIET"),
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
        );

    let matches = parser.get_matches();
    let input_file = Path::new(matches.value_of("INPUT").unwrap());

    if !file_utils::file_exists(input_file) {
        eprintln!("Couldn't find input file '{:?}'.", &input_file);
        exit(1);
    }

    if let Err(reason) = thinp::thin::check::check(&input_file) {
        println!("Application error: {}", reason);
        process::exit(1);
    }
}
