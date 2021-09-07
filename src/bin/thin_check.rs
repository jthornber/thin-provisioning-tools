extern crate clap;
extern crate thinp;

use atty::Stream;
use clap::{App, Arg};
use std::path::Path;
use std::process;
use std::sync::Arc;
use thinp::file_utils;
use thinp::io_engine::*;
use thinp::report::*;
use thinp::thin::check::{check, ThinCheckOptions, MAX_CONCURRENT_IO};

fn main() {
    let parser = App::new("thin_check")
        .version(thinp::version::tools_version())
        .about("Validates thin provisioning metadata on a device or file.")
        // flags
        .arg(
            Arg::with_name("ASYNC_IO")
                .help("Force use of io_uring for synchronous io")
                .long("async-io")
                .hidden(true),
        )
        .arg(
            Arg::with_name("AUTO_REPAIR")
                .help("Auto repair trivial issues.")
                .long("auto-repair")
                .conflicts_with_all(&[
                    "IGNORE_NON_FATAL",
                    "METADATA_SNAPSHOT",
                    "OVERRIDE_MAPPING_ROOT",
                    "SB_ONLY",
                    "SKIP_MAPPINGS",
                ]),
        )
        .arg(
            // Using --clear-needs-check along with --skip-mappings is allowed
            // (but not recommended) for backward compatibility (commit 1fe8a0d)
            Arg::with_name("CLEAR_NEEDS_CHECK")
                .help("Clears the 'needs_check' flag in the superblock")
                .long("clear-needs-check-flag")
                .conflicts_with_all(&[
                    "IGNORE_NON_FATAL",
                    "METADATA_SNAPSHOT",
                    "OVERRIDE_MAPPING_ROOT",
                    "SB_ONLY",
                ]),
        )
        .arg(
            Arg::with_name("IGNORE_NON_FATAL")
                .help("Only return a non-zero exit code if a fatal error is found.")
                .long("ignore-non-fatal-errors"),
        )
        .arg(
            Arg::with_name("METADATA_SNAPSHOT")
                .help("Check the metadata snapshot on a live pool")
                .short("m")
                .long("metadata-snapshot"),
        )
        .arg(
            Arg::with_name("QUIET")
                .help("Suppress output messages, return only exit code.")
                .short("q")
                .long("quiet"),
        )
        .arg(
            Arg::with_name("SB_ONLY")
                .help("Only check the superblock.")
                .long("super-block-only"),
        )
        .arg(
            Arg::with_name("SKIP_MAPPINGS")
                .help("Don't check the mapping tree")
                .long("skip-mappings"),
        )
        // options
        .arg(
            Arg::with_name("OVERRIDE_MAPPING_ROOT")
                .help("Specify a mapping root to use")
                .long("override-mapping-root")
                .value_name("OVERRIDE_MAPPING_ROOT")
                .takes_value(true),
        )
        // arguments
        .arg(
            Arg::with_name("INPUT")
                .help("Specify the input device to check")
                .required(true)
                .index(1),
        );

    let matches = parser.get_matches();
    let input_file = Path::new(matches.value_of("INPUT").unwrap());

    if let Err(e) = file_utils::is_file_or_blk(input_file) {
        eprintln!("Invalid input file '{}': {}.", input_file.display(), e);
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

    let engine: Arc<dyn IoEngine + Send + Sync>;

    if matches.is_present("ASYNC_IO") {
        engine = Arc::new(
            AsyncIoEngine::new(&input_file, MAX_CONCURRENT_IO, false)
                .expect("unable to open input file"),
        );
    } else {
        let nr_threads = std::cmp::max(8, num_cpus::get() * 2);
        engine = Arc::new(
            SyncIoEngine::new(&input_file, nr_threads, false).expect("unable to open input file"),
        );
    }

    let opts = ThinCheckOptions {
        engine,
        sb_only: matches.is_present("SB_ONLY"),
        skip_mappings: matches.is_present("SKIP_MAPPINGS"),
        ignore_non_fatal: matches.is_present("IGNORE_NON_FATAL"),
        auto_repair: matches.is_present("AUTO_REPAIR"),
        report,
    };

    if let Err(reason) = check(opts) {
        eprintln!("{}", reason);
        process::exit(1);
    }
}
