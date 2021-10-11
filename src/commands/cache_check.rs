extern crate clap;

use atty::Stream;
use clap::{App, Arg};
use std::path::Path;
use std::process;
use std::sync::Arc;

use crate::cache::check::{check, CacheCheckOptions};
use crate::report::*;
use crate::commands::utils::*;

//------------------------------------------

pub fn run(args: &[std::ffi::OsString]) {
    let parser = App::new("cache_check")
        .version(crate::version::tools_version())
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
                .long("auto-repair"),
        )
        .arg(
            Arg::with_name("IGNORE_NON_FATAL")
                .help("Only return a non-zero exit code if a fatal error is found.")
                .long("ignore-non-fatal-errors"),
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
            Arg::with_name("SKIP_HINTS")
                .help("Don't check the hint array")
                .long("skip-hints"),
        )
        .arg(
            Arg::with_name("SKIP_DISCARDS")
                .help("Don't check the discard bitset")
                .long("skip-discards"),
        )
        // arguments
        .arg(
            Arg::with_name("INPUT")
                .help("Specify the input device to check")
                .required(true)
                .index(1),
        );

    let matches = parser.get_matches_from(args);
    let input_file = Path::new(matches.value_of("INPUT").unwrap());

    let report = if matches.is_present("QUIET") {
        std::sync::Arc::new(mk_quiet_report())
    } else if atty::is(Stream::Stdout) {
        std::sync::Arc::new(mk_progress_bar_report())
    } else {
        Arc::new(mk_simple_report())
    };

    check_input_file(input_file, &report);
    check_file_not_tiny(input_file, &report);

    let opts = CacheCheckOptions {
        dev: &input_file,
        async_io: matches.is_present("ASYNC_IO"),
        sb_only: matches.is_present("SB_ONLY"),
        skip_mappings: matches.is_present("SKIP_MAPPINGS"),
        skip_hints: matches.is_present("SKIP_HINTS"),
        skip_discards: matches.is_present("SKIP_DISCARDS"),
        ignore_non_fatal: matches.is_present("IGNORE_NON_FATAL"),
        auto_repair: matches.is_present("AUTO_REPAIR"),
        report: report.clone(),
    };

    if let Err(reason) = check(opts) {
        report.fatal(&format!("{}", reason));
        process::exit(1);
    }
}

//------------------------------------------
