extern crate clap;
extern crate thinp;

use atty::Stream;
use clap::{App, Arg};
use std::path::Path;
use std::sync::Arc;

use thinp::report::*;
use thinp::cache::check::{check, CacheCheckOptions};

//------------------------------------------

fn main() {
    let parser = App::new("cache_check")
        .version(thinp::version::TOOLS_VERSION)
        .arg(
            Arg::with_name("INPUT")
                .help("Specify the input device to check")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::with_name("SB_ONLY")
                .help("Only check the superblock.")
                .long("super-block-only")
                .value_name("SB_ONLY"),
        )
        .arg(
            Arg::with_name("SKIP_MAPPINGS")
                .help("Don't check the mapping array")
                .long("skip-mappings")
                .value_name("SKIP_MAPPINGS"),
        )
        .arg(
            Arg::with_name("SKIP_HINTS")
                .help("Don't check the hint array")
                .long("skip-hints")
                .value_name("SKIP_HINTS"),
        )
        .arg(
            Arg::with_name("SKIP_DISCARDS")
                .help("Don't check the discard bitset")
                .long("skip-discards")
                .value_name("SKIP_DISCARDS"),
        )
        .arg(
            Arg::with_name("QUIET")
                .help("Suppress output messages, return only exit code.")
                .short("q")
                .long("quiet"),
        );

    let matches = parser.get_matches();
    let input_file = Path::new(matches.value_of("INPUT").unwrap());

    let report;
    if matches.is_present("QUIET") {
        report = std::sync::Arc::new(mk_quiet_report());
    } else if atty::is(Stream::Stdout) {
        report = std::sync::Arc::new(mk_progress_bar_report());
    } else {
        report = Arc::new(mk_simple_report());
    }

    let opts = CacheCheckOptions {
        dev: &input_file,
        async_io: false,
        sb_only: matches.is_present("SB_ONLY"),
        skip_mappings: matches.is_present("SKIP_MAPPINGS"),
        skip_hints: matches.is_present("SKIP_HINTS"),
        skip_discards: matches.is_present("SKIP_DISCARDS"),
        report,
    };

    if let Err(reason) = check(opts) {
        eprintln!("{}", reason);
        std::process::exit(1);
    }
}

//------------------------------------------
