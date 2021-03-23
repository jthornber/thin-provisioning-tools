extern crate clap;
extern crate thinp;

use clap::{App, Arg};
use std::path::Path;
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
        );

    let matches = parser.get_matches();
    let input_file = Path::new(matches.value_of("INPUT").unwrap());

    let opts = CacheCheckOptions {
        dev: &input_file,
        async_io: false,
        sb_only: matches.is_present("SB_ONLY"),
        skip_mappings: matches.is_present("SKIP_MAPPINGS"),
        skip_hints: matches.is_present("SKIP_HINTS"),
        skip_discards: matches.is_present("SKIP_DISCARDS"),
    };

    if let Err(reason) = check(opts) {
        eprintln!("{}", reason);
        std::process::exit(1);
    }
}

//------------------------------------------
