extern crate clap;
extern crate thinp;

use clap::{App, Arg};
use std::path::Path;
use thinp::cache::dump::{dump, CacheDumpOptions};

//------------------------------------------

fn main() {
    let parser = App::new("cache_dump")
        .version(thinp::version::TOOLS_VERSION)
        .arg(
            Arg::with_name("INPUT")
                .help("Specify the input device to check")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::with_name("REPAIR")
                .help("")
                .long("repair")
                .value_name("REPAIR"),
        );

    let matches = parser.get_matches();
    let input_file = Path::new(matches.value_of("INPUT").unwrap());

    let opts = CacheDumpOptions {
        dev: &input_file,
        async_io: false,
        repair: matches.is_present("REPAIR"),
    };

    if let Err(reason) = dump(opts) {
        eprintln!("{}", reason);
        std::process::exit(1);
    }
}

//------------------------------------------
