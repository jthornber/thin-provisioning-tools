// This work is based on the implementation by Nikhil Kshirsagar which
// can be found here:
//    https://github.com/nkshirsagar/thinpool_shrink/blob/split_ranges/thin_shrink.py

extern crate clap;

use clap::{Arg, Command};
use std::path::Path;
use std::process::exit;

use crate::commands::utils::*;

pub fn run(args: &[std::ffi::OsString]) {
    let parser = Command::new("thin_shrink")
        .color(clap::ColorChoice::Never)
        .version(crate::version::tools_version())
        .about("Rewrite xml metadata and move data in an inactive pool.")
        .arg(
            Arg::new("INPUT")
                .help("Specify thinp metadata xml file")
                .required(true)
                .short('i')
                .long("input")
                .value_name("FILE")
                .takes_value(true),
        )
        .arg(
            Arg::new("OUTPUT")
                .help("Specify output xml file")
                .required(true)
                .short('o')
                .long("output")
                .value_name("FILE")
                .takes_value(true),
        )
        .arg(
            Arg::new("DATA")
                .help("Specify pool data device where data will be moved")
                .required(true)
                .long("data")
                .value_name("DATA")
                .takes_value(true),
        )
        .arg(
            Arg::new("NOCOPY")
                .help("Skip the copying of data, useful for benchmarking")
                .required(false)
                .long("no-copy")
                .value_name("NOCOPY")
                .takes_value(false),
        )
        .arg(
            Arg::new("SIZE")
                .help("Specify new size for the pool (in data blocks)")
                .required(true)
                .long("nr-blocks")
                .value_name("SIZE")
                .takes_value(true),
        );

    let matches = parser.get_matches_from(args);

    // FIXME: check these look like xml
    let input_file = Path::new(matches.value_of("INPUT").unwrap());
    let output_file = Path::new(matches.value_of("OUTPUT").unwrap());
    let size = matches.value_of("SIZE").unwrap().parse::<u64>().unwrap();
    let data_file = Path::new(matches.value_of("DATA").unwrap());
    let do_copy = !matches.is_present("NOCOPY");

    let report = mk_report(false);
    check_input_file(input_file, &report);

    if let Err(reason) =
        crate::shrink::toplevel::shrink(input_file, output_file, data_file, size, do_copy)
    {
        eprintln!("Application error: {}\n", reason);
        exit(1);
    }
}
