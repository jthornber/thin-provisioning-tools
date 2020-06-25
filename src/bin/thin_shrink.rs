extern crate clap;
extern crate thinp;

use clap::{App, Arg};
use std::process::exit;
use thinp::file_utils;

fn main() {
    let parser = App::new("thin_shrink")
        .version(thinp::version::TOOLS_VERSION)
        .about("Rewrite xml metadata and move data in an inactive pool.")
        .arg(
            Arg::with_name("INPUT")
                .help("Specify thinp metadata xml file")
                .required(true)
                .long("input")
                .value_name("INPUT")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("OUTPUT")
                .help("Specify output xml file")
                .required(true)
                .long("output")
                .value_name("OUTPUT")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("DATA")
                .help("Specify pool data device where data will be moved")
                .required(true)
                .long("data")
                .value_name("DATA")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("SIZE")
                .help("Specify new size for the pool (in data blocks)")
                .required(true)
                .long("nr-blocks")
                .value_name("SIZE")
                .takes_value(true),
        );

    let matches = parser.get_matches();

    // FIXME: check these look like xml
    let input_file = matches.value_of("INPUT").unwrap();
    let output_file = matches.value_of("OUTPUT").unwrap();
    let size = matches.value_of("SIZE").unwrap().parse::<u64>().unwrap();
    let data_file = matches.value_of("DATA").unwrap();

    if !file_utils::file_exists(input_file) {
        eprintln!("Couldn't find input file '{}'.", &input_file);
        exit(1);
    }

    if let Err(reason) = thinp::shrink::toplevel::shrink(&input_file, &output_file, &data_file, size) {
        println!("Application error: {}\n", reason);
        exit(1);
    }
}
