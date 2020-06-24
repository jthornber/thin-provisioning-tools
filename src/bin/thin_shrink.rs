extern crate clap;
extern crate thinp;

use clap::{App, Arg};
use std::process::exit;
use thinp::file_utils;

fn main() {
    let parser = App::new("thin_shrink")
	.version(thinp::version::TOOLS_VERSION)
        .about("Rewrite xml metadata and move data in an inactive pool.")
        .arg(Arg::with_name("INPUT")
            .help("Specify thinp metadata xml file")
            .required(true)
            .long("input")
            .value_name("INPUT")
            .takes_value(true))
        .arg(Arg::with_name("OUTPUT")
            .help("Specify output xml file")
            .required(true)
            .long("output")
            .value_name("OUTPUT")
            .takes_value(true));

    let matches = parser.get_matches();

    // FIXME: check these look like xml
    let input_file = matches.value_of("INPUT").unwrap();
    let output_file = matches.value_of("OUTPUT").unwrap();

    if !file_utils::file_exists(input_file) {
        eprintln!("Couldn't find input file '{}'.", &input_file);
        exit(1);
    }

    if let Err(reason) = thinp::shrink::toplevel::shrink(&input_file, &output_file) {
        println!("Application error: {}\n", reason);
        exit(1);
    }
}
