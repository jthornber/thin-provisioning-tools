extern crate clap;
extern crate thinp;

use clap::{App, Arg};
use std::path::Path;
use std::process::exit;
use thinp::file_utils;

fn main() {
    let parser = App::new("thin_metadata_pack")
	.version(thinp::version::tools_version())
        .about("Produces a compressed file of thin metadata.  Only packs metadata blocks that are actually used.")
        .arg(Arg::with_name("INPUT")
            .help("Specify thinp metadata binary device/file")
            .required(true)
            .short("i")
            .value_name("DEV")
            .takes_value(true))
        .arg(Arg::with_name("OUTPUT")
            .help("Specify packed output file")
            .required(true)
            .short("o")
            .value_name("FILE")
            .takes_value(true));

    let matches = parser.get_matches();
    let input_file = Path::new(matches.value_of("INPUT").unwrap());
    let output_file = Path::new(matches.value_of("OUTPUT").unwrap());

    if let Err(e) = file_utils::is_file_or_blk(input_file) {
        eprintln!("Invalid input file '{}': {}.", input_file.display(), e);
        exit(1);
    }

    if let Err(reason) = thinp::pack::toplevel::pack(&input_file, &output_file) {
        println!("Application error: {}\n", reason);
        exit(1);
    }
}
