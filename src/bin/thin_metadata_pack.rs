extern crate clap;
extern crate thinp;

use clap::{App, Arg};
use std::process;

fn main() {
    let parser = App::new("thin_metadata_pack")
	.version(thinp::version::TOOLS_VERSION)
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
    let input_file = matches.value_of("INPUT").unwrap();
    let output_file = matches.value_of("OUTPUT").unwrap();

    if let Err(reason) = thinp::pack::pack::pack(&input_file, &output_file) {
        println!("Application error: {}\n", reason);
        process::exit(1);
    }
}
