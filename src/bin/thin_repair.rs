extern crate clap;
extern crate thinp;

use atty::Stream;
use clap::{App, Arg};
use std::path::Path;
use std::process;
use std::sync::Arc;
use thinp::file_utils;
use thinp::report::*;
use thinp::thin::metadata_repair::SuperblockOverrides;
use thinp::thin::repair::{repair, ThinRepairOptions};

fn main() {
    let parser = App::new("thin_repair")
        .version(thinp::version::tools_version())
        .about("Repair thin-provisioning metadata, and write it to different device or file")
        // flags
        .arg(
            Arg::with_name("ASYNC_IO")
                .help("Force use of io_uring for synchronous io")
                .long("async-io")
                .hidden(true),
        )
        .arg(
            Arg::with_name("QUIET")
                .help("Suppress output messages, return only exit code.")
                .short("q")
                .long("quiet"),
        )
        // options
        .arg(
            Arg::with_name("DATA_BLOCK_SIZE")
                .help("Provide the data block size for repairing")
                .long("data-block-size")
                .value_name("SECTORS"),
        )
        .arg(
            Arg::with_name("INPUT")
                .help("Specify the input device")
                .short("i")
                .long("input")
                .value_name("FILE")
                .required(true),
        )
        .arg(
            Arg::with_name("NR_DATA_BLOCKS")
                .help("Override the number of data blocks if needed")
                .long("nr-data-blocks")
                .value_name("NUM"),
        )
        .arg(
            Arg::with_name("OUTPUT")
                .help("Specify the output device")
                .short("o")
                .long("output")
                .value_name("FILE")
                .required(true),
        )
        .arg(
            Arg::with_name("TRANSACTION_ID")
                .help("Override the transaction id if needed")
                .long("transaction-id")
                .value_name("NUM"),
        );

    let matches = parser.get_matches();
    let input_file = Path::new(matches.value_of("INPUT").unwrap());
    let output_file = Path::new(matches.value_of("OUTPUT").unwrap());

    if let Err(e) = file_utils::is_file_or_blk(input_file) {
        eprintln!("Invalid input file '{}': {}.", input_file.display(), e);
        process::exit(1);
    }

    let transaction_id = matches.value_of("TRANSACTION_ID").map(|s| {
        s.parse::<u64>().unwrap_or_else(|_| {
            eprintln!("Couldn't parse transaction_id");
            process::exit(1);
        })
    });

    let data_block_size = matches.value_of("DATA_BLOCK_SIZE").map(|s| {
        s.parse::<u32>().unwrap_or_else(|_| {
            eprintln!("Couldn't parse data_block_size");
            process::exit(1);
        })
    });

    let nr_data_blocks = matches.value_of("NR_DATA_BLOCKS").map(|s| {
        s.parse::<u64>().unwrap_or_else(|_| {
            eprintln!("Couldn't parse nr_data_blocks");
            process::exit(1);
        })
    });

    let report;

    if matches.is_present("QUIET") {
        report = std::sync::Arc::new(mk_quiet_report());
    } else if atty::is(Stream::Stdout) {
        report = std::sync::Arc::new(mk_progress_bar_report());
    } else {
        report = Arc::new(mk_simple_report());
    }

    let opts = ThinRepairOptions {
        input: &input_file,
        output: &output_file,
        async_io: matches.is_present("ASYNC_IO"),
        report,
        overrides: SuperblockOverrides {
            transaction_id,
            data_block_size,
            nr_data_blocks,
        },
    };

    if let Err(reason) = repair(opts) {
        eprintln!("{}", reason);
        process::exit(1);
    }
}
