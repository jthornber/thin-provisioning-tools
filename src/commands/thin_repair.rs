extern crate clap;

use clap::{Arg, Command};
use std::path::Path;
use std::process;

use crate::commands::utils::*;
use crate::thin::metadata_repair::SuperblockOverrides;
use crate::thin::repair::{repair, ThinRepairOptions};

pub fn run(args: &[std::ffi::OsString]) {
    let parser = Command::new("thin_repair")
        .color(clap::ColorChoice::Never)
        .version(crate::version::tools_version())
        .about("Repair thin-provisioning metadata, and write it to different device or file")
        // flags
        .arg(
            Arg::new("ASYNC_IO")
                .help("Force use of io_uring for synchronous io")
                .long("async-io")
                .hide(true),
        )
        .arg(
            Arg::new("QUIET")
                .help("Suppress output messages, return only exit code.")
                .short('q')
                .long("quiet"),
        )
        // options
        .arg(
            Arg::new("DATA_BLOCK_SIZE")
                .help("Provide the data block size for repairing")
                .long("data-block-size")
                .value_name("SECTORS"),
        )
        .arg(
            Arg::new("INPUT")
                .help("Specify the input device")
                .short('i')
                .long("input")
                .value_name("FILE")
                .required(true),
        )
        .arg(
            Arg::new("NR_DATA_BLOCKS")
                .help("Override the number of data blocks if needed")
                .long("nr-data-blocks")
                .value_name("NUM"),
        )
        .arg(
            Arg::new("OUTPUT")
                .help("Specify the output device")
                .short('o')
                .long("output")
                .value_name("FILE")
                .required(true),
        )
        .arg(
            Arg::new("TRANSACTION_ID")
                .help("Override the transaction id if needed")
                .long("transaction-id")
                .value_name("NUM"),
        );

    let matches = parser.get_matches_from(args);
    let input_file = Path::new(matches.value_of("INPUT").unwrap());
    let output_file = Path::new(matches.value_of("OUTPUT").unwrap());

    let report = mk_report(matches.is_present("QUIET"));
    check_input_file(input_file, &report);
    check_output_file(output_file, &report);

    let opts = ThinRepairOptions {
        input: input_file,
        output: output_file,
        async_io: matches.is_present("ASYNC_IO"),
        report: report.clone(),
        overrides: SuperblockOverrides {
            transaction_id: optional_value_or_exit::<u64>(&matches, "TRANSACTION_ID"),
            data_block_size: optional_value_or_exit::<u32>(&matches, "DATA_BLOCK_SIZE"),
            nr_data_blocks: optional_value_or_exit::<u64>(&matches, "NR_DATA_BLOCKS"),
        },
    };

    if let Err(reason) = repair(opts) {
        report.fatal(&format!("{}", reason));
        process::exit(1);
    }
}
