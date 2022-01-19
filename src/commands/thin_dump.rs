extern crate clap;

use atty::Stream;
use clap::{App, Arg};
use std::path::Path;
use std::process;
use std::sync::Arc;

use crate::commands::utils::*;
use crate::report::*;
use crate::thin::dump::{dump, ThinDumpOptions};
use crate::thin::metadata_repair::SuperblockOverrides;

pub fn run(args: &[std::ffi::OsString]) {
    let parser = App::new("thin_dump")
        .color(clap::ColorChoice::Never)
        .version(crate::version::tools_version())
        .about("Dump thin-provisioning metadata to stdout in XML format")
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
        .arg(
            Arg::new("REPAIR")
                .help("Repair the metadata whilst dumping it")
                .short('r')
                .long("repair")
                .conflicts_with("METADATA_SNAPSHOT"),
        )
        .arg(
            Arg::new("SKIP_MAPPINGS")
                .help("Do not dump the mappings")
                .long("skip-mappings"),
        )
        // options
        .arg(
            Arg::new("DATA_BLOCK_SIZE")
                .help("Provide the data block size for repairing")
                .long("data-block-size")
                .value_name("SECTORS"),
        )
        .arg(
            Arg::new("METADATA_SNAPSHOT")
                .help("Access the metadata snapshot on a live pool")
                .short('m')
                .long("metadata-snapshot")
                .value_name("METADATA_SNAPSHOT"),
        )
        .arg(
            Arg::new("NR_DATA_BLOCKS")
                .help("Override the number of data blocks if needed")
                .long("nr-data-blocks")
                .value_name("NUM"),
        )
        .arg(
            Arg::new("OUTPUT")
                .help("Specify the output file rather than stdout")
                .short('o')
                .long("output")
                .value_name("FILE"),
        )
        .arg(
            Arg::new("TRANSACTION_ID")
                .help("Override the transaction id if needed")
                .long("transaction-id")
                .value_name("NUM"),
        )
        // arguments
        .arg(
            Arg::new("INPUT")
                .help("Specify the input device to dump")
                .required(true)
                .index(1),
        );

    let matches = parser.get_matches_from(args);
    let input_file = Path::new(matches.value_of("INPUT").unwrap());
    let output_file = if matches.is_present("OUTPUT") {
        Some(Path::new(matches.value_of("OUTPUT").unwrap()))
    } else {
        None
    };

    let report = std::sync::Arc::new(mk_simple_report());
    check_input_file(input_file, &report);

    let transaction_id = if matches.is_present("TRANSACTION_ID") {
        Some(matches.value_of_t_or_exit::<u64>("TRANSACTION_ID"))
    } else {
        None
    };

    let data_block_size = if matches.is_present("DATA_BLOCK_SIZE") {
        Some(matches.value_of_t_or_exit::<u32>("DATA_BLOCK_SIZE"))
    } else {
        None
    };

    let nr_data_blocks = if matches.is_present("NR_DATA_BLOCKS") {
        Some(matches.value_of_t_or_exit::<u64>("NR_DATA_BLOCKS"))
    } else {
        None
    };

    let report;

    if matches.is_present("QUIET") {
        report = std::sync::Arc::new(mk_quiet_report());
    } else if atty::is(Stream::Stdout) {
        report = std::sync::Arc::new(mk_progress_bar_report());
    } else {
        report = Arc::new(mk_simple_report());
    }

    let opts = ThinDumpOptions {
        input: input_file,
        output: output_file,
        async_io: matches.is_present("ASYNC_IO"),
        report: report.clone(),
        repair: matches.is_present("REPAIR"),
        use_metadata_snap: matches.is_present("METADATA_SNAPSHOT"),
        overrides: SuperblockOverrides {
            transaction_id,
            data_block_size,
            nr_data_blocks,
        },
    };

    if let Err(reason) = dump(opts) {
        report.fatal(&format!("{}", reason));
        process::exit(1);
    }
}
