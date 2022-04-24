use clap::{Arg, ArgGroup, Command};
use std::path::Path;
use std::process;

use crate::thin::metadata_generator::*;

//------------------------------------------

pub fn run(args: &[std::ffi::OsString]) {
    let parser = Command::new("thin_generate_metadata")
        .color(clap::ColorChoice::Never)
        .version(crate::version::tools_version())
        .about("A tool for creating synthetic thin metadata.")
        // flags
        .arg(
            Arg::new("ASYNC_IO")
                .help("Force use of io_uring for synchronous io")
                .long("async-io")
                .hide(true),
        )
        .arg(
            Arg::new("FORMAT")
                .help("Format the metadata")
                .long("format")
                .group("commands"),
        )
        .arg(
            Arg::new("SET_NEEDS_CHECK")
                .help("Set the NEEDS_CHECK flag")
                .long("set-needs-check")
                .group("commands"),
        )
        // options
        .arg(
            Arg::new("DATA_BLOCK_SIZE")
                .help("Specify the data block size while formatting")
                .long("block-size")
                .value_name("SECTORS")
                .default_value("128"),
        )
        .arg(
            Arg::new("NR_DATA_BLOCKS")
                .help("Specify the number of data blocks")
                .long("nr-data-blocks")
                .value_name("NUM")
                .default_value("10240"),
        )
        .arg(
            Arg::new("OUTPUT")
                .help("Specify the output device")
                .short('o')
                .long("output")
                .value_name("FILE")
                .required(true),
        )
        .group(ArgGroup::new("commands").required(true));

    let matches = parser.get_matches_from(args);

    let opts = ThinGenerateOpts {
        async_io: matches.is_present("ASYNC_IO"),
        op: if matches.is_present("FORMAT") {
            MetadataOp::Format
        } else if matches.is_present("SET_NEEDS_CHECK") {
            MetadataOp::SetNeedsCheck
        } else {
            eprintln!("unknown option");
            process::exit(1);
        },
        data_block_size: matches.value_of_t_or_exit::<u32>("DATA_BLOCK_SIZE"),
        nr_data_blocks: matches.value_of_t_or_exit::<u64>("NR_DATA_BLOCKS"),
        output: Path::new(matches.value_of("OUTPUT").unwrap()),
    };

    if let Err(reason) = generate_metadata(opts) {
        eprintln!("{}", reason);
        process::exit(1)
    }
}

//------------------------------------------
