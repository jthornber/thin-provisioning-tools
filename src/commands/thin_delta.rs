extern crate clap;

use clap::{value_t_or_exit, App, Arg, ArgGroup};
use std::path::Path;
use std::process;

use crate::commands::utils::*;
use crate::thin::delta::*;
use crate::thin::delta_visitor::Snap;

//------------------------------------------

pub fn run(args: &[std::ffi::OsString]) {
    let parser = App::new("thin_delta")
        .version(crate::version::tools_version())
        .about("Print the differences in the mappings between two thin devices")
        // flags
        .arg(
            Arg::with_name("ASYNC_IO")
                .help("Force use of io_uring for synchronous io")
                .long("async-io")
                .hidden(true),
        )
        .arg(
            Arg::with_name("METADATA_SNAP")
                .help("Use metadata snapshot")
                .short("m")
                .long("metadata-snap"),
        )
        .arg(
            Arg::with_name("VERBOSE")
                .help("Provide extra information on the mappings")
                .long("verbose"),
        )
        // options
        .arg(
            Arg::with_name("ROOT1")
                .help("The root block for the first thin volume to diff")
                .long("root1")
                .value_name("BLOCKNR")
                .group("SNAP1"),
        )
        .arg(
            Arg::with_name("ROOT2")
                .help("The root block for the second thin volume to diff")
                .long("root2")
                .value_name("BLOCKNR")
                .group("SNAP2"),
        )
        .arg(
            Arg::with_name("THIN1")
                .help("The numeric identifier for the first thin volume to diff")
                .long("thin1")
                .value_name("DEV_ID")
                .visible_alias("snap1")
                .group("SNAP1"),
        )
        .arg(
            Arg::with_name("THIN2")
                .help("The numeric identifier for the second thin volume to diff")
                .long("thin2")
                .value_name("DEV_ID")
                .visible_alias("snap2")
                .group("SNAP2"),
        )
        // arguments
        .arg(
            Arg::with_name("INPUT")
                .help("Specify the input device to dump")
                .required(true)
                .index(1),
        )
        // groups
        .group(ArgGroup::with_name("SNAP1").required(true))
        .group(ArgGroup::with_name("SNAP2").required(true));

    let matches = parser.get_matches_from(args);
    let input_file = Path::new(matches.value_of("INPUT").unwrap());

    let report = mk_report(false);
    check_input_file(input_file, &report);
    check_file_not_tiny(input_file, &report);

    let snap1 = if matches.is_present("THIN1") {
        Snap::DeviceId(value_t_or_exit!(matches.value_of("THIN1"), u64))
    } else {
        Snap::RootBlock(value_t_or_exit!(matches.value_of("ROOT1"), u64))
    };

    let snap2 = if matches.is_present("THIN2") {
        Snap::DeviceId(value_t_or_exit!(matches.value_of("THIN2"), u64))
    } else {
        Snap::RootBlock(value_t_or_exit!(matches.value_of("ROOT2"), u64))
    };

    let opts = ThinDeltaOptions {
        input: input_file,
        async_io: matches.is_present("ASYNC_IO"),
        report: report.clone(),
        snap1,
        snap2,
        verbose: matches.is_present("VERBOSE"),
        use_metadata_snap: matches.is_present("METADATA_SNAP"),
    };

    if let Err(reason) = delta(opts) {
        report.fatal(&format!("{}", reason));
        process::exit(1);
    }
}

//------------------------------------------
