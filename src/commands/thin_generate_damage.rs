use clap::{Arg, ArgGroup, Command};
use std::path::Path;
use std::process;

use crate::thin::damage_generator::*;

//------------------------------------------

pub fn run(args: &[std::ffi::OsString]) {
    let parser = Command::new("thin_generate_damage")
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
            Arg::new("CREATE_METADATA_LEAKS")
                .help("Create leaked metadata blocks")
                .long("create-metadata-leaks")
                .requires_all(&["EXPECTED", "ACTUAL", "NR_BLOCKS"])
                .group("commands"),
        )
        // options
        .arg(
            Arg::new("EXPECTED")
                .help("The expected reference count of damaged blocks")
                .long("expected")
                .value_name("REFCONT"),
        )
        .arg(
            Arg::new("ACTUAL")
                .help("The actual reference count of damaged blocks")
                .long("actual")
                .value_name("REFCOUNT"),
        )
        .arg(
            Arg::new("NR_BLOCKS")
                .help("Specify the number of metadata blocks")
                .long("nr-blocks")
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
        .group(ArgGroup::new("commands").required(true));

    let matches = parser.get_matches_from(args);

    let opts = ThinDamageOpts {
        async_io: matches.is_present("ASYNC_IO"),
        op: if matches.is_present("CREATE_METADATA_LEAKS") {
            DamageOp::CreateMetadataLeaks {
                nr_blocks: matches.value_of_t_or_exit::<usize>("NR_BLOCKS"),
                expected_rc: matches.value_of_t_or_exit::<u32>("EXPECTED"),
                actual_rc: matches.value_of_t_or_exit::<u32>("ACTUAL"),
            }
        } else {
            eprintln!("unknown option");
            process::exit(1);
        },
        output: Path::new(matches.value_of("OUTPUT").unwrap()),
    };

    if let Err(reason) = damage_metadata(opts) {
        eprintln!("{}", reason);
        process::exit(1)
    }
}

//------------------------------------------
