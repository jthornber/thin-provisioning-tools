extern crate clap;

use clap::{value_t_or_exit, App, Arg};
use std::ffi::OsString;
use std::process;

use crate::thin::metadata_size::{metadata_size, ThinMetadataSizeOptions};
use crate::units::*;

//------------------------------------------

fn parse_args<I, T>(args: I) -> (ThinMetadataSizeOptions, Units, bool)
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let parser = App::new("thin_metadata_size")
        .version(crate::version::tools_version())
        .about("Estimate the size of the metadata device needed for a given configuration.")
        // options
        .arg(
            Arg::with_name("BLOCK_SIZE")
                .help("Specify the data block size")
                .short("b")
                .long("block-size")
                .required(true)
                .value_name("SECTORS"),
        )
        .arg(
            Arg::with_name("POOL_SIZE")
                .help("Specify the size of pool device")
                .short("s")
                .long("pool-size")
                .required(true)
                .value_name("SECTORS"),
        )
        .arg(
            Arg::with_name("MAX_THINS")
                .help("Maximum number of thin devices and snapshots")
                .short("m")
                .long("max-thins")
                .required(true)
                .value_name("NUM"),
        )
        .arg(
            Arg::with_name("UNIT")
                .help("Specify the output unit")
                .short("u")
                .long("unit")
                .value_name("UNIT")
                .default_value("sector"),
        )
        .arg(
            Arg::with_name("NUMERIC_ONLY")
                .help("Output numeric value only")
                .short("n")
                .long("numeric-only"),
        );

    let matches = parser.get_matches_from(args);

    // TODO: handle unit suffix
    let pool_size = value_t_or_exit!(matches.value_of("POOL_SIZE"), u64);
    let block_size = value_t_or_exit!(matches.value_of("BLOCK_SIZE"), u32);
    let max_thins = value_t_or_exit!(matches.value_of("MAX_THINS"), u64);
    let unit = value_t_or_exit!(matches.value_of("UNIT"), Units);
    let numeric_only = matches.is_present("NUMERIC_ONLY");

    (
        ThinMetadataSizeOptions {
            nr_blocks: pool_size / block_size as u64,
            max_thins,
        },
        unit,
        numeric_only,
    )
}

pub fn run(args: &[std::ffi::OsString]) {
    let (opts, unit, numeric_only) = parse_args(args);

    match metadata_size(&opts) {
        Ok(size) => {
            let size = to_units(size * 512, unit.clone());
            if numeric_only {
                println!("{}", size);
            } else {
                let mut name = unit.to_string();
                name.push('s');
                println!("{} {}", size, name);
            }
        }
        Err(reason) => {
            eprintln!("{}", reason);
            process::exit(1);
        }
    }
}

//------------------------------------------
