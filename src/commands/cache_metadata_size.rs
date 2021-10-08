extern crate clap;

use clap::{value_t_or_exit, App, Arg, ArgGroup};
use std::ffi::OsString;
use std::process;

use crate::cache::metadata_size::{metadata_size, CacheMetadataSizeOptions};
use crate::math::div_up;

//------------------------------------------

fn parse_args<I, T>(args: I) -> CacheMetadataSizeOptions
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let parser = App::new("cache_metadata_size")
        .version(crate::version::tools_version())
        .about("Estimate the size of the metadata device needed for a given configuration.")
        .usage("cache_metadata_size [OPTIONS] <--device-size <SECTORS> --block-size <SECTORS> | --nr-blocks <NUM>>")
        // options
        .arg(
            Arg::with_name("BLOCK_SIZE")
                .help("Specify the size of each cache block")
                .long("block-size")
                .requires("DEVICE_SIZE")
                .value_name("SECTORS"),
        )
        .arg(
            Arg::with_name("DEVICE_SIZE")
                .help("Specify total size of the fast device used in the cache")
                .long("device-size")
                .requires("BLOCK_SIZE")
                .value_name("SECTORS"),
        )
        .arg(
            Arg::with_name("NR_BLOCKS")
                .help("Specify the number of cache blocks")
                .long("nr-blocks")
                .value_name("NUM"),
        )
        .arg(
            Arg::with_name("MAX_HINT_WIDTH")
                .help("Specity the per-block hint width")
                .long("max-hint-width")
                .value_name("BYTES")
                .default_value("4"),
        )
        .group(
            ArgGroup::with_name("selection")
            .args(&["DEVICE_SIZE", "NR_BLOCKS"])
            .required(true)
        );

    let matches = parser.get_matches_from(args);

    let nr_blocks = matches.value_of("NR_BLOCKS").map_or_else(
        || {
            let device_size = value_t_or_exit!(matches.value_of("DEVICE_SIZE"), u64);
            let block_size = value_t_or_exit!(matches.value_of("BLOCK_SIZE"), u32);
            div_up(device_size, block_size as u64)
        },
        |_| value_t_or_exit!(matches.value_of("NR_BLOCKS"), u64),
    );

    let max_hint_width = value_t_or_exit!(matches.value_of("MAX_HINT_WIDTH"), u32);

    CacheMetadataSizeOptions {
        nr_blocks,
        max_hint_width,
    }
}

pub fn run(args: &[std::ffi::OsString]) {
    let opts = parse_args(args);

    match metadata_size(&opts) {
        Ok(size) => {
            println!("{} sectors", size);
        }
        Err(reason) => {
            eprintln!("{}", reason);
            process::exit(1);
        }
    }
}

//------------------------------------------
