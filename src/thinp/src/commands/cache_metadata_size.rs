extern crate clap;

use clap::{App, Arg, ArgGroup};
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
        .color(clap::ColorChoice::Never)
        .version(crate::version::tools_version())
        .about("Estimate the size of the metadata device needed for a given configuration.")
        .override_usage("cache_metadata_size [OPTIONS] <--device-size <SECTORS> --block-size <SECTORS> | --nr-blocks <NUM>>")
        // options
        .arg(
            Arg::new("BLOCK_SIZE")
                .help("Specify the size of each cache block")
                .long("block-size")
                .requires("DEVICE_SIZE")
                .value_name("SECTORS"),
        )
        .arg(
            Arg::new("DEVICE_SIZE")
                .help("Specify total size of the fast device used in the cache")
                .long("device-size")
                .requires("BLOCK_SIZE")
                .value_name("SECTORS"),
        )
        .arg(
            Arg::new("NR_BLOCKS")
                .help("Specify the number of cache blocks")
                .long("nr-blocks")
                .value_name("NUM"),
        )
        .arg(
            Arg::new("MAX_HINT_WIDTH")
                .help("Specity the per-block hint width")
                .long("max-hint-width")
                .value_name("BYTES")
                .default_value("4"),
        )
        .group(
            ArgGroup::new("selection")
            .args(&["DEVICE_SIZE", "NR_BLOCKS"])
            .required(true)
        );

    let matches = parser.get_matches_from(args);

    let nr_blocks = matches.value_of("NR_BLOCKS").map_or_else(
        || {
            let device_size = matches.value_of_t_or_exit::<u64>("DEVICE_SIZE");
            let block_size = matches.value_of_t_or_exit::<u32>("BLOCK_SIZE");
            div_up(device_size, block_size as u64)
        },
        |_| matches.value_of_t_or_exit::<u64>("NR_BLOCKS"),
    );

    let max_hint_width = matches.value_of_t_or_exit::<u32>("MAX_HINT_WIDTH");

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
