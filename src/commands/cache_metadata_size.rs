extern crate clap;

use clap::{Arg, ArgGroup};
use std::ffi::OsString;

use crate::cache::metadata_size::{metadata_size, CacheMetadataSizeOptions};
use crate::commands::Command;
use crate::math::div_up;
use crate::units::*;

//------------------------------------------

pub struct CacheMetadataSizeCommand;

impl CacheMetadataSizeCommand {
    fn cli<'a>(&self) -> clap::Command<'a> {
        clap::Command::new(self.name())
            .color(clap::ColorChoice::Never)
            .version(crate::version::tools_version())
            .about("Estimate the size of the metadata device needed for a given configuration.")
            .override_usage("cache_metadata_size [OPTIONS] <--device-size <SIZE> --block-size <SIZE> | --nr-blocks <NUM>>")
            // flags
            .arg(
                Arg::new("NUMERIC_ONLY")
                    .help("Output numeric value only")
                    .short('n')
                    .long("numeric-only"),
            )
            // options
            .arg(
                Arg::new("BLOCK_SIZE")
                    .help("Specify the size of each cache block")
                    .long("block-size")
                    .requires("DEVICE_SIZE")
                    .value_name("SIZE[bskmg]"),
            )
            .arg(
                Arg::new("DEVICE_SIZE")
                    .help("Specify total size of the fast device used in the cache")
                    .long("device-size")
                    .requires("BLOCK_SIZE")
                    .value_name("SIZE[bskmgtp]"),
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
            .arg(
                Arg::new("UNIT")
                    .help("Specify the output unit in {bskKmMgG}")
                    .short('u')
                    .long("unit")
                    .value_name("UNIT")
                    .default_value("sector"),
            )
            .group(
                ArgGroup::new("selection")
                .args(&["DEVICE_SIZE", "NR_BLOCKS"])
                .required(true)
            )
    }

    fn parse_args<I, T>(&self, args: I) -> (CacheMetadataSizeOptions, Units, bool)
    where
        I: IntoIterator<Item = T>,
        T: Into<OsString> + Clone,
    {
        let matches = self.cli().get_matches_from(args);

        let nr_blocks = matches.value_of("NR_BLOCKS").map_or_else(
            || {
                let device_size = matches
                    .value_of_t_or_exit::<StorageSize>("DEVICE_SIZE")
                    .size_bytes();
                let block_size = matches
                    .value_of_t_or_exit::<StorageSize>("BLOCK_SIZE")
                    .size_bytes();
                div_up(device_size, block_size as u64)
            },
            |_| matches.value_of_t_or_exit::<u64>("NR_BLOCKS"),
        );

        let max_hint_width = matches.value_of_t_or_exit::<u32>("MAX_HINT_WIDTH");
        let unit = matches.value_of_t_or_exit::<Units>("UNIT");
        let numeric_only = matches.is_present("NUMERIC_ONLY");

        (
            CacheMetadataSizeOptions {
                nr_blocks,
                max_hint_width,
            },
            unit,
            numeric_only,
        )
    }
}

impl<'a> Command<'a> for CacheMetadataSizeCommand {
    fn name(&self) -> &'a str {
        "cache_metadata_size"
    }

    fn run(&self, args: &mut dyn Iterator<Item = std::ffi::OsString>) -> std::io::Result<()> {
        let (opts, unit, numeric_only) = self.parse_args(args);

        match metadata_size(&opts) {
            Ok(size) => {
                let size = to_units(size, unit);
                if numeric_only {
                    println!("{}", size);
                } else {
                    let mut name = unit.to_string();
                    name.push('s'); // plural form
                    println!("{} {}", size, name);
                }
                Ok(())
            }
            Err(reason) => {
                eprintln!("{}", reason);
                Err(std::io::Error::from_raw_os_error(libc::EPERM))
            }
        }
    }
}

//------------------------------------------
