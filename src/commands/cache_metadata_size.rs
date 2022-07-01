use anyhow::{anyhow, Result};
use clap::Arg;
use std::ffi::OsString;
use std::io;

use crate::cache::metadata_size::*;
use crate::commands::utils::*;
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
                    .conflicts_with_all(&["BLOCK_SIZE", "DEVICE_SIZE"])
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
    }

    fn parse_args<I, T>(&self, args: I) -> Result<(CacheMetadataSizeOptions, Units, bool)>
    where
        I: IntoIterator<Item = T>,
        T: Into<OsString> + Clone,
    {
        let matches = self.cli().get_matches_from(args);

        let nr_blocks = if matches.is_present("NR_BLOCKS") {
            matches.value_of_t_or_exit::<u64>("NR_BLOCKS")
        } else if matches.is_present("BLOCK_SIZE") && matches.is_present("DEVICE_SIZE") {
            let device_size = matches
                .value_of_t_or_exit::<StorageSize>("DEVICE_SIZE")
                .size_bytes();
            let block_size = matches
                .value_of_t_or_exit::<StorageSize>("BLOCK_SIZE")
                .size_bytes();

            check_cache_block_size(block_size)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;

            if device_size < block_size {
                return Err(anyhow!("pool size must be larger than block size"));
            }

            div_up(device_size, block_size as u64)
        } else {
            return Err(anyhow!(
                "Please specify either --device-size and --block-size, or --nr-blocks."
            ));
        };

        let max_hint_width = matches.value_of_t_or_exit::<u32>("MAX_HINT_WIDTH");
        let unit = matches.value_of_t_or_exit::<Units>("UNIT");
        let numeric_only = matches.is_present("NUMERIC_ONLY");

        Ok((
            CacheMetadataSizeOptions {
                nr_blocks,
                max_hint_width,
            },
            unit,
            numeric_only,
        ))
    }
}

impl<'a> Command<'a> for CacheMetadataSizeCommand {
    fn name(&self) -> &'a str {
        "cache_metadata_size"
    }

    fn run(&self, args: &mut dyn Iterator<Item = std::ffi::OsString>) -> exitcode::ExitCode {
        let opts = self.parse_args(args);
        if opts.is_err() {
            let report = mk_report(false);
            return to_exit_code(&report, opts);
        }

        let (opts, unit, numeric_only) = opts.unwrap();

        match metadata_size(&opts) {
            Ok(size) => {
                let size = to_units(size, unit);

                let mut output = if size < 1.0 || size.trunc() == size {
                    format!("{}", size)
                } else {
                    format!("{:.2}", size)
                };

                if !numeric_only {
                    output.push(' ');
                    output.push_str(&unit.to_string());
                    output.push('s'); // plural form
                }

                println!("{}", output);

                exitcode::OK
            }
            Err(reason) => {
                eprintln!("{}", reason);
                // FIXME: use a report and call to_exit_code
                exitcode::USAGE
            }
        }
    }
}

//------------------------------------------
