use anyhow::{anyhow, Result};
use clap::builder::{PossibleValuesParser, TypedValueParser};
use clap::{value_parser, Arg};
use std::ffi::OsString;
use std::io;
use std::str::FromStr;

use crate::cache::metadata_size::*;
use crate::commands::utils::*;
use crate::commands::Command;
use crate::math::div_up;
use crate::report::mk_simple_report;
use crate::units::*;
use crate::version::*;

//------------------------------------------

#[derive(Clone, Debug)]
enum OutputFormat {
    Full,
    ShortUnits,
    LongUnits,
    NumericOnly,
}

impl FromStr for OutputFormat {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "short" => Ok(OutputFormat::ShortUnits),
            "long" => Ok(OutputFormat::LongUnits),
            _ => Err(anyhow!("invalid option")),
        }
    }
}

//------------------------------------------

pub struct CacheMetadataSizeCommand;

impl CacheMetadataSizeCommand {
    fn cli(&self) -> clap::Command {
        let cmd = clap::Command::new(self.name())
            .next_display_order(None)
            .version(crate::tools_version!())
            .disable_version_flag(true)
            .about("Estimate the size of the metadata device needed for a given configuration.")
            .override_usage("cache_metadata_size [OPTIONS] <--device-size <SIZE> --block-size <SIZE> | --nr-blocks <NUM>>")
            // flags
            .arg(
                Arg::new("NUMERIC_ONLY")
                    .help("Output numeric value only")
                    .short('n')
                    .long("numeric-only")
                    .value_name("OPT")
                    .value_parser(PossibleValuesParser::new(["short", "long"]).map(|s| s.parse::<OutputFormat>().unwrap()))
                    .num_args(0..=1)
                    .require_equals(true)
                    .hide_possible_values(true),
            )
            // options
            .arg(
                Arg::new("BLOCK_SIZE")
                    .help("Specify the size of each cache block")
                    .long("block-size")
                    .requires("DEVICE_SIZE")
                    .value_name("SIZE[bskmg]")
                    .value_parser(value_parser!(StorageSize)),
            )
            .arg(
                Arg::new("DEVICE_SIZE")
                    .help("Specify total size of the fast device used in the cache")
                    .long("device-size")
                    .requires("BLOCK_SIZE")
                    .value_name("SIZE[bskmgtp]")
                    .value_parser(value_parser!(StorageSize)),
            )
            .arg(
                Arg::new("NR_BLOCKS")
                    .help("Specify the number of cache blocks")
                    .long("nr-blocks")
                    .conflicts_with_all(["BLOCK_SIZE", "DEVICE_SIZE"])
                    .value_name("NUM")
                    .value_parser(value_parser!(u64)),
            )
            .arg(
                Arg::new("MAX_HINT_WIDTH")
                    .help("Specity the per-block hint width")
                    .long("max-hint-width")
                    .value_name("BYTES")
                    .value_parser(value_parser!(u32))
                    .default_value("4"),
            )
            .arg(
                Arg::new("UNIT")
                    .help("Specify the output unit in {bskKmMgG}")
                    .short('u')
                    .long("unit")
                    .value_name("UNIT")
                    .value_parser(value_parser!(Units))
                    .default_value("sector"),
            );

        version_args(cmd)
    }

    fn parse_args<I, T>(&self, args: I) -> Result<(CacheMetadataSizeOptions, Units, OutputFormat)>
    where
        I: IntoIterator<Item = T>,
        T: Into<OsString> + Clone,
    {
        let matches = self.cli().get_matches_from(args);
        display_version(&matches);

        let nr_blocks = matches.get_one::<u64>("NR_BLOCKS");
        let device_size = matches.get_one::<StorageSize>("DEVICE_SIZE");
        let block_size = matches.get_one::<StorageSize>("BLOCK_SIZE");

        let nr_blocks = if let Some(n) = nr_blocks {
            *n
        } else if let (Some(ds), Some(bs)) = (device_size, block_size) {
            let device_size = ds.size_bytes();
            let block_size = bs.size_bytes();

            check_cache_block_size(block_size)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;

            if device_size < block_size {
                return Err(anyhow!("pool size must be larger than block size"));
            }

            div_up(device_size, block_size)
        } else {
            return Err(anyhow!(
                "Please specify either --device-size and --block-size, or --nr-blocks."
            ));
        };

        let max_hint_width = *matches.get_one::<u32>("MAX_HINT_WIDTH").unwrap();
        let unit = *matches.get_one::<Units>("UNIT").unwrap();

        let format = if let Some(fmt) = matches.get_one::<OutputFormat>("NUMERIC_ONLY") {
            fmt.clone()
        } else if matches!(
            matches.value_source("NUMERIC_ONLY"),
            Some(clap::parser::ValueSource::CommandLine)
        ) {
            OutputFormat::NumericOnly
        } else {
            OutputFormat::Full
        };

        Ok((
            CacheMetadataSizeOptions {
                nr_blocks,
                max_hint_width,
            },
            unit,
            format,
        ))
    }
}

impl<'a> Command<'a> for CacheMetadataSizeCommand {
    fn name(&self) -> &'a str {
        "cache_metadata_size"
    }

    fn run(&self, args: &mut dyn Iterator<Item = std::ffi::OsString>) -> exitcode::ExitCode {
        let report = mk_simple_report();

        let opts = self.parse_args(args);
        if opts.is_err() {
            return to_exit_code(&report, opts);
        }

        let (opts, unit, format) = opts.unwrap();

        match metadata_size(&opts) {
            Ok(size) => {
                let size = to_units(size, unit);

                let mut output = if size < 1.0 || size.trunc() == size {
                    format!("{}", size)
                } else {
                    format!("{:.2}", size)
                };

                match format {
                    OutputFormat::Full => {
                        output.push(' ');
                        output.push_str(&unit.to_string());
                        output.push('s'); // plural form
                    }
                    OutputFormat::ShortUnits => output.push_str(&unit.to_letter()),
                    OutputFormat::LongUnits => {
                        // be backward compatible: no space between the numeric value and the unit
                        output.push_str(&unit.to_string());
                        output.push('s'); // plural form
                    }
                    _ => {} // do nothing
                }

                report.to_stdout(&output);

                exitcode::OK
            }
            Err(reason) => {
                report.fatal(&reason.to_string());

                // FIXME: use a report and call to_exit_code
                exitcode::USAGE
            }
        }
    }
}

//------------------------------------------
