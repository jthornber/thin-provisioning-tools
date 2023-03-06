use anyhow::{anyhow, Result};
use clap::{value_parser, Arg};
use std::ffi::OsString;
use std::io;
use std::str::FromStr;

use crate::commands::utils::*;
use crate::commands::Command;
use crate::report::mk_simple_report;
use crate::thin::metadata_size::*;
use crate::units::*;

//------------------------------------------

#[derive(Clone)]
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

pub struct ThinMetadataSizeCommand;

impl ThinMetadataSizeCommand {
    fn cli<'a>(&self) -> clap::Command<'a> {
        clap::Command::new(self.name())
            .color(clap::ColorChoice::Never)
            .version(crate::tools_version!())
            .about("Estimate the size of the metadata device needed for a given configuration.")
            // flags
            .arg(
                Arg::new("NUMERIC_ONLY")
                    .help("Output numeric value only")
                    .short('n')
                    .long("numeric-only")
                    .value_name("OPT")
                    .value_parser(value_parser!(OutputFormat))
                    .min_values(0)
                    .max_values(1)
                    .require_equals(true)
                    .possible_values(["short", "long"])
                    .hide_possible_values(true),
            )
            // options
            .arg(
                Arg::new("BLOCK_SIZE")
                    .help("Specify the data block size")
                    .short('b')
                    .long("block-size")
                    .required(true)
                    .value_name("SIZE[bskmg]"),
            )
            .arg(
                Arg::new("POOL_SIZE")
                    .help("Specify the size of pool device")
                    .short('s')
                    .long("pool-size")
                    .required(true)
                    .value_name("SIZE[bskmgtp]"),
            )
            .arg(
                Arg::new("MAX_THINS")
                    .help("Maximum number of thin devices and snapshots")
                    .short('m')
                    .long("max-thins")
                    .required(true)
                    .value_name("NUM"),
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

    fn parse_args<I, T>(&self, args: I) -> Result<(ThinMetadataSizeOptions, Units, OutputFormat)>
    where
        I: IntoIterator<Item = T>,
        T: Into<OsString> + Clone,
    {
        let matches = self.cli().get_matches_from(args);

        let pool_size = matches
            .value_of_t_or_exit::<StorageSize>("POOL_SIZE")
            .size_bytes();
        let block_size = matches
            .value_of_t_or_exit::<StorageSize>("BLOCK_SIZE")
            .size_bytes();
        let max_thins = matches.value_of_t_or_exit::<u64>("MAX_THINS");
        let unit = matches.value_of_t_or_exit::<Units>("UNIT");

        let format = if matches.is_present("NUMERIC_ONLY") {
            matches
                .get_one::<OutputFormat>("NUMERIC_ONLY")
                .cloned()
                .unwrap_or(OutputFormat::NumericOnly)
        } else {
            OutputFormat::Full
        };

        check_data_block_size(block_size)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;

        if pool_size < block_size {
            return Err(anyhow!("pool size must be larger than block size"));
        }

        Ok((
            ThinMetadataSizeOptions {
                nr_blocks: pool_size / block_size,
                max_thins,
            },
            unit,
            format,
        ))
    }
}

impl<'a> Command<'a> for ThinMetadataSizeCommand {
    fn name(&self) -> &'a str {
        "thin_metadata_size"
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
                        // no space between the numeric value and the unit
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

                // FIXME: use to_exit_code
                exitcode::USAGE
            }
        }
    }
}

//------------------------------------------
