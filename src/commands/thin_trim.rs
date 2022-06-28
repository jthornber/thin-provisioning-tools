extern crate clap;

use clap::Arg;
use std::path::Path;

use crate::commands::utils::*;
use crate::thin::trim::{trim, ThinTrimOptions};

//------------------------------------------
use crate::commands::Command;

pub struct ThinTrimCommand;

impl ThinTrimCommand {
    fn cli<'a>(&self) -> clap::Command<'a> {
        clap::Command::new(self.name())
            .color(clap::ColorChoice::Never)
            .version(crate::version::tools_version())
            .about("Issue discard requests for free pool space (offline tool).")
            // flags
            .arg(
                Arg::new("ASYNC_IO")
                    .help("Force use of io_uring for synchronous io")
                    .long("async-io")
                    .hide(true),
            )
            .arg(
                Arg::new("QUIET")
                    .help("Suppress output messages, return only exit code.")
                    .short('q')
                    .long("quiet"),
            )
            // options
            .arg(
                Arg::new("METADATA_DEV")
                    .help("Specify the pool metadata device")
                    .long("metadata-dev")
                    .value_name("FILE")
                    .required(true),
            )
            .arg(
                Arg::new("DATA_DEV")
                    .help("Specify the pool data device")
                    .long("data-dev")
                    .value_name("FILE")
                    .required(true),
            )
    }
}

impl<'a> Command<'a> for ThinTrimCommand {
    fn name(&self) -> &'a str {
        "thin_trim"
    }

    fn run(&self, args: &mut dyn Iterator<Item = std::ffi::OsString>) -> exitcode::ExitCode {
        let matches = self.cli().get_matches_from(args);

        let metadata_dev = Path::new(matches.value_of("METADATA_DEV").unwrap());
        let data_dev = Path::new(matches.value_of("DATA_DEV").unwrap());

        let report = mk_report(matches.is_present("QUIET"));
        check_input_file(metadata_dev, &report);
        check_input_file(data_dev, &report);

        let opts = ThinTrimOptions {
            metadata_dev,
            data_dev,
            async_io: matches.is_present("ASYNC_IO"),
            report: report.clone(),
        };

        to_exit_code(&report, trim(opts))
    }
}
