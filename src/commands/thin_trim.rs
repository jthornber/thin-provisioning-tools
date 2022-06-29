extern crate clap;

use clap::Arg;
use std::path::Path;

use crate::commands::engine::*;
use crate::commands::utils::*;
use crate::thin::trim::{trim, ThinTrimOptions};

//------------------------------------------
use crate::commands::Command;

pub struct ThinTrimCommand;

impl ThinTrimCommand {
    fn cli<'a>(&self) -> clap::Command<'a> {
        let cmd = clap::Command::new(self.name())
            .color(clap::ColorChoice::Never)
            .version(crate::version::tools_version())
            .about("Issue discard requests for free pool space (offline tool).")
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
            );
            engine_args(cmd)
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

        let engine_opts = parse_engine_opts(ToolType::Era, true, &matches);
        if engine_opts.is_err() {
            return to_exit_code(&report, engine_opts);
        }

        let opts = ThinTrimOptions {
            metadata_dev,
            data_dev,
            engine_opts: engine_opts.unwrap(),
            report: report.clone(),
        };

        to_exit_code(&report, trim(opts))
    }
}
