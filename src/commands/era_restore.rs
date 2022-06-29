extern crate clap;

use clap::Arg;
use std::path::Path;

use crate::commands::engine::*;
use crate::commands::utils::*;
use crate::commands::Command;
use crate::era::restore::{restore, EraRestoreOptions};

pub struct EraRestoreCommand;

impl EraRestoreCommand {
    fn cli<'a>(&self) -> clap::Command<'a> {
        let cmd = clap::Command::new(self.name())
            .color(clap::ColorChoice::Never)
            .version(crate::version::tools_version())
            .about("Convert XML format metadata to binary.")
            // flags
            .arg(
                Arg::new("QUIET")
                    .help("Suppress output messages, return only exit code.")
                    .short('q')
                    .long("quiet"),
            )
            // options
            .arg(
                Arg::new("INPUT")
                    .help("Specify the input xml")
                    .short('i')
                    .long("input")
                    .value_name("FILE")
                    .required(true),
            )
            .arg(
                Arg::new("OUTPUT")
                    .help("Specify the output device to check")
                    .short('o')
                    .long("output")
                    .value_name("FILE")
                    .required(true),
            );
            engine_args(cmd)
    }
}

impl<'a> Command<'a> for EraRestoreCommand {
    fn name(&self) -> &'a str {
        "era_restore"
    }

    fn run(&self, args: &mut dyn Iterator<Item = std::ffi::OsString>) -> exitcode::ExitCode {
        let matches = self.cli().get_matches_from(args);

        let input_file = Path::new(matches.value_of("INPUT").unwrap());
        let output_file = Path::new(matches.value_of("OUTPUT").unwrap());

        let report = mk_report(matches.is_present("QUIET"));
        check_input_file(input_file, &report);
        check_output_file(output_file, &report);

        let engine_opts = parse_engine_opts(ToolType::Era, &matches);
        if engine_opts.is_err() {
            return to_exit_code(&report, engine_opts);
        }

        let opts = EraRestoreOptions {
            input: input_file,
            output: output_file,
            engine_opts: engine_opts.unwrap(),
            report: report.clone(),
        };

        to_exit_code(&report, restore(opts))
    }
}
