extern crate clap;

use atty::Stream;
use clap::Arg;
use std::path::Path;

use std::sync::Arc;

use crate::commands::engine::*;
use crate::commands::utils::*;
use crate::commands::Command;
use crate::era::check::{check, EraCheckOptions};
use crate::report::*;

//------------------------------------------

pub struct EraCheckCommand;

impl EraCheckCommand {
    fn cli<'a>(&self) -> clap::Command<'a> {
        let cmd = clap::Command::new(self.name())
            .color(clap::ColorChoice::Never)
            .version(crate::version::tools_version())
            .about("Validate era metadata on device or file.")
            // flags
            .arg(
                Arg::new("IGNORE_NON_FATAL")
                    .help("Only return a non-zero exit code if a fatal error is found.")
                    .long("ignore-non-fatal-errors"),
            )
            .arg(
                Arg::new("QUIET")
                    .help("Suppress output messages, return only exit code.")
                    .short('q')
                    .long("quiet"),
            )
            .arg(
                Arg::new("SB_ONLY")
                    .help("Only check the superblock.")
                    .long("super-block-only"),
            )
            // arguments
            .arg(
                Arg::new("INPUT")
                    .help("Specify the input device to check")
                    .required(true)
                    .index(1),
            );
            engine_args(cmd)
    }
}

impl<'a> Command<'a> for EraCheckCommand {
    fn name(&self) -> &'a str {
        "era_check"
    }

    fn run(&self, args: &mut dyn Iterator<Item = std::ffi::OsString>) -> exitcode::ExitCode {
        let matches = self.cli().get_matches_from(args);

        let input_file = Path::new(matches.value_of("INPUT").unwrap());

        let report = if matches.is_present("QUIET") {
            std::sync::Arc::new(mk_quiet_report())
        } else if atty::is(Stream::Stdout) {
            std::sync::Arc::new(mk_progress_bar_report())
        } else {
            Arc::new(mk_simple_report())
        };

        check_input_file(input_file, &report);
        check_file_not_tiny(input_file, &report);
        check_not_xml(input_file, &report);

        let engine_opts = parse_engine_opts(ToolType::Era, false, &matches);
        if engine_opts.is_err() {
            return to_exit_code(&report, engine_opts);
        }

        let opts = EraCheckOptions {
            dev: input_file,
            engine_opts: engine_opts.unwrap(),
            sb_only: matches.is_present("SB_ONLY"),
            ignore_non_fatal: matches.is_present("IGNORE_NON_FATAL"),
            report: report.clone(),
        };

        to_exit_code(&report, check(&opts))
    }
}

//------------------------------------------
