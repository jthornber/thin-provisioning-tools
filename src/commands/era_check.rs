extern crate clap;

use clap::{Arg, ArgAction};
use std::path::Path;

use crate::commands::engine::*;
use crate::commands::utils::*;
use crate::commands::Command;
use crate::era::check::{check, EraCheckOptions};
use crate::report::*;

//------------------------------------------

pub struct EraCheckCommand;

impl EraCheckCommand {
    fn cli(&self) -> clap::Command {
        let cmd = clap::Command::new(self.name())
            .next_display_order(None)
            .version(crate::tools_version!())
            .about("Validate era metadata on device or file.")
            // flags
            .arg(
                Arg::new("IGNORE_NON_FATAL")
                    .help("Only return a non-zero exit code if a fatal error is found.")
                    .long("ignore-non-fatal-errors")
                    .action(ArgAction::SetTrue),
            )
            .arg(
                Arg::new("QUIET")
                    .help("Suppress output messages, return only exit code.")
                    .short('q')
                    .long("quiet")
                    .action(ArgAction::SetTrue),
            )
            .arg(
                Arg::new("SB_ONLY")
                    .help("Only check the superblock.")
                    .long("super-block-only")
                    .action(ArgAction::SetTrue),
            )
            // arguments
            .arg(
                Arg::new("INPUT")
                    .help("Specify the input device to check")
                    .required(true)
                    .index(1),
            );
        verbose_args(engine_args(cmd))
    }
}

impl<'a> Command<'a> for EraCheckCommand {
    fn name(&self) -> &'a str {
        "era_check"
    }

    fn run(&self, args: &mut dyn Iterator<Item = std::ffi::OsString>) -> exitcode::ExitCode {
        let matches = self.cli().get_matches_from(args);

        let input_file = Path::new(matches.get_one::<String>("INPUT").unwrap());

        let report = mk_report(matches.get_flag("QUIET"));
        let log_level = match parse_log_level(&matches) {
            Ok(level) => level,
            Err(e) => return to_exit_code::<()>(&report, Err(anyhow::Error::msg(e))),
        };
        report.set_level(log_level);

        if let Err(e) = check_input_file(input_file)
            .and_then(check_file_not_tiny)
            .and_then(check_not_xml)
        {
            return to_exit_code::<()>(&report, Err(e));
        }

        let engine_opts = parse_engine_opts(ToolType::Era, &matches);
        if engine_opts.is_err() {
            return to_exit_code(&report, engine_opts);
        }

        let opts = EraCheckOptions {
            dev: input_file,
            engine_opts: engine_opts.unwrap(),
            sb_only: matches.get_flag("SB_ONLY"),
            ignore_non_fatal: matches.get_flag("IGNORE_NON_FATAL"),
            report: report.clone(),
        };

        to_exit_code(&report, check(&opts))
    }
}

//------------------------------------------
