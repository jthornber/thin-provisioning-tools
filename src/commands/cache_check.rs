extern crate clap;

use clap::{Arg, ArgAction};
use std::path::Path;

use crate::cache::check::{check, CacheCheckOptions};
use crate::commands::engine::*;
use crate::commands::utils::*;
use crate::commands::Command;
use crate::report::*;
use crate::version::*;

//------------------------------------------

pub struct CacheCheckCommand;

impl CacheCheckCommand {
    fn cli(&self) -> clap::Command {
        let cmd = clap::Command::new(self.name())
            .next_display_order(None)
            .version(crate::tools_version!())
            .disable_version_flag(true)
            .about("Validates cache metadata on a device or file.")
            // flags
            .arg(
                Arg::new("AUTO_REPAIR")
                    .help("Auto repair trivial issues")
                    .long("auto-repair")
                    .action(ArgAction::SetTrue),
            )
            .arg(
                Arg::new("CLEAR_NEEDS_CHECK")
                    .help("Clears the 'needs_check' flag in the superblock")
                    .long("clear-needs-check-flag")
                    .action(ArgAction::SetTrue),
            )
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
                    .help("Only check the superblock")
                    .long("super-block-only")
                    .action(ArgAction::SetTrue),
            )
            .arg(
                Arg::new("SKIP_HINTS")
                    .help("Don't check the hint array")
                    .long("skip-hints")
                    .action(ArgAction::SetTrue),
            )
            .arg(
                Arg::new("SKIP_DISCARDS")
                    .help("Don't check the discard bitset")
                    .long("skip-discards")
                    .action(ArgAction::SetTrue),
            )
            .arg(
                Arg::new("SKIP_MAPPINGS")
                    .help("Don't check the mapping array")
                    .long("skip-mappings")
                    .action(ArgAction::SetTrue),
            )
            // arguments
            .arg(
                Arg::new("INPUT")
                    .help("Specify the input device to check")
                    .required(true)
                    .index(1),
            );
        verbose_args(engine_args(version_args(cmd)))
    }
}

impl<'a> Command<'a> for CacheCheckCommand {
    fn name(&self) -> &'a str {
        "cache_check"
    }

    fn run(&self, args: &mut dyn Iterator<Item = std::ffi::OsString>) -> exitcode::ExitCode {
        let matches = self.cli().get_matches_from(args);
        display_version(&matches);

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

        let engine_opts = parse_engine_opts(ToolType::Cache, &matches);
        if engine_opts.is_err() {
            return to_exit_code(&report, engine_opts);
        }
        let engine_opts = engine_opts.unwrap();

        let opts = CacheCheckOptions {
            dev: input_file,
            engine_opts,
            sb_only: matches.get_flag("SB_ONLY"),
            skip_mappings: matches.get_flag("SKIP_MAPPINGS"),
            skip_hints: matches.get_flag("SKIP_HINTS"),
            skip_discards: matches.get_flag("SKIP_DISCARDS"),
            ignore_non_fatal: matches.get_flag("IGNORE_NON_FATAL"),
            auto_repair: matches.get_flag("AUTO_REPAIR"),
            clear_needs_check: matches.get_flag("CLEAR_NEEDS_CHECK"),
            report: report.clone(),
        };

        to_exit_code(&report, check(opts))
    }
}

//------------------------------------------
