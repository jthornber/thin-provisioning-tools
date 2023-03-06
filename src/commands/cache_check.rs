extern crate clap;

use clap::Arg;
use std::path::Path;

use crate::cache::check::{check, CacheCheckOptions};
use crate::commands::engine::*;
use crate::commands::utils::*;
use crate::commands::Command;
use crate::report::*;

//------------------------------------------

pub struct CacheCheckCommand;

impl CacheCheckCommand {
    fn cli<'a>(&self) -> clap::Command<'a> {
        let cmd = clap::Command::new(self.name())
            .color(clap::ColorChoice::Never)
            .version(crate::tools_version!())
            .about("Validates cache metadata on a device or file.")
            // flags
            .arg(
                Arg::new("AUTO_REPAIR")
                    .help("Auto repair trivial issues")
                    .long("auto-repair"),
            )
            .arg(
                Arg::new("CLEAR_NEEDS_CHECK")
                    .help("Clears the 'needs_check' flag in the superblock")
                    .long("clear-needs-check-flag"),
            )
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
                    .help("Only check the superblock")
                    .long("super-block-only"),
            )
            .arg(
                Arg::new("SKIP_HINTS")
                    .help("Don't check the hint array")
                    .long("skip-hints"),
            )
            .arg(
                Arg::new("SKIP_DISCARDS")
                    .help("Don't check the discard bitset")
                    .long("skip-discards"),
            )
            .arg(
                Arg::new("SKIP_MAPPINGS")
                    .help("Don't check the mapping array")
                    .long("skip-mappings"),
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

impl<'a> Command<'a> for CacheCheckCommand {
    fn name(&self) -> &'a str {
        "cache_check"
    }

    fn run(&self, args: &mut dyn Iterator<Item = std::ffi::OsString>) -> exitcode::ExitCode {
        let matches = self.cli().get_matches_from(args);

        let input_file = Path::new(matches.value_of("INPUT").unwrap());

        let report = mk_report(matches.is_present("QUIET"));
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
            sb_only: matches.is_present("SB_ONLY"),
            skip_mappings: matches.is_present("SKIP_MAPPINGS"),
            skip_hints: matches.is_present("SKIP_HINTS"),
            skip_discards: matches.is_present("SKIP_DISCARDS"),
            ignore_non_fatal: matches.is_present("IGNORE_NON_FATAL"),
            auto_repair: matches.is_present("AUTO_REPAIR"),
            clear_needs_check: matches.is_present("CLEAR_NEEDS_CHECK"),
            report: report.clone(),
        };

        to_exit_code(&report, check(opts))
    }
}

//------------------------------------------
