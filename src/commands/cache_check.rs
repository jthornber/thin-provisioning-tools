extern crate clap;

use atty::Stream;
use clap::Arg;
use std::path::Path;

use std::sync::Arc;

use crate::cache::check::{check, CacheCheckOptions};
use crate::commands::utils::*;
use crate::commands::Command;
use crate::report::*;

//------------------------------------------

pub struct CacheCheckCommand;

impl CacheCheckCommand {
    fn cli<'a>(&self) -> clap::Command<'a> {
        clap::Command::new(self.name())
            .color(clap::ColorChoice::Never)
            .version(crate::version::tools_version())
            .about("Validates cache metadata on a device or file.")
            // flags
            .arg(
                Arg::new("ASYNC_IO")
                    .help("Force use of io_uring for synchronous io")
                    .long("async-io")
                    .hide(true),
            )
            .arg(
                Arg::new("AUTO_REPAIR")
                    .help("Auto repair trivial issues.")
                    .long("auto-repair"),
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
                    .help("Only check the superblock.")
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
                    .help("Don't check the mapping tree")
                    .long("skip-mappings"),
            )
            // arguments
            .arg(
                Arg::new("INPUT")
                    .help("Specify the input device to check")
                    .required(true)
                    .index(1),
            )
    }
}

impl<'a> Command<'a> for CacheCheckCommand {
    fn name(&self) -> &'a str {
        "cache_check"
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

        let opts = CacheCheckOptions {
            dev: input_file,
            async_io: matches.is_present("ASYNC_IO"),
            sb_only: matches.is_present("SB_ONLY"),
            skip_mappings: matches.is_present("SKIP_MAPPINGS"),
            skip_hints: matches.is_present("SKIP_HINTS"),
            skip_discards: matches.is_present("SKIP_DISCARDS"),
            ignore_non_fatal: matches.is_present("IGNORE_NON_FATAL"),
            auto_repair: matches.is_present("AUTO_REPAIR"),
            report: report.clone(),
        };

        to_exit_code(&report, check(opts))
    }
}

//------------------------------------------
