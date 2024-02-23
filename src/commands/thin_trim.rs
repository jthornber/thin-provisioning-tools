extern crate clap;

use clap::{Arg, ArgAction};
use std::path::Path;

use crate::commands::engine::*;
use crate::commands::utils::*;
use crate::report::{parse_log_level, verbose_args};
use crate::thin::check::{check, ThinCheckOptions};
use crate::thin::trim::{trim, ThinTrimOptions};
use crate::version::*;

//------------------------------------------
use crate::commands::Command;

pub struct ThinTrimCommand;

impl ThinTrimCommand {
    fn cli(&self) -> clap::Command {
        let cmd = clap::Command::new(self.name())
            .next_display_order(None)
            .version(crate::tools_version!())
            .disable_version_flag(true)
            .about("Issue discard requests for free pool space (offline tool).")
            .arg(
                Arg::new("QUIET")
                    .help("Suppress output messages, return only exit code.")
                    .short('q')
                    .long("quiet")
                    .action(ArgAction::SetTrue),
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
        verbose_args(engine_args(version_args(cmd)))
    }
}

impl<'a> Command<'a> for ThinTrimCommand {
    fn name(&self) -> &'a str {
        "thin_trim"
    }

    fn run(&self, args: &mut dyn Iterator<Item = std::ffi::OsString>) -> exitcode::ExitCode {
        let matches = self.cli().get_matches_from(args);
        display_version(&matches);

        let metadata_dev = Path::new(matches.get_one::<String>("METADATA_DEV").unwrap());
        let data_dev = Path::new(matches.get_one::<String>("DATA_DEV").unwrap());

        let report = mk_report(matches.get_flag("QUIET"));
        let log_level = match parse_log_level(&matches) {
            Ok(level) => level,
            Err(e) => return to_exit_code::<()>(&report, Err(anyhow::Error::msg(e))),
        };
        report.set_level(log_level);

        if let Err(e) = check_input_file(metadata_dev)
            .and_then(check_file_not_tiny)
            .and_then(|_| check_input_file(data_dev))
        {
            return to_exit_code::<()>(&report, Err(e));
        }

        let engine_opts = match parse_engine_opts(ToolType::Thin, &matches) {
            Ok(opts) => opts,
            Err(_) => return exitcode::USAGE,
        };

        let check_opts = ThinCheckOptions {
            input: metadata_dev,
            engine_opts: engine_opts.clone(),
            sb_only: false,
            skip_mappings: false,
            ignore_non_fatal: false,
            auto_repair: false,
            clear_needs_check: false,
            override_mapping_root: None,
            override_details_root: None,
            report: report.clone(),
        };

        if check(check_opts).is_err() {
            report.fatal(
                "metadata contains errors (run thin_check for details).\n\
                perhaps you need to run thin_repair.",
            );
            return exitcode::DATAERR;
        }

        let opts = ThinTrimOptions {
            metadata_dev,
            data_dev,
            engine_opts,
            report: report.clone(),
        };

        to_exit_code(&report, trim(opts))
    }
}
