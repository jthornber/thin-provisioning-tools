extern crate clap;

use clap::{Arg, ArgAction};
use std::path::Path;

use crate::cache::repair::{repair, CacheRepairOptions};
use crate::commands::engine::*;
use crate::commands::utils::*;
use crate::commands::Command;
use crate::report::*;

pub struct CacheRepairCommand;

impl CacheRepairCommand {
    fn cli(&self) -> clap::Command {
        let cmd = clap::Command::new(self.name())
            .next_display_order(None)
            .version(crate::tools_version!())
            .about("Repair binary cache metadata, and write it to a different device or file")
            .arg(
                Arg::new("QUIET")
                    .help("Suppress output messages, return only exit code.")
                    .short('q')
                    .long("quiet")
                    .action(ArgAction::SetTrue),
            )
            // options
            .arg(
                Arg::new("INPUT")
                    .help("Specify the input device")
                    .short('i')
                    .long("input")
                    .value_name("FILE")
                    .required(true),
            )
            .arg(
                Arg::new("OUTPUT")
                    .help("Specify the output device")
                    .short('o')
                    .long("output")
                    .value_name("FILE")
                    .required(true),
            )
            // a dummy argument for compatibility with lvconvert
            .arg(Arg::new("DUMMY").required(false).hide(true).index(1));

        verbose_args(engine_args(cmd))
    }
}

impl<'a> Command<'a> for CacheRepairCommand {
    fn name(&self) -> &'a str {
        "cache_repair"
    }

    fn run(&self, args: &mut dyn Iterator<Item = std::ffi::OsString>) -> exitcode::ExitCode {
        let matches = self.cli().get_matches_from(args);

        let report = mk_report(matches.get_flag("QUIET"));
        let log_level = match parse_log_level(&matches) {
            Ok(level) => level,
            Err(e) => return to_exit_code::<()>(&report, Err(anyhow::Error::msg(e))),
        };
        report.set_level(log_level);

        let input_file = Path::new(matches.get_one::<String>("INPUT").unwrap());
        let output_file = Path::new(matches.get_one::<String>("OUTPUT").unwrap());

        if let Err(e) = check_input_file(input_file)
            .and_then(check_file_not_tiny)
            .and_then(|_| check_output_file(output_file))
        {
            return to_exit_code::<()>(&report, Err(e));
        }

        let engine_opts = parse_engine_opts(ToolType::Cache, &matches);
        if engine_opts.is_err() {
            return to_exit_code(&report, engine_opts);
        }
        let engine_opts = engine_opts.unwrap();

        let opts = CacheRepairOptions {
            input: input_file,
            output: output_file,
            engine_opts,
            report: report.clone(),
        };

        to_exit_code(&report, repair(opts))
    }
}
