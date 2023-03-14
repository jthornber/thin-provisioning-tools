extern crate clap;

use clap::Arg;
use std::path::Path;

use crate::cache::restore::{restore, CacheRestoreOptions};
use crate::commands::engine::*;
use crate::commands::utils::*;
use crate::commands::Command;
use crate::report::{parse_log_level, verbose_args};

pub struct CacheRestoreCommand;

impl CacheRestoreCommand {
    fn cli<'a>(&self) -> clap::Command<'a> {
        let cmd = clap::Command::new(self.name())
            .color(clap::ColorChoice::Never)
            .version(crate::tools_version!())
            .about("Convert XML format metadata to binary.")
            .arg(
                Arg::new("QUIET")
                    .help("Suppress output messages, return only exit code")
                    .short('q')
                    .long("quiet"),
            )
            .arg(
                Arg::new("OMIT_CLEAN_SHUTDOWN")
                    .help("Don't set the clean shutdown flag")
                    .long("omit-clean-shutdown"),
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
                Arg::new("METADATA_VERSION")
                    .help("Specify the output metadata version")
                    .long("metadata-version")
                    .value_name("NUM")
                    .possible_values(["1", "2"])
                    .default_value("2"),
            )
            .arg(
                Arg::new("OUTPUT")
                    .help("Specify the output device")
                    .short('o')
                    .long("output")
                    .value_name("FILE")
                    .required(true),
            );
        verbose_args(engine_args(cmd))
    }
}

impl<'a> Command<'a> for CacheRestoreCommand {
    fn name(&self) -> &'a str {
        "cache_restore"
    }

    fn run(&self, args: &mut dyn Iterator<Item = std::ffi::OsString>) -> exitcode::ExitCode {
        let matches = self.cli().get_matches_from(args);

        let input_file = Path::new(matches.value_of("INPUT").unwrap());
        let output_file = Path::new(matches.value_of("OUTPUT").unwrap());

        let report = mk_report(matches.is_present("QUIET"));
        let log_level = match parse_log_level(&matches) {
            Ok(level) => level,
            Err(e) => return to_exit_code::<()>(&report, Err(anyhow::Error::msg(e))),
        };
        report.set_level(log_level);

        if let Err(e) = check_input_file(input_file).and_then(|_| check_output_file(output_file)) {
            return to_exit_code::<()>(&report, Err(e));
        }

        let engine_opts = parse_engine_opts(ToolType::Cache, &matches);
        if engine_opts.is_err() {
            return to_exit_code(&report, engine_opts);
        }

        let opts = CacheRestoreOptions {
            input: input_file,
            output: output_file,
            metadata_version: matches.value_of_t_or_exit::<u8>("METADATA_VERSION"),
            engine_opts: engine_opts.unwrap(),
            report: report.clone(),
            omit_clean_shutdown: matches.is_present("OMIT_CLEAN_SHUTDOWN"),
        };

        to_exit_code(&report, restore(opts))
    }
}
