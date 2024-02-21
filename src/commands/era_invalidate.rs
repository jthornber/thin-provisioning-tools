use clap::{value_parser, Arg};
use std::path::Path;

use crate::commands::engine::*;
use crate::commands::utils::*;
use crate::commands::Command;
use crate::era::invalidate::{invalidate, EraInvalidateOptions};
use crate::version::*;

//------------------------------------------

pub struct EraInvalidateCommand;

impl EraInvalidateCommand {
    fn cli(&self) -> clap::Command {
        let cmd = clap::Command::new(self.name())
            .next_display_order(None)
            .version(crate::tools_version!())
            .disable_version_flag(true)
            .about("List blocks that may have changed since a given era")
            .arg(
                Arg::new("METADATA_SNAPSHOT")
                    .help("Use the metadata snapshot rather than the current superblock")
                    .long("metadata-snapshot"),
            )
            // options
            .arg(
                Arg::new("OUTPUT")
                    .help("Specify the output file rather than stdout")
                    .short('o')
                    .long("output")
                    .value_name("FILE"),
            )
            .arg(
                Arg::new("WRITTEN_SINCE")
                    .help("Blocks written since the given era will be listed")
                    .long("written-since")
                    .required(true)
                    .value_name("ERA")
                    .value_parser(value_parser!(u32)),
            )
            // arguments
            .arg(
                Arg::new("INPUT")
                    .help("Specify the input device")
                    .required(true)
                    .index(1),
            );
        engine_args(version_args(cmd))
    }
}

impl<'a> Command<'a> for EraInvalidateCommand {
    fn name(&self) -> &'a str {
        "era_invalidate"
    }

    fn run(&self, args: &mut dyn Iterator<Item = std::ffi::OsString>) -> exitcode::ExitCode {
        let matches = self.cli().get_matches_from(args);
        display_version(&matches);

        let input_file = Path::new(matches.get_one::<String>("INPUT").unwrap());
        let output_file = matches.get_one::<String>("OUTPUT").map(Path::new);

        // Create a temporary report just in case these checks
        // need to report anything.
        let report = std::sync::Arc::new(crate::report::mk_simple_report());

        if let Err(e) = check_input_file(input_file).and_then(check_file_not_tiny) {
            return to_exit_code::<()>(&report, Err(e));
        }

        let engine_opts = parse_engine_opts(ToolType::Era, &matches);
        if engine_opts.is_err() {
            return to_exit_code(&report, engine_opts);
        }

        let opts = EraInvalidateOptions {
            input: input_file,
            output: output_file,
            engine_opts: engine_opts.unwrap(),
            threshold: matches.get_one::<u32>("WRITTEN_SINCE").map_or(0, |v| *v),
        };

        to_exit_code(&report, invalidate(&opts))
    }
}

//------------------------------------------
