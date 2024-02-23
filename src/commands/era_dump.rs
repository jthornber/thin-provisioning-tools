extern crate clap;

use clap::{Arg, ArgAction};
use std::path::Path;

use crate::commands::engine::*;
use crate::commands::utils::*;
use crate::commands::Command;
use crate::era::dump::{dump, EraDumpOptions};
use crate::version::*;

//------------------------------------------

pub struct EraDumpCommand;

impl EraDumpCommand {
    fn cli(&self) -> clap::Command {
        let cmd = clap::Command::new(self.name())
            .next_display_order(None)
            .version(crate::tools_version!())
            .disable_version_flag(true)
            .about("Dump the era metadata to stdout in XML format")
            .arg(
                Arg::new("LOGICAL")
                    .help("Fold any unprocessed write sets into the final era array")
                    .long("logical")
                    .action(ArgAction::SetTrue),
            )
            .arg(
                Arg::new("REPAIR")
                    .help("Repair the metadata whilst dumping it")
                    .short('r')
                    .long("repair")
                    .action(ArgAction::SetTrue),
            )
            // options
            .arg(
                Arg::new("OUTPUT")
                    .help("Specify the output file rather than stdout")
                    .short('o')
                    .long("output")
                    .value_name("FILE"),
            )
            // arguments
            .arg(
                Arg::new("INPUT")
                    .help("Specify the input device to dump")
                    .required(true)
                    .index(1),
            );
        engine_args(version_args(cmd))
    }
}

impl<'a> Command<'a> for EraDumpCommand {
    fn name(&self) -> &'a str {
        "era_dump"
    }

    fn run(&self, args: &mut dyn Iterator<Item = std::ffi::OsString>) -> exitcode::ExitCode {
        let matches = self.cli().get_matches_from(args);
        display_version(&matches);

        let input_file = Path::new(matches.get_one::<String>("INPUT").unwrap());
        let output_file = matches.get_one::<String>("OUTPUT").map(Path::new);

        let report = std::sync::Arc::new(crate::report::mk_simple_report());

        if let Err(e) = check_input_file(input_file).and_then(check_file_not_tiny) {
            return to_exit_code::<()>(&report, Err(e));
        }

        let engine_opts = parse_engine_opts(ToolType::Era, &matches);
        if engine_opts.is_err() {
            return to_exit_code(&report, engine_opts);
        }

        let opts = EraDumpOptions {
            input: input_file,
            output: output_file,
            engine_opts: engine_opts.unwrap(),
            logical: matches.get_flag("LOGICAL"),
            repair: matches.get_flag("REPAIR"),
        };

        to_exit_code(&report, dump(opts))
    }
}

//------------------------------------------
