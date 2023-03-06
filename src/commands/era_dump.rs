extern crate clap;

use clap::Arg;
use std::path::Path;

use crate::commands::engine::*;
use crate::commands::utils::*;
use crate::commands::Command;
use crate::era::dump::{dump, EraDumpOptions};

//------------------------------------------

pub struct EraDumpCommand;

impl EraDumpCommand {
    fn cli<'a>(&self) -> clap::Command<'a> {
        let cmd = clap::Command::new(self.name())
            .color(clap::ColorChoice::Never)
            .version(crate::tools_version!())
            .about("Dump the era metadata to stdout in XML format")
            .arg(
                Arg::new("LOGICAL")
                    .help("Fold any unprocessed write sets into the final era array")
                    .long("logical"),
            )
            .arg(
                Arg::new("REPAIR")
                    .help("Repair the metadata whilst dumping it")
                    .short('r')
                    .long("repair"),
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
        engine_args(cmd)
    }
}

impl<'a> Command<'a> for EraDumpCommand {
    fn name(&self) -> &'a str {
        "era_dump"
    }

    fn run(&self, args: &mut dyn Iterator<Item = std::ffi::OsString>) -> exitcode::ExitCode {
        let matches = self.cli().get_matches_from(args);

        let input_file = Path::new(matches.value_of("INPUT").unwrap());
        let output_file = if matches.is_present("OUTPUT") {
            Some(Path::new(matches.value_of("OUTPUT").unwrap()))
        } else {
            None
        };

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
            logical: matches.is_present("LOGICAL"),
            repair: matches.is_present("REPAIR"),
        };

        to_exit_code(&report, dump(opts))
    }
}

//------------------------------------------
