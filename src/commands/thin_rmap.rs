extern crate clap;

use clap::{value_parser, Arg};
use std::ops::Range;
use std::path::Path;

use crate::commands::engine::*;
use crate::commands::utils::*;
use crate::commands::Command;
use crate::thin::rmap::*;

//------------------------------------------

pub struct ThinRmapCommand;

impl ThinRmapCommand {
    fn cli(&self) -> clap::Command {
        let cmd = clap::Command::new(self.name())
            .next_display_order(None)
            .version(crate::tools_version!())
            .about("Output reverse map of a thin provisioned region of blocks")
            // options
            .arg(
                // FIXME: clap doesn't support placing the index argument
                //        after the multiple arguments, e.g.,
                //        thin_rmap --region 0..1 --region 2..3 tmeta.bin
                Arg::new("REGION")
                    .help("Specify range of blocks on the data device")
                    .long("region")
                    .action(clap::ArgAction::Append)
                    .required(true)
                    .value_name("BLOCK_RANGE")
                    .value_parser(value_parser!(RangeU64)),
            )
            // arguments
            .arg(
                Arg::new("INPUT")
                    .help("Specify the input device")
                    .required(true)
                    .index(1),
            );
        engine_args(cmd)
    }
}

impl<'a> Command<'a> for ThinRmapCommand {
    fn name(&self) -> &'a str {
        "thin_rmap"
    }

    fn run(&self, args: &mut dyn Iterator<Item = std::ffi::OsString>) -> exitcode::ExitCode {
        let matches = self.cli().get_matches_from(args);
        let input_file = Path::new(matches.get_one::<String>("INPUT").unwrap());

        let report = mk_report(false);

        if let Err(e) = check_input_file(input_file).and_then(check_file_not_tiny) {
            return to_exit_code::<()>(&report, Err(e));
        }

        // FIXME: get rid of the intermediate RangeU64 struct
        let regions: Vec<Range<u64>> = matches
            .get_many::<RangeU64>("REGION")
            .unwrap()
            .map(|v| Range::<u64> {
                start: v.start,
                end: v.end,
            })
            .collect();

        let engine_opts = parse_engine_opts(ToolType::Thin, &matches);
        if engine_opts.is_err() {
            return to_exit_code(&report, engine_opts);
        }

        let opts = ThinRmapOptions {
            input: input_file,
            engine_opts: engine_opts.unwrap(),
            regions,
            report: report.clone(),
        };

        to_exit_code(&report, rmap(opts))
    }
}

//------------------------------------------
