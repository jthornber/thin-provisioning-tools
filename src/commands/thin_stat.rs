use clap::Arg;
use std::path::Path;

use crate::commands::engine::*;
use crate::commands::utils::*;
use crate::report::mk_simple_report;
use crate::thin::stat::*;

//------------------------------------------
use crate::commands::Command;

pub struct ThinStatCommand;

impl ThinStatCommand {
    fn cli<'a>(&self) -> clap::Command<'a> {
        let cmd = clap::Command::new(self.name())
            .color(clap::ColorChoice::Never)
            .version(crate::tools_version!())
            .about("Tool to show metadata statistics")
            .arg(
                Arg::new("OP")
                    .help("Choose the target to stat")
                    .long("op")
                    .default_value("data_blocks"),
            )
            .arg(
                Arg::new("INPUT")
                    .help("Specify the input device")
                    .required(true)
                    .index(1),
            );

        engine_args(cmd)
    }
}

impl<'a> Command<'a> for ThinStatCommand {
    fn name(&self) -> &'a str {
        "thin_stat"
    }

    fn run(&self, args: &mut dyn Iterator<Item = std::ffi::OsString>) -> exitcode::ExitCode {
        let matches = self.cli().get_matches_from(args);
        let report = std::sync::Arc::new(mk_simple_report());

        let engine_opts = parse_engine_opts(ToolType::Thin, &matches);
        if engine_opts.is_err() {
            return to_exit_code(&report, engine_opts);
        }

        let opts = ThinStatOpts {
            engine_opts: engine_opts.unwrap(),
            input: Path::new(matches.value_of("INPUT").unwrap()),
            op: match matches.value_of("OP") {
                Some("data_blocks") => StatOp::DataBlockRefCounts,
                Some("metadata_blocks") => StatOp::MetadataBlockRefCounts,
                Some("data_run_len") => StatOp::DataRunLength,
                _ => return exitcode::USAGE,
            },
        };

        to_exit_code(&report, stat(opts))
    }
}

//------------------------------------------
