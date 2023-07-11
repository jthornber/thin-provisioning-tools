extern crate clap;

use clap::{value_parser, Arg, ArgAction};
use std::path::Path;

use crate::commands::engine::*;
use crate::commands::utils::*;
use crate::commands::Command;
use crate::report::{parse_log_level, verbose_args};
use crate::thin::metadata_repair::SuperblockOverrides;
use crate::thin::restore::{restore, ThinRestoreOptions};

pub struct ThinRestoreCommand;

impl ThinRestoreCommand {
    fn cli(&self) -> clap::Command {
        let cmd = clap::Command::new(self.name())
            .next_display_order(None)
            .version(crate::tools_version!())
            .about("Convert XML format metadata to binary.")
            .arg(
                Arg::new("QUIET")
                    .help("Suppress output messages, return only exit code.")
                    .short('q')
                    .long("quiet")
                    .action(ArgAction::SetTrue),
            )
            // options
            .arg(
                Arg::new("DATA_BLOCK_SIZE")
                    .help("Override the data block size if needed")
                    .long("data-block-size")
                    .value_name("SECTORS")
                    .value_parser(value_parser!(u32)),
            )
            .arg(
                Arg::new("INPUT")
                    .help("Specify the input xml")
                    .short('i')
                    .long("input")
                    .value_name("FILE")
                    .required(true),
            )
            .arg(
                Arg::new("NR_DATA_BLOCKS")
                    .help("Override the number of data blocks if needed")
                    .long("nr-data-blocks")
                    .value_name("NUM")
                    .value_parser(value_parser!(u64)),
            )
            .arg(
                Arg::new("OUTPUT")
                    .help("Specify the output device")
                    .short('o')
                    .long("output")
                    .value_name("FILE")
                    .required(true),
            )
            .arg(
                Arg::new("TRANSACTION_ID")
                    .help("Override the transaction id if needed")
                    .long("transaction-id")
                    .value_name("NUM")
                    .value_parser(value_parser!(u64)),
            );
        verbose_args(engine_args(cmd))
    }
}

impl<'a> Command<'a> for ThinRestoreCommand {
    fn name(&self) -> &'a str {
        "thin_restore"
    }

    fn run(&self, args: &mut dyn Iterator<Item = std::ffi::OsString>) -> exitcode::ExitCode {
        let matches = self.cli().get_matches_from(args);

        let input_file = Path::new(matches.get_one::<String>("INPUT").unwrap());
        let output_file = Path::new(matches.get_one::<String>("OUTPUT").unwrap());

        let report = mk_report(matches.get_flag("QUIET"));
        let log_level = match parse_log_level(&matches) {
            Ok(level) => level,
            Err(e) => return to_exit_code::<()>(&report, Err(anyhow::Error::msg(e))),
        };
        report.set_level(log_level);

        if let Err(e) = check_input_file(input_file).and_then(|_| check_output_file(output_file)) {
            return to_exit_code::<()>(&report, Err(e));
        }

        let engine_opts = parse_engine_opts(ToolType::Thin, &matches);
        if engine_opts.is_err() {
            return to_exit_code(&report, engine_opts);
        }

        let opts = ThinRestoreOptions {
            input: input_file,
            output: output_file,
            engine_opts: engine_opts.unwrap(),
            report: report.clone(),
            overrides: SuperblockOverrides {
                transaction_id: matches.get_one::<u64>("TRANSACTION_ID").cloned(),
                data_block_size: matches.get_one::<u32>("DATA_BLOCK_SIZE").cloned(),
                nr_data_blocks: matches.get_one::<u64>("NR_DATA_BLOCKS").cloned(),
            },
        };

        to_exit_code(&report, restore(opts))
    }
}
