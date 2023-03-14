extern crate clap;

use clap::Arg;
use std::path::Path;

use crate::commands::engine::*;
use crate::commands::utils::*;
use crate::commands::Command;
use crate::report::{parse_log_level, verbose_args};
use crate::thin::metadata_repair::SuperblockOverrides;
use crate::thin::repair::{repair, ThinRepairOptions};

pub struct ThinRepairCommand;

impl ThinRepairCommand {
    fn cli<'a>(&self) -> clap::Command<'a> {
        let cmd = clap::Command::new(self.name())
            .color(clap::ColorChoice::Never)
            .version(crate::tools_version!())
            .about("Repair thin-provisioning metadata, and write it to different device or file")
            .arg(
                Arg::new("QUIET")
                    .help("Suppress output messages, return only exit code.")
                    .short('q')
                    .long("quiet"),
            )
            // options
            .arg(
                Arg::new("DATA_BLOCK_SIZE")
                    .help("Provide the data block size for repairing")
                    .long("data-block-size")
                    .value_name("SECTORS"),
            )
            .arg(
                Arg::new("INPUT")
                    .help("Specify the input device")
                    .short('i')
                    .long("input")
                    .value_name("FILE")
                    .required(true),
            )
            .arg(
                Arg::new("NR_DATA_BLOCKS")
                    .help("Override the number of data blocks if needed")
                    .long("nr-data-blocks")
                    .value_name("NUM"),
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
                    .value_name("NUM"),
            )
            // a dummy argument for compatibility with lvconvert
            .arg(Arg::new("DUMMY").required(false).hide(true).index(1));

        verbose_args(engine_args(cmd))
    }
}

impl<'a> Command<'a> for ThinRepairCommand {
    fn name(&self) -> &'a str {
        "thin_repair"
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

        if let Err(e) = check_input_file(input_file)
            .and_then(check_file_not_tiny)
            .and_then(|_| check_output_file(output_file))
        {
            return to_exit_code::<()>(&report, Err(e));
        }

        let engine_opts = parse_engine_opts(ToolType::Thin, &matches);
        if engine_opts.is_err() {
            return to_exit_code(&report, engine_opts);
        }

        let opts = ThinRepairOptions {
            input: input_file,
            output: output_file,
            engine_opts: engine_opts.unwrap(),
            report: report.clone(),
            overrides: SuperblockOverrides {
                transaction_id: optional_value_or_exit::<u64>(&matches, "TRANSACTION_ID"),
                data_block_size: optional_value_or_exit::<u32>(&matches, "DATA_BLOCK_SIZE"),
                nr_data_blocks: optional_value_or_exit::<u64>(&matches, "NR_DATA_BLOCKS"),
            },
        };

        to_exit_code(&report, repair(opts))
    }
}
