extern crate clap;

use clap::Arg;
use std::path::Path;

use crate::commands::engine::*;
use crate::commands::utils::*;
use crate::commands::Command;
use crate::report::*;
use crate::thin::dump::{dump, OutputFormat, ThinDumpOptions};
use crate::thin::metadata_repair::SuperblockOverrides;

pub struct ThinDumpCommand;

impl ThinDumpCommand {
    fn cli<'a>(&self) -> clap::Command<'a> {
        let cmd = clap::Command::new(self.name())
            .color(clap::ColorChoice::Never)
            .version(crate::tools_version!())
            .about("Dump thin-provisioning metadata to stdout in XML format")
            .arg(
                Arg::new("QUIET")
                    .help("Suppress output messages, return only exit code.")
                    .short('q')
                    .long("quiet"),
            )
            .arg(
                Arg::new("REPAIR")
                    .help("Repair the metadata whilst dumping it")
                    .short('r')
                    .long("repair")
                    .conflicts_with("METADATA_SNAPSHOT"),
            )
            .arg(
                Arg::new("SKIP_MAPPINGS")
                    .help("Do not dump the mappings")
                    .long("skip-mappings"),
            )
            // options
            .arg(
                Arg::new("DATA_BLOCK_SIZE")
                    .help("Provide the data block size for repairing")
                    .long("data-block-size")
                    .value_name("SECTORS"),
            )
            .arg(
                Arg::new("DEV_ID")
                    .help("Dump the specified device")
                    .long("dev-id")
                    .multiple_occurrences(true)
                    .value_name("THIN_ID"),
            )
            .arg(
                Arg::new("FORMAT")
                    .help("Choose the output format")
                    .short('f')
                    .long("format")
                    .value_name("TYPE")
                    .possible_values(["xml", "human_readable"])
                    .hide_possible_values(true)
                    .default_value("xml")
                    .hide_default_value(true),
            )
            .arg(
                Arg::new("METADATA_SNAPSHOT")
                    .help("Access the metadata snapshot on a live pool")
                    .short('m')
                    .long("metadata-snap")
                    .value_name("BLOCKNR")
                    .min_values(0)
                    .max_values(1)
                    .require_equals(true),
            )
            .arg(
                Arg::new("NR_DATA_BLOCKS")
                    .help("Override the number of data blocks if needed")
                    .long("nr-data-blocks")
                    .value_name("NUM"),
            )
            .arg(
                Arg::new("OUTPUT")
                    .help("Specify the output file rather than stdout")
                    .short('o')
                    .long("output")
                    .value_name("FILE"),
            )
            .arg(
                Arg::new("TRANSACTION_ID")
                    .help("Override the transaction id if needed")
                    .long("transaction-id")
                    .value_name("NUM"),
            )
            // arguments
            .arg(
                Arg::new("INPUT")
                    .help("Specify the input device to dump")
                    .required(true)
                    .index(1),
            );
        verbose_args(engine_args(cmd))
    }
}

impl<'a> Command<'a> for ThinDumpCommand {
    fn name(&self) -> &'a str {
        "thin_dump"
    }

    fn run(&self, args: &mut dyn Iterator<Item = std::ffi::OsString>) -> exitcode::ExitCode {
        let matches = self.cli().get_matches_from(args);

        let input_file = Path::new(matches.value_of("INPUT").unwrap());
        let output_file = if matches.is_present("OUTPUT") {
            Some(Path::new(matches.value_of("OUTPUT").unwrap()))
        } else {
            None
        };

        let report = mk_report(matches.is_present("QUIET"));
        let log_level = match parse_log_level(&matches) {
            Ok(level) => level,
            Err(e) => return to_exit_code::<()>(&report, Err(anyhow::Error::msg(e))),
        };
        report.set_level(log_level);

        if let Err(e) = check_input_file(input_file).and_then(check_file_not_tiny) {
            return to_exit_code::<()>(&report, Err(e));
        }

        let engine_opts = parse_engine_opts(ToolType::Thin, &matches);
        if engine_opts.is_err() {
            return to_exit_code(&report, engine_opts);
        }

        let selected_devs = if matches.is_present("DEV_ID") {
            let devs: Vec<u64> = matches
                .values_of_t_or_exit::<u64>("DEV_ID")
                .into_iter()
                .collect();
            Some(devs)
        } else {
            None
        };

        let opts = ThinDumpOptions {
            input: input_file,
            output: output_file,
            engine_opts: engine_opts.unwrap(),
            report: report.clone(),
            repair: matches.is_present("REPAIR"),
            skip_mappings: matches.is_present("SKIP_MAPPINGS"),
            overrides: SuperblockOverrides {
                transaction_id: optional_value_or_exit::<u64>(&matches, "TRANSACTION_ID"),
                data_block_size: optional_value_or_exit::<u32>(&matches, "DATA_BLOCK_SIZE"),
                nr_data_blocks: optional_value_or_exit::<u64>(&matches, "NR_DATA_BLOCKS"),
            },
            selected_devs,
            format: matches.value_of_t_or_exit::<OutputFormat>("FORMAT"),
        };

        to_exit_code(&report, dump(opts))
    }
}
