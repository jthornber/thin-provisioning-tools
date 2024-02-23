extern crate clap;

use clap::builder::{PossibleValuesParser, TypedValueParser};
use clap::{value_parser, Arg, ArgAction};
use std::path::Path;

use crate::commands::engine::*;
use crate::commands::utils::*;
use crate::commands::Command;
use crate::report::*;
use crate::thin::dump::{dump, OutputFormat, ThinDumpOptions};
use crate::thin::metadata_repair::SuperblockOverrides;
use crate::version::*;

pub struct ThinDumpCommand;

impl ThinDumpCommand {
    fn cli(&self) -> clap::Command {
        let cmd = clap::Command::new(self.name())
            .next_display_order(None)
            .version(crate::tools_version!())
            .disable_version_flag(true)
            .about("Dump thin-provisioning metadata to stdout in XML format")
            .arg(
                Arg::new("QUIET")
                    .help("Suppress output messages, return only exit code.")
                    .short('q')
                    .long("quiet")
                    .action(ArgAction::SetTrue),
            )
            .arg(
                Arg::new("REPAIR")
                    .help("Repair the metadata whilst dumping it")
                    .short('r')
                    .long("repair")
                    .action(ArgAction::SetTrue)
                    .conflicts_with("METADATA_SNAPSHOT"),
            )
            .arg(
                Arg::new("SKIP_MAPPINGS")
                    .help("Do not dump the mappings")
                    .long("skip-mappings")
                    .action(ArgAction::SetTrue),
            )
            // options
            .arg(
                Arg::new("DATA_BLOCK_SIZE")
                    .help("Provide the data block size for repairing")
                    .long("data-block-size")
                    .value_name("SECTORS")
                    .value_parser(value_parser!(u32)),
            )
            .arg(
                Arg::new("DEV_ID")
                    .help("Dump the specified device")
                    .long("dev-id")
                    .action(clap::ArgAction::Append)
                    .value_name("THIN_ID")
                    .value_parser(value_parser!(u64)),
            )
            .arg(
                Arg::new("FORMAT")
                    .help("Choose the output format")
                    .short('f')
                    .long("format")
                    .value_name("TYPE")
                    .value_parser(
                        PossibleValuesParser::new(["xml", "human_readable"])
                            .map(|s| s.parse::<OutputFormat>().unwrap()),
                    )
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
                    .value_parser(value_parser!(u64))
                    .num_args(0..=1)
                    .require_equals(true),
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
                    .help("Specify the output file rather than stdout")
                    .short('o')
                    .long("output")
                    .value_name("FILE"),
            )
            .arg(
                Arg::new("TRANSACTION_ID")
                    .help("Override the transaction id if needed")
                    .long("transaction-id")
                    .value_name("NUM")
                    .value_parser(value_parser!(u64)),
            )
            // arguments
            .arg(
                Arg::new("INPUT")
                    .help("Specify the input device to dump")
                    .required(true)
                    .index(1),
            );
        verbose_args(engine_args(version_args(cmd)))
    }
}

impl<'a> Command<'a> for ThinDumpCommand {
    fn name(&self) -> &'a str {
        "thin_dump"
    }

    fn run(&self, args: &mut dyn Iterator<Item = std::ffi::OsString>) -> exitcode::ExitCode {
        let matches = self.cli().get_matches_from(args);
        display_version(&matches);

        let input_file = Path::new(matches.get_one::<String>("INPUT").unwrap());
        let output_file = matches.get_one::<String>("OUTPUT").map(Path::new);

        let report = mk_report(matches.get_flag("QUIET"));
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

        let selected_devs: Option<Vec<u64>> = matches
            .get_many::<u64>("DEV_ID")
            .map(|devs| devs.copied().collect());

        let opts = ThinDumpOptions {
            input: input_file,
            output: output_file,
            engine_opts: engine_opts.unwrap(),
            report: report.clone(),
            repair: matches.get_flag("REPAIR"),
            skip_mappings: matches.get_flag("SKIP_MAPPINGS"),
            overrides: SuperblockOverrides {
                transaction_id: matches.get_one::<u64>("TRANSACTION_ID").cloned(),
                data_block_size: matches.get_one::<u32>("DATA_BLOCK_SIZE").cloned(),
                nr_data_blocks: matches.get_one::<u64>("NR_DATA_BLOCKS").cloned(),
            },
            selected_devs,
            format: matches.get_one::<OutputFormat>("FORMAT").unwrap().clone(),
        };

        to_exit_code(&report, dump(opts))
    }
}
