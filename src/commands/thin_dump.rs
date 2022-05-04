extern crate clap;

use atty::Stream;
use clap::Arg;
use std::path::Path;

use std::sync::Arc;

use crate::commands::utils::*;
use crate::commands::Command;
use crate::report::*;
use crate::thin::dump::{dump, ThinDumpOptions};
use crate::thin::metadata_repair::SuperblockOverrides;

pub struct ThinDumpCommand;

impl ThinDumpCommand {
    fn cli<'a>(&self) -> clap::Command<'a> {
        clap::Command::new(self.name())
            .color(clap::ColorChoice::Never)
            .version(crate::version::tools_version())
            .about("Dump thin-provisioning metadata to stdout in XML format")
            // flags
            .arg(
                Arg::new("ASYNC_IO")
                    .help("Force use of io_uring for synchronous io")
                    .long("async-io")
                    .hide(true),
            )
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
                Arg::new("METADATA_SNAPSHOT")
                    .help("Access the metadata snapshot on a live pool")
                    .short('m')
                    .long("metadata-snapshot")
                    .value_name("METADATA_SNAPSHOT"),
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
            )
    }
}

impl<'a> Command<'a> for ThinDumpCommand {
    fn name(&self) -> &'a str {
        "thin_dump"
    }

    fn run(&self, args: &mut dyn Iterator<Item = std::ffi::OsString>) -> std::io::Result<()> {
        let matches = self.cli().get_matches_from(args);

        let input_file = Path::new(matches.value_of("INPUT").unwrap());
        let output_file = if matches.is_present("OUTPUT") {
            Some(Path::new(matches.value_of("OUTPUT").unwrap()))
        } else {
            None
        };

        let report = std::sync::Arc::new(mk_simple_report());
        check_input_file(input_file, &report);

        let report = if matches.is_present("QUIET") {
            std::sync::Arc::new(mk_quiet_report())
        } else if atty::is(Stream::Stdout) {
            std::sync::Arc::new(mk_progress_bar_report())
        } else {
            Arc::new(mk_simple_report())
        };

        let opts = ThinDumpOptions {
            input: input_file,
            output: output_file,
            async_io: matches.is_present("ASYNC_IO"),
            report: report.clone(),
            repair: matches.is_present("REPAIR"),
            use_metadata_snap: matches.is_present("METADATA_SNAPSHOT"),
            overrides: SuperblockOverrides {
                transaction_id: optional_value_or_exit::<u64>(&matches, "TRANSACTION_ID"),
                data_block_size: optional_value_or_exit::<u32>(&matches, "DATA_BLOCK_SIZE"),
                nr_data_blocks: optional_value_or_exit::<u64>(&matches, "NR_DATA_BLOCKS"),
            },
        };

        dump(opts).map_err(|reason| {
            report.fatal(&format!("{}", reason));
            std::io::Error::from_raw_os_error(libc::EPERM)
        })
    }
}
