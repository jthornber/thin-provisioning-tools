extern crate clap;

use clap::Arg;
use std::path::Path;

use crate::commands::utils::*;
use crate::commands::Command;
use crate::thin::metadata_repair::SuperblockOverrides;
use crate::thin::restore::{restore, ThinRestoreOptions};

pub struct ThinRestoreCommand;

impl ThinRestoreCommand {
    fn cli<'a>(&self) -> clap::Command<'a> {
        clap::Command::new(self.name())
            .color(clap::ColorChoice::Never)
            .version(crate::version::tools_version())
            .about("Convert XML format metadata to binary.")
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
            // options
            .arg(
                Arg::new("DATA_BLOCK_SIZE")
                    .help("Override the data block size if needed")
                    .long("data-block-size")
                    .value_name("SECTORS"),
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
    }
}

impl<'a> Command<'a> for ThinRestoreCommand {
    fn name(&self) -> &'a str {
        "thin_restore"
    }

    fn run(&self, args: &mut dyn Iterator<Item = std::ffi::OsString>) -> std::io::Result<()> {
        let matches = self.cli().get_matches_from(args);

        let input_file = Path::new(matches.value_of("INPUT").unwrap());
        let output_file = Path::new(matches.value_of("OUTPUT").unwrap());

        let report = mk_report(matches.is_present("QUIET"));
        check_input_file(input_file, &report);
        check_output_file(output_file, &report);

        let opts = ThinRestoreOptions {
            input: input_file,
            output: output_file,
            async_io: matches.is_present("ASYNC_IO"),
            report: report.clone(),
            overrides: SuperblockOverrides {
                transaction_id: optional_value_or_exit::<u64>(&matches, "TRANSACTION_ID"),
                data_block_size: optional_value_or_exit::<u32>(&matches, "DATA_BLOCK_SIZE"),
                nr_data_blocks: optional_value_or_exit::<u64>(&matches, "NR_DATA_BLOCKS"),
            },
        };

        restore(opts).map_err(|reason| {
            report.fatal(&format!("{}", reason));
            std::io::Error::from_raw_os_error(libc::EPERM)
        })
    }
}
