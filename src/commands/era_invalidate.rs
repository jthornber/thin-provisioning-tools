extern crate clap;

use clap::Arg;
use std::path::Path;

use crate::commands::utils::*;
use crate::commands::Command;
use crate::era::invalidate::{invalidate, EraInvalidateOptions};

//------------------------------------------

pub struct EraInvalidateCommand;

impl EraInvalidateCommand {
    fn cli<'a>(&self) -> clap::Command<'a> {
        clap::Command::new(self.name())
            .color(clap::ColorChoice::Never)
            .version(crate::version::tools_version())
            .about("List blocks that may have changed since a given era")
            // flags
            .arg(
                Arg::new("ASYNC_IO")
                    .help("Force use of io_uring for synchronous io")
                    .long("async-io")
                    .hide(true),
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
            )
            .arg(
                Arg::new("WRITTEN_SINCE")
                    .help("Blocks written since the given era will be listed")
                    .long("written-since")
                    .required(true)
                    .value_name("ERA"),
            )
    }
}

impl<'a> Command<'a> for EraInvalidateCommand {
    fn name(&self) -> &'a str {
        "era_invalidate"
    }

    fn run(&self, args: &mut dyn Iterator<Item = std::ffi::OsString>) -> std::io::Result<()> {
        let matches = self.cli().get_matches_from(args);

        let input_file = Path::new(matches.value_of("INPUT").unwrap());
        let output_file = if matches.is_present("OUTPUT") {
            Some(Path::new(matches.value_of("OUTPUT").unwrap()))
        } else {
            None
        };

        // Create a temporary report just in case these checks
        // need to report anything.
        let report = std::sync::Arc::new(crate::report::mk_simple_report());
        check_input_file(input_file, &report);
        check_file_not_tiny(input_file, &report);
        drop(report);

        let opts = EraInvalidateOptions {
            input: input_file,
            output: output_file,
            async_io: matches.is_present("ASYNC_IO"),
            threshold: optional_value_or_exit::<u32>(&matches, "WRITTEN_SINCE").unwrap_or(0),
            use_metadata_snap: matches.is_present("METADATA_SNAP"),
        };

        invalidate(&opts).map_err(|reason| {
            eprintln!("{}", reason);
            std::io::Error::from_raw_os_error(libc::EPERM)
        })
    }
}

//------------------------------------------
