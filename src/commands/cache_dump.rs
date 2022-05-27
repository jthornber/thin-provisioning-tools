extern crate clap;

use clap::Arg;
use std::path::Path;

use crate::cache::dump::{dump, CacheDumpOptions};
use crate::commands::utils::*;
use crate::commands::Command;
use crate::dump_utils::OutputError;

//------------------------------------------

pub struct CacheDumpCommand;

impl CacheDumpCommand {
    fn cli<'a>(&self) -> clap::Command<'a> {
        clap::Command::new(self.name())
            .color(clap::ColorChoice::Never)
            .version(crate::version::tools_version())
            .about("Dump the cache metadata to stdout in XML format")
            // flags
            .arg(
                Arg::new("ASYNC_IO")
                    .help("Force use of io_uring for synchronous io")
                    .long("async-io")
                    .hide(true),
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
            )
    }
}

impl<'a> Command<'a> for CacheDumpCommand {
    fn name(&self) -> &'a str {
        "cache_dump"
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

        let opts = CacheDumpOptions {
            input: input_file,
            output: output_file,
            async_io: matches.is_present("ASYNC_IO"),
            repair: matches.is_present("REPAIR"),
        };

        if let Err(e) = dump(opts) {
            if !e.is::<OutputError>() {
                report.fatal(&format!("{:?}", e));
                report.fatal(
                    "metadata contains errors (run cache_check for details).\n\
                    perhaps you wanted to run with --repair ?",
                );
            }
            return Err(std::io::Error::from_raw_os_error(libc::EPERM));
        }

        Ok(())
    }
}

//------------------------------------------
