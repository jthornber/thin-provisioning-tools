extern crate clap;

use atty::Stream;
use clap::Arg;
use std::path::Path;

use std::sync::Arc;

use crate::cache::repair::{repair, CacheRepairOptions};
use crate::commands::utils::*;
use crate::commands::Command;
use crate::report::*;

pub struct CacheRepairCommand;

impl CacheRepairCommand {
    fn cli<'a>(&self) -> clap::Command<'a> {
        clap::Command::new(self.name())
            .color(clap::ColorChoice::Never)
            .version(crate::version::tools_version())
            .about("Repair binary cache metadata, and write it to a different device or file")
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
                Arg::new("INPUT")
                    .help("Specify the input device")
                    .short('i')
                    .long("input")
                    .value_name("FILE")
                    .required(true),
            )
            .arg(
                Arg::new("OUTPUT")
                    .help("Specify the output device")
                    .short('o')
                    .long("output")
                    .value_name("FILE")
                    .required(true),
            )
    }
}

impl<'a> Command<'a> for CacheRepairCommand {
    fn name(&self) -> &'a str {
        "cache_repair"
    }

    fn run(&self, args: &mut dyn Iterator<Item = std::ffi::OsString>) -> std::io::Result<()> {
        let matches = self.cli().get_matches_from(args);

        let input_file = Path::new(matches.value_of("INPUT").unwrap());
        let output_file = Path::new(matches.value_of("OUTPUT").unwrap());

        let report = if matches.is_present("QUIET") {
            std::sync::Arc::new(mk_quiet_report())
        } else if atty::is(Stream::Stdout) {
            std::sync::Arc::new(mk_progress_bar_report())
        } else {
            Arc::new(mk_simple_report())
        };

        check_input_file(input_file, &report);

        let opts = CacheRepairOptions {
            input: input_file,
            output: output_file,
            async_io: matches.is_present("ASYNC_IO"),
            report: report.clone(),
        };

        repair(opts).map_err(|reason| {
            report.fatal(&format!("{}", reason));
            std::io::Error::from_raw_os_error(libc::EPERM)
        })
    }
}
