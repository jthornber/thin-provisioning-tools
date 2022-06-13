extern crate clap;

use clap::Arg;
use std::path::Path;

use crate::cache::restore::{restore, CacheRestoreOptions};
use crate::commands::utils::*;
use crate::commands::Command;

pub struct CacheRestoreCommand;

impl CacheRestoreCommand {
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
                Arg::new("INPUT")
                    .help("Specify the input xml")
                    .short('i')
                    .long("input")
                    .value_name("FILE")
                    .required(true),
            )
            .arg(
                Arg::new("METADATA_VERSION")
                    .help("Specify the output metadata version")
                    .long("metadata-version")
                    .value_name("NUM")
                    .possible_values(["1", "2"])
                    .default_value("2"),
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

impl<'a> Command<'a> for CacheRestoreCommand {
    fn name(&self) -> &'a str {
        "cache_restore"
    }

    fn run(&self, args: &mut dyn Iterator<Item = std::ffi::OsString>) -> std::io::Result<()> {
        let matches = self.cli().get_matches_from(args);

        let input_file = Path::new(matches.value_of("INPUT").unwrap());
        let output_file = Path::new(matches.value_of("OUTPUT").unwrap());

        let report = mk_report(matches.is_present("QUIET"));
        check_input_file(input_file, &report);
        check_output_file(output_file, &report);

        let opts = CacheRestoreOptions {
            input: input_file,
            output: output_file,
            metadata_version: matches.value_of_t_or_exit::<u8>("METADATA_VERSION"),
            async_io: matches.is_present("ASYNC_IO"),
            report: report.clone(),
        };

        restore(opts).map_err(|reason| {
            report.fatal(&format!("{}", reason));
            std::io::Error::from_raw_os_error(libc::EPERM)
        })
    }
}
