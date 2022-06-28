extern crate clap;

use clap::Arg;
use std::path::Path;

use crate::commands::utils::*;
use crate::commands::Command;
use crate::thin::ls::*;

pub struct ThinLsCommand;

impl ThinLsCommand {
    fn cli<'a>(&self) -> clap::Command<'a> {
        clap::Command::new(self.name())
            .color(clap::ColorChoice::Never)
            .version(crate::version::tools_version())
            .about("List thin volumes within a pool")
            // flags
            .arg(
                Arg::new("ASYNC_IO")
                    .help("Force use of io_uring for synchronous io")
                    .long("async-io")
                    .hide(true),
            )
            .arg(
                Arg::new("NO_HEADERS")
                    .help("Don't output headers")
                    .long("no-headers"),
            )
            .arg(
                Arg::new("METADATA_SNAP")
                    .help("Use metadata snapshot")
                    .short('m')
                    .long("metadata-snap"),
            )
            // options
            .arg(
                Arg::new("FORMAT")
                    .help("Give a comma separated list of fields to be output")
                    .short('o')
                    .long("format")
                    .value_delimiter(',')
                    .value_name("FIELDS"),
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

impl<'a> Command<'a> for ThinLsCommand {
    fn name(&self) -> &'a str {
        "thin_ls"
    }

    fn run(&self, args: &mut dyn Iterator<Item = std::ffi::OsString>) -> exitcode::ExitCode {
        use OutputField::*;

        let matches = self.cli().get_matches_from(args);

        let input_file = Path::new(matches.value_of("INPUT").unwrap());

        let report = mk_report(false);
        check_input_file(input_file, &report);
        check_file_not_tiny(input_file, &report);

        let fields = if matches.is_present("FORMAT") {
            matches.values_of_t_or_exit::<OutputField>("FORMAT")
        } else {
            vec![DeviceId, Mapped, CreationTime, SnapshottedTime]
        };

        let opts = ThinLsOptions {
            input: input_file,
            async_io: matches.is_present("ASYNC_IO"),
            use_metadata_snap: matches.is_present("METADATA_SNAP"),
            fields,
            no_headers: matches.is_present("NO_HEADERS"),
            report: report.clone(),
        };

        to_exit_code(&report, ls(opts))
    }
}
