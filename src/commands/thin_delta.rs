extern crate clap;

use clap::{Arg, ArgGroup};
use std::path::Path;

use crate::commands::utils::*;
use crate::commands::Command;
use crate::thin::delta::*;
use crate::thin::delta_visitor::Snap;

//------------------------------------------

pub struct ThinDeltaCommand;

impl ThinDeltaCommand {
    fn cli<'a>(&self) -> clap::Command<'a> {
        clap::Command::new(self.name())
            .color(clap::ColorChoice::Never)
            .version(crate::version::tools_version())
            .about("Print the differences in the mappings between two thin devices")
            // flags
            .arg(
                Arg::new("ASYNC_IO")
                    .help("Force use of io_uring for synchronous io")
                    .long("async-io")
                    .hide(true),
            )
            .arg(
                Arg::new("METADATA_SNAP")
                    .help("Use metadata snapshot")
                    .short('m')
                    .long("metadata-snap"),
            )
            .arg(
                Arg::new("VERBOSE")
                    .help("Provide extra information on the mappings")
                    .long("verbose"),
            )
            // options
            .arg(
                Arg::new("ROOT1")
                    .help("The root block for the first thin volume to diff")
                    .long("root1")
                    .value_name("BLOCKNR")
                    .group("SNAP1"),
            )
            .arg(
                Arg::new("ROOT2")
                    .help("The root block for the second thin volume to diff")
                    .long("root2")
                    .value_name("BLOCKNR")
                    .group("SNAP2"),
            )
            .arg(
                Arg::new("THIN1")
                    .help("The numeric identifier for the first thin volume to diff")
                    .long("thin1")
                    .value_name("DEV_ID")
                    .visible_alias("snap1")
                    .group("SNAP1"),
            )
            .arg(
                Arg::new("THIN2")
                    .help("The numeric identifier for the second thin volume to diff")
                    .long("thin2")
                    .value_name("DEV_ID")
                    .visible_alias("snap2")
                    .group("SNAP2"),
            )
            // arguments
            .arg(
                Arg::new("INPUT")
                    .help("Specify the input device to dump")
                    .required(true)
                    .index(1),
            )
            // groups
            .group(ArgGroup::new("SNAP1").required(true))
            .group(ArgGroup::new("SNAP2").required(true))
    }
}

impl<'a> Command<'a> for ThinDeltaCommand {
    fn name(&self) -> &'a str {
        "thin_delta"
    }

    fn run(&self, args: &mut dyn Iterator<Item = std::ffi::OsString>) -> std::io::Result<()> {
        let matches = self.cli().get_matches_from(args);

        let input_file = Path::new(matches.value_of("INPUT").unwrap());

        let report = mk_report(false);
        check_input_file(input_file, &report);
        check_file_not_tiny(input_file, &report);

        let snap1 = if matches.is_present("THIN1") {
            Snap::DeviceId(matches.value_of_t_or_exit::<u64>("THIN1"))
        } else {
            Snap::RootBlock(matches.value_of_t_or_exit::<u64>("ROOT1"))
        };

        let snap2 = if matches.is_present("THIN2") {
            Snap::DeviceId(matches.value_of_t_or_exit::<u64>("THIN2"))
        } else {
            Snap::RootBlock(matches.value_of_t_or_exit::<u64>("ROOT2"))
        };

        let opts = ThinDeltaOptions {
            input: input_file,
            async_io: matches.is_present("ASYNC_IO"),
            report: report.clone(),
            snap1,
            snap2,
            verbose: matches.is_present("VERBOSE"),
            use_metadata_snap: matches.is_present("METADATA_SNAP"),
        };

        delta(opts).map_err(|reason| {
            report.fatal(&format!("{}", reason));
            std::io::Error::from_raw_os_error(libc::EPERM)
        })
    }
}

//------------------------------------------
