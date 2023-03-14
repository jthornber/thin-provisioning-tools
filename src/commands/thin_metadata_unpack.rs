extern crate clap;

use clap::Arg;
use std::path::Path;

use crate::commands::engine::*;
use crate::commands::utils::*;
use crate::commands::Command;
use crate::pack::toplevel::unpack;
use crate::report::mk_simple_report;

pub struct ThinMetadataUnpackCommand;

impl ThinMetadataUnpackCommand {
    fn cli<'a>(&self) -> clap::Command<'a> {
        let cmd = clap::Command::new(self.name())
            .color(clap::ColorChoice::Never)
            .version(crate::tools_version!())
            .about("Unpack a compressed file of thin metadata.")
            // flags
            .arg(
                Arg::new("FORCE")
                    .help("Force overwrite the output file")
                    .short('f')
                    .long("force"),
            )
            // options
            .arg(
                Arg::new("INPUT")
                    .help("Specify packed input file")
                    .required(true)
                    .short('i')
                    .value_name("FILE")
                    .takes_value(true),
            )
            .arg(
                Arg::new("OUTPUT")
                    .help("Specify thinp metadata binary device/file")
                    .required(true)
                    .short('o')
                    .value_name("DEV")
                    .takes_value(true),
            );
        engine_args(cmd)
    }
}

impl<'a> Command<'a> for ThinMetadataUnpackCommand {
    fn name(&self) -> &'a str {
        "thin_metadata_unpack"
    }

    fn run(&self, args: &mut dyn Iterator<Item = std::ffi::OsString>) -> exitcode::ExitCode {
        let matches = self.cli().get_matches_from(args);

        let input_file = Path::new(matches.value_of("INPUT").unwrap());
        let output_file = Path::new(matches.value_of("OUTPUT").unwrap());

        let report = mk_simple_report();

        if let Err(e) = check_input_file(input_file) {
            return to_exit_code::<()>(&report, Err(e));
        }

        if !matches.is_present("FORCE") {
            if let Err(e) = check_overwrite_metadata(&report, output_file) {
                return to_exit_code::<()>(&report, Err(e));
            }
        }

        let report = std::sync::Arc::new(report);
        to_exit_code(&report, unpack(input_file, output_file))
    }
}
