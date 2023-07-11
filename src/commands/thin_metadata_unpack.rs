extern crate clap;

use clap::{Arg, ArgAction};
use std::path::Path;

use crate::commands::engine::*;
use crate::commands::utils::*;
use crate::commands::Command;
use crate::pack::toplevel::unpack;
use crate::report::mk_simple_report;

pub struct ThinMetadataUnpackCommand;

impl ThinMetadataUnpackCommand {
    fn cli(&self) -> clap::Command {
        let cmd = clap::Command::new(self.name())
            .next_display_order(None)
            .version(crate::tools_version!())
            .about("Unpack a compressed file of thin metadata.")
            // flags
            .arg(
                Arg::new("FORCE")
                    .help("Force overwrite the output file")
                    .short('f')
                    .long("force")
                    .action(ArgAction::SetTrue),
            )
            // options
            .arg(
                Arg::new("INPUT")
                    .help("Specify packed input file")
                    .required(true)
                    .short('i')
                    .value_name("FILE"),
            )
            .arg(
                Arg::new("OUTPUT")
                    .help("Specify thinp metadata binary device/file")
                    .required(true)
                    .short('o')
                    .value_name("DEV"),
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

        let input_file = Path::new(matches.get_one::<String>("INPUT").unwrap());
        let output_file = Path::new(matches.get_one::<String>("OUTPUT").unwrap());

        let report = mk_simple_report();

        if let Err(e) = check_input_file(input_file) {
            return to_exit_code::<()>(&report, Err(e));
        }

        if !matches.get_flag("FORCE") {
            if let Err(e) = check_overwrite_metadata(&report, output_file) {
                return to_exit_code::<()>(&report, Err(e));
            }
        }

        let report = std::sync::Arc::new(report);
        to_exit_code(&report, unpack(input_file, output_file))
    }
}
