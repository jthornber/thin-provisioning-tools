extern crate clap;

use clap::{Arg, ArgAction};
use std::path::Path;

use crate::commands::utils::*;
use crate::commands::Command;
use crate::report::*;

pub struct ThinMetadataPackCommand;

impl ThinMetadataPackCommand {
    fn cli(&self) -> clap::Command {
        clap::Command::new(self.name())
            .next_display_order(None)
            .version(crate::tools_version!())
            .about("Produces a compressed file of thin metadata.  Only packs metadata blocks that are actually used.")
            // flags
            .arg(Arg::new("FORCE")
                .help("Force overwrite the output file")
                .short('f')
                .long("force")
                .action(ArgAction::SetTrue))
            // options
            .arg(Arg::new("INPUT")
                .help("Specify thinp metadata binary device/file")
                .required(true)
                .short('i')
                .long("input")
                .value_name("DEV"))
            .arg(Arg::new("OUTPUT")
                .help("Specify packed output file")
                .required(true)
                .short('o')
                .long("output")
                .value_name("FILE"))
    }
}

impl<'a> Command<'a> for ThinMetadataPackCommand {
    fn name(&self) -> &'a str {
        "thin_metadata_pack"
    }

    fn run(&self, args: &mut dyn Iterator<Item = std::ffi::OsString>) -> exitcode::ExitCode {
        let matches = self.cli().get_matches_from(args);

        let input_file = Path::new(matches.get_one::<String>("INPUT").unwrap());
        let output_file = Path::new(matches.get_one::<String>("OUTPUT").unwrap());

        let report = mk_simple_report();

        if let Err(e) = check_input_file(input_file)
            .and_then(check_file_not_tiny)
            .and_then(check_not_xml)
        {
            return to_exit_code::<()>(&report, Err(e));
        }

        if !matches.get_flag("FORCE") {
            if let Err(e) = check_overwrite_metadata(&report, output_file) {
                return to_exit_code::<()>(&report, Err(e));
            }
        }

        let report = std::sync::Arc::new(report);
        to_exit_code(
            &report,
            crate::pack::toplevel::pack(input_file, output_file),
        )
    }
}
