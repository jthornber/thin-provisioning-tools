extern crate clap;

use clap::Arg;
use std::path::Path;

use crate::commands::utils::*;
use crate::commands::Command;
use crate::report::*;

pub struct ThinMetadataPackCommand;

impl ThinMetadataPackCommand {
    fn cli<'a>(&self) -> clap::Command<'a> {
        clap::Command::new(self.name())
            .color(clap::ColorChoice::Never)
            .version(crate::tools_version!())
            .about("Produces a compressed file of thin metadata.  Only packs metadata blocks that are actually used.")
            // flags
            .arg(Arg::new("FORCE")
                .help("Force overwrite the output file")
                .short('f')
                .long("force"))
            // options
            .arg(Arg::new("INPUT")
                .help("Specify thinp metadata binary device/file")
                .required(true)
                .short('i')
                .value_name("DEV")
                .takes_value(true))
            .arg(Arg::new("OUTPUT")
                .help("Specify packed output file")
                .required(true)
                .short('o')
                .value_name("FILE")
                .takes_value(true))
    }
}

impl<'a> Command<'a> for ThinMetadataPackCommand {
    fn name(&self) -> &'a str {
        "thin_metadata_pack"
    }

    fn run(&self, args: &mut dyn Iterator<Item = std::ffi::OsString>) -> exitcode::ExitCode {
        let matches = self.cli().get_matches_from(args);

        let input_file = Path::new(matches.value_of("INPUT").unwrap());
        let output_file = Path::new(matches.value_of("OUTPUT").unwrap());

        let report = mk_simple_report();

        if let Err(e) = check_input_file(input_file)
            .and_then(check_file_not_tiny)
            .and_then(check_not_xml)
        {
            return to_exit_code::<()>(&report, Err(e));
        }

        if !matches.is_present("FORCE") {
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
