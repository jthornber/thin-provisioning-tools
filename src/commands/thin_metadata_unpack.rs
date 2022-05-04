extern crate clap;

use clap::Arg;
use std::path::Path;

use crate::commands::utils::check_input_file;
use crate::commands::Command;
use crate::pack::toplevel::unpack;
use crate::report::mk_simple_report;

pub struct ThinMetadataUnpackCommand;

impl ThinMetadataUnpackCommand {
    fn cli<'a>(&self) -> clap::Command<'a> {
        clap::Command::new(self.name())
            .color(clap::ColorChoice::Never)
            .version(crate::version::tools_version())
            .about("Unpack a compressed file of thin metadata.")
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
            )
    }
}

impl<'a> Command<'a> for ThinMetadataUnpackCommand {
    fn name(&self) -> &'a str {
        "thin_metadata_unpack"
    }

    fn run(&self, args: &mut dyn Iterator<Item = std::ffi::OsString>) -> std::io::Result<()> {
        let matches = self.cli().get_matches_from(args);

        let input_file = Path::new(matches.value_of("INPUT").unwrap());
        let output_file = Path::new(matches.value_of("OUTPUT").unwrap());

        let report = mk_simple_report();
        check_input_file(input_file, &report);

        unpack(input_file, output_file).map_err(|reason| {
            report.fatal(&format!("Application error: {}", reason));
            std::io::Error::from_raw_os_error(libc::EPERM)
        })
    }
}
