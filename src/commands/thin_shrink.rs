// This work is based on the implementation by Nikhil Kshirsagar which
// can be found here:
//    https://github.com/nkshirsagar/thinpool_shrink/blob/split_ranges/thin_shrink.py

extern crate clap;

use clap::Arg;
use std::ffi;
use std::io;
use std::path::Path;

use crate::commands::utils::*;
use crate::commands::Command;
use crate::shrink::toplevel::{shrink, ThinShrinkOptions};

pub struct ThinShrinkCommand;

impl ThinShrinkCommand {
    fn cli<'a>(&self) -> clap::Command<'a> {
        clap::Command::new(self.name())
            .color(clap::ColorChoice::Never)
            .version(crate::version::tools_version())
            .about("Rewrite xml metadata and move data in an inactive pool.")
            .arg(
                Arg::new("INPUT")
                    .help("Specify thinp metadata xml file")
                    .required(true)
                    .short('i')
                    .long("input")
                    .value_name("FILE")
                    .takes_value(true),
            )
            .arg(
                Arg::new("OUTPUT")
                    .help("Specify output xml file")
                    .required(true)
                    .short('o')
                    .long("output")
                    .value_name("FILE")
                    .takes_value(true),
            )
            .arg(
                Arg::new("DATA")
                    .help("Specify pool data device where data will be moved")
                    .required(true)
                    .long("data")
                    .value_name("FILE")
                    .takes_value(true),
            )
            .arg(
                Arg::new("NOCOPY")
                    .help("Skip the copying of data, useful for benchmarking")
                    .long("no-copy"),
            )
            .arg(
                Arg::new("NR_BLOCKS")
                    .help("Specify new size for the pool (in data blocks)")
                    .required(true)
                    .long("nr-blocks")
                    .value_name("NUM")
                    .takes_value(true),
            )
            .arg(
                Arg::new("BINARY")
                    .help("Perform binary metadata rebuild rather than XML rewrite")
                    .long("binary"),
            )
    }

    fn parse_args<I, T>(&self, args: I) -> io::Result<ThinShrinkOptions>
    where
        I: IntoIterator<Item = T>,
        T: Into<ffi::OsString> + Clone,
    {
        let matches = self.cli().get_matches_from(args);

        let input = Path::new(matches.value_of("INPUT").unwrap());
        let output = Path::new(matches.value_of("OUTPUT").unwrap());
        let nr_blocks = matches.value_of_t_or_exit::<u64>("NR_BLOCKS");
        let data_device = Path::new(matches.value_of("DATA").unwrap());
        let do_copy = !matches.is_present("NOCOPY");
        let binary_mode = matches.is_present("BINARY");
        let report = mk_report(false);

        check_input_file(input, &report);
        if binary_mode {
            check_file_not_tiny(input, &report);
            check_not_xml(input, &report);
            check_output_file(output, &report);
        }

        if do_copy {
            // TODO: check file size
            check_input_file(data_device, &report);
        }

        Ok(ThinShrinkOptions {
            input: input.to_path_buf(),
            output: output.to_path_buf(),
            nr_blocks,
            data_device: data_device.to_path_buf(),
            do_copy,
            binary_mode,
            report,
        })
    }
}

impl<'a> Command<'a> for ThinShrinkCommand {
    fn name(&self) -> &'a str {
        "thin_shrink"
    }

    fn run(&self, args: &mut dyn Iterator<Item = ffi::OsString>) -> io::Result<()> {
        let opts = self.parse_args(args)?;

        shrink(opts).map_err(|reason| {
            eprintln!("Application error: {}\n", reason);
            std::io::Error::from_raw_os_error(libc::EPERM)
        })
    }
}
