// This work is based on the implementation by Nikhil Kshirsagar which
// can be found here:
//    https://github.com/nkshirsagar/thinpool_shrink/blob/split_ranges/thin_shrink.py

extern crate clap;

use clap::{value_parser, Arg, ArgAction};
use std::ffi;
use std::io;
use std::path::Path;

use crate::commands::utils::*;
use crate::commands::Command;
use crate::report::*;
use crate::thin::shrink::{shrink, ThinShrinkOptions};

pub struct ThinShrinkCommand;

impl ThinShrinkCommand {
    fn cli(&self) -> clap::Command {
        clap::Command::new(self.name())
            .next_display_order(None)
            .version(crate::tools_version!())
            .about("Rewrite xml metadata and move data in an inactive pool.")
            .arg(
                Arg::new("INPUT")
                    .help("Specify thinp metadata xml file")
                    .required(true)
                    .short('i')
                    .long("input")
                    .value_name("FILE"),
            )
            .arg(
                Arg::new("OUTPUT")
                    .help("Specify output xml file")
                    .required(true)
                    .short('o')
                    .long("output")
                    .value_name("FILE"),
            )
            .arg(
                Arg::new("DATA")
                    .help("Specify pool data device where data will be moved")
                    .required(true)
                    .long("data")
                    .value_name("FILE"),
            )
            .arg(
                Arg::new("NOCOPY")
                    .help("Skip the copying of data, useful for benchmarking")
                    .long("no-copy")
                    .action(ArgAction::SetTrue),
            )
            .arg(
                Arg::new("NR_BLOCKS")
                    .help("Specify new size for the pool (in data blocks)")
                    .required(true)
                    .long("nr-blocks")
                    .value_name("NUM")
                    .value_parser(value_parser!(u64)),
            )
            .arg(
                Arg::new("BINARY")
                    .help("Perform binary metadata rebuild rather than XML rewrite")
                    .long("binary")
                    .action(ArgAction::SetTrue),
            )
    }

    fn parse_args<I, T>(&self, args: I) -> io::Result<ThinShrinkOptions>
    where
        I: IntoIterator<Item = T>,
        T: Into<ffi::OsString> + Clone,
    {
        let matches = self.cli().get_matches_from(args);

        let input = Path::new(matches.get_one::<String>("INPUT").unwrap());
        let output = Path::new(matches.get_one::<String>("OUTPUT").unwrap());
        let nr_blocks = *matches.get_one::<u64>("NR_BLOCKS").unwrap();
        let data_device = Path::new(matches.get_one::<String>("DATA").unwrap());
        let do_copy = !matches.get_flag("NOCOPY");
        let binary_mode = matches.get_flag("BINARY");
        let report = mk_report(false);

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

    fn run(&self, args: &mut dyn Iterator<Item = ffi::OsString>) -> exitcode::ExitCode {
        let opts = self.parse_args(args);
        if opts.is_err() {
            return exitcode::USAGE;
        }
        let opts = opts.unwrap();

        let report = std::sync::Arc::new(mk_simple_report());

        let mut r = check_input_file(&opts.input);

        if opts.binary_mode {
            r = r
                .and_then(check_file_not_tiny)
                .and_then(|_| check_output_file(&opts.output));
        }

        if opts.do_copy {
            // TODO: check file size
            r = r.and_then(|_| check_input_file(&opts.data_device));
        }

        if let Err(e) = r {
            return to_exit_code::<()>(&report, Err(e));
        }

        to_exit_code(&report, shrink(opts))
    }
}
