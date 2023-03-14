use clap::{value_parser, Arg, ArgGroup};
use std::path::Path;
use std::process;

use crate::commands::engine::*;
use crate::commands::utils::*;
use crate::thin::metadata_generator::*;

//------------------------------------------
use crate::commands::Command;

pub struct ThinGenerateMetadataCommand;

impl ThinGenerateMetadataCommand {
    fn cli<'a>(&self) -> clap::Command<'a> {
        let cmd = clap::Command::new(self.name())
            .color(clap::ColorChoice::Never)
            .version(crate::tools_version!())
            .about("A tool for creating synthetic thin metadata.")
            .arg(
                Arg::new("FORMAT")
                    .help("Format the metadata")
                    .long("format")
                    .group("commands"),
            )
            .arg(
                Arg::new("SET_NEEDS_CHECK")
                    .help("Set the 'needs_check' flag")
                    .long("set-needs-check")
                    .value_name("BOOL")
                    .value_parser(value_parser!(bool))
                    .hide_possible_values(true)
                    .min_values(0)
                    .max_values(1)
                    .require_equals(true)
                    .group("commands"),
            )
            // options
            .arg(
                Arg::new("DATA_BLOCK_SIZE")
                    .help("Specify the data block size while formatting")
                    .long("block-size")
                    .value_name("SECTORS")
                    .default_value("128"),
            )
            .arg(
                Arg::new("NR_DATA_BLOCKS")
                    .help("Specify the number of data blocks")
                    .long("nr-data-blocks")
                    .value_name("NUM")
                    .default_value("10240"),
            )
            .arg(
                Arg::new("OUTPUT")
                    .help("Specify the output device")
                    .short('o')
                    .long("output")
                    .value_name("FILE")
                    .required(true),
            )
            .group(ArgGroup::new("commands").required(true));
        engine_args(cmd)
    }
}

impl<'a> Command<'a> for ThinGenerateMetadataCommand {
    fn name(&self) -> &'a str {
        "thin_generate_metadata"
    }

    fn run(&self, args: &mut dyn Iterator<Item = std::ffi::OsString>) -> exitcode::ExitCode {
        let matches = self.cli().get_matches_from(args);

        let report = mk_report(false);

        let engine_opts = parse_engine_opts(ToolType::Thin, &matches);
        if engine_opts.is_err() {
            return to_exit_code(&report, engine_opts);
        }

        let opts = ThinGenerateOpts {
            engine_opts: engine_opts.unwrap(),
            op: if matches.is_present("FORMAT") {
                MetadataOp::Format(ThinFormatOpts {
                    data_block_size: matches.value_of_t_or_exit::<u32>("DATA_BLOCK_SIZE"),
                    nr_data_blocks: matches.value_of_t_or_exit::<u64>("NR_DATA_BLOCKS"),
                })
            } else if matches.is_present("SET_NEEDS_CHECK") {
                MetadataOp::SetNeedsCheck(
                    *matches.get_one::<bool>("SET_NEEDS_CHECK").unwrap_or(&true),
                )
            } else {
                eprintln!("unknown option");
                process::exit(1);
            },
            output: Path::new(matches.value_of("OUTPUT").unwrap()),
        };

        to_exit_code(&report, generate_metadata(opts))
    }
}

//------------------------------------------
