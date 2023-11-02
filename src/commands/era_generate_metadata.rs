use clap::{value_parser, Arg, ArgAction, ArgGroup};

use std::path::Path;
use std::process;

use crate::commands::engine::*;
use crate::commands::utils::*;
use crate::commands::Command;
use crate::era::metadata_generator::*;

//------------------------------------------

pub struct EraGenerateMetadataCommand;

impl EraGenerateMetadataCommand {
    fn cli(&self) -> clap::Command {
        let cmd = clap::Command::new(self.name())
            .next_display_order(None)
            .version(crate::tools_version!())
            .about("A tool for creating synthetic era metadata.")
            // flags
            .arg(
                Arg::new("FORMAT")
                    .help("Format the metadata")
                    .long("format")
                    .action(ArgAction::SetTrue),
            )
            // options
            .arg(
                Arg::new("BLOCK_SIZE")
                    .help("Specify the data block size while formatting")
                    .long("block-size")
                    .value_name("SECTORS")
                    .value_parser(value_parser!(u32))
                    .default_value("128"),
            )
            .arg(
                Arg::new("NR_BLOCKS")
                    .help("Specify the number of data blocks")
                    .long("nr-blocks")
                    .value_name("NUM")
                    .value_parser(value_parser!(u32))
                    .default_value("10240"),
            )
            .arg(
                Arg::new("CURRENT_ERA")
                    .help("Specify the current era")
                    .long("current-era")
                    .value_name("NUM")
                    .value_parser(value_parser!(u32))
                    .default_value("0"),
            )
            .arg(
                Arg::new("NR_WRITESETS")
                    .help("Specify the number of archived writesets of old eras")
                    .long("nr-writesets")
                    .value_name("NUM")
                    .value_parser(value_parser!(u32))
                    .default_value("0"),
            )
            .arg(
                Arg::new("OUTPUT")
                    .help("Specify the output device")
                    .short('o')
                    .long("output")
                    .value_name("FILE")
                    .required(true),
            )
            .group(ArgGroup::new("commands").args(["FORMAT"]).required(true));
        engine_args(cmd)
    }
}

impl<'a> Command<'a> for EraGenerateMetadataCommand {
    fn name(&self) -> &'a str {
        "era_generate_metadata"
    }

    fn run(&self, args: &mut dyn Iterator<Item = std::ffi::OsString>) -> exitcode::ExitCode {
        let matches = self.cli().get_matches_from(args);

        let output_file = Path::new(matches.get_one::<String>("OUTPUT").unwrap());

        let report = mk_report(false);
        let engine_opts = parse_engine_opts(ToolType::Era, &matches);
        if engine_opts.is_err() {
            return to_exit_code(&report, engine_opts);
        }

        let op = match matches.get_one::<clap::Id>("commands").unwrap().as_str() {
            "FORMAT" => MetadataOp::Format(EraFormatOpts {
                block_size: *matches.get_one::<u32>("BLOCK_SIZE").unwrap(),
                nr_blocks: *matches.get_one::<u32>("NR_BLOCKS").unwrap(),
                current_era: *matches.get_one::<u32>("CURRENT_ERA").unwrap(),
                nr_writesets: *matches.get_one::<u32>("NR_WRITESETS").unwrap(),
            }),
            _ => {
                eprintln!("unknown option");
                process::exit(1);
            }
        };

        let opts = EraGenerateOpts {
            op,
            engine_opts: engine_opts.unwrap(),
            output: output_file,
        };

        to_exit_code(&report, generate_metadata(opts))
    }
}

//------------------------------------------
