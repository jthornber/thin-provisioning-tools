use clap::builder::{PossibleValuesParser, TypedValueParser};
use clap::{value_parser, Arg, ArgAction, ArgGroup};

use std::path::Path;
use std::process;

use crate::cache::metadata_generator::*;
use crate::commands::engine::*;
use crate::commands::utils::*;
use crate::commands::Command;

//------------------------------------------

pub struct CacheGenerateMetadataCommand;

impl CacheGenerateMetadataCommand {
    fn cli(&self) -> clap::Command {
        let cmd = clap::Command::new(self.name())
            .next_display_order(None)
            .version(crate::tools_version!())
            .about("A tool for creating synthetic cache metadata.")
            // flags
            .arg(
                Arg::new("FORMAT")
                    .help("Format the metadata")
                    .long("format")
                    .action(ArgAction::SetTrue),
            )
            // options
            .arg(
                Arg::new("CACHE_BLOCK_SIZE")
                    .help("Specify the cache block size while formatting")
                    .long("cache-block-size")
                    .value_name("SECTORS")
                    .value_parser(value_parser!(u32))
                    .default_value("128"),
            )
            .arg(
                Arg::new("HOTSPOT_SIZE")
                    .help("Specify the average size of hotspots on the origin")
                    .long("hotspot-size")
                    .value_name("SIZE")
                    .value_parser(value_parser!(usize))
                    .default_value("1"),
            )
            .arg(
                Arg::new("NR_CACHE_BLOCKS")
                    .help("Specify the number of cache blocks")
                    .long("nr-cache-blocks")
                    .value_name("NUM")
                    .value_parser(value_parser!(u32))
                    .default_value("10240"),
            )
            .arg(
                Arg::new("NR_ORIGIN_BLOCKS")
                    .help("Specify the number of origin blocks")
                    .long("nr-origin-blocks")
                    .value_name("NUM")
                    .value_parser(value_parser!(u64))
                    .default_value("1048576"),
            )
            .arg(
                Arg::new("PERCENT_DIRTY")
                    .help("Specify the percentage of dirty blocks")
                    .long("percent-dirty")
                    .value_name("NUM")
                    .value_parser(value_parser!(u8))
                    .default_value("50"),
            )
            .arg(
                Arg::new("PERCENT_RESIDENT")
                    .help("Specify the percentage of valid blocks")
                    .long("percent-resident")
                    .value_name("NUM")
                    .value_parser(value_parser!(u8))
                    .default_value("80"),
            )
            .arg(
                Arg::new("METADATA_VERSION")
                    .help("Specify the output metadata version")
                    .long("metadata-version")
                    .value_name("NUM")
                    .value_parser(
                        PossibleValuesParser::new(["1", "2"]).map(|s| s.parse::<u8>().unwrap()),
                    )
                    .default_value("2"),
            )
            .arg(
                Arg::new("OUTPUT")
                    .help("Specify the output device")
                    .short('o')
                    .long("output")
                    .value_name("FILE")
                    .required(true),
            )
            .arg(
                Arg::new("SET_SUPERBLOCK_VERSION")
                    .help("Set the superblock version for debugging purpose")
                    .long("set-superblock-version")
                    .value_name("NUM")
                    .value_parser(value_parser!(u32)),
            )
            .arg(
                Arg::new("SET_NEEDS_CHECK")
                    .help("Set the 'needs_check' flag")
                    .long("set-needs-check")
                    .value_name("BOOL")
                    .value_parser(value_parser!(bool))
                    .num_args(0..=1)
                    .require_equals(true),
            )
            .arg(
                Arg::new("SET_CLEAN_SHUTDOWN")
                    .help("Set the 'clean_shutdown' flag")
                    .long("set-clean-shutdown")
                    .value_name("BOOL")
                    .value_parser(value_parser!(bool))
                    .num_args(0..=1)
                    .require_equals(true),
            )
            .group(
                ArgGroup::new("commands")
                    .args([
                        "FORMAT",
                        "SET_SUPERBLOCK_VERSION",
                        "SET_NEEDS_CHECK",
                        "SET_CLEAN_SHUTDOWN",
                    ])
                    .required(true),
            );
        engine_args(cmd)
    }
}

impl<'a> Command<'a> for CacheGenerateMetadataCommand {
    fn name(&self) -> &'a str {
        "cache_generate_metadata"
    }

    fn run(&self, args: &mut dyn Iterator<Item = std::ffi::OsString>) -> exitcode::ExitCode {
        let matches = self.cli().get_matches_from(args);

        let output_file = Path::new(matches.get_one::<String>("OUTPUT").unwrap());

        let report = mk_report(false);
        let engine_opts = parse_engine_opts(ToolType::Cache, &matches);
        if engine_opts.is_err() {
            return to_exit_code(&report, engine_opts);
        }

        let op = match matches.get_one::<clap::Id>("commands").unwrap().as_str() {
            "FORMAT" => MetadataOp::Format(CacheFormatOpts {
                block_size: *matches.get_one::<u32>("CACHE_BLOCK_SIZE").unwrap(),
                nr_cache_blocks: *matches.get_one::<u32>("NR_CACHE_BLOCKS").unwrap(),
                nr_origin_blocks: *matches.get_one::<u64>("NR_ORIGIN_BLOCKS").unwrap(),
                percent_resident: *matches.get_one::<u8>("PERCENT_RESIDENT").unwrap(),
                percent_dirty: *matches.get_one::<u8>("PERCENT_DIRTY").unwrap(),
                metadata_version: *matches.get_one::<u8>("METADATA_VERSION").unwrap(),
                hotspot_size: *matches.get_one::<usize>("HOTSPOT_SIZE").unwrap(),
            }),
            "SET_NEEDS_CHECK" => MetadataOp::SetNeedsCheck(
                *matches.get_one::<bool>("SET_NEEDS_CHECK").unwrap_or(&true),
            ),
            "SET_CLEAN_SHUTDOWN" => MetadataOp::SetCleanShutdown(
                *matches
                    .get_one::<bool>("SET_CLEAN_SHUTDOWN")
                    .unwrap_or(&true),
            ),
            "SET_SUPERBLOCK_VERSION" => MetadataOp::SetSuperblockVersion(
                *matches.get_one::<u32>("SET_SUPERBLOCK_VERSION").unwrap(),
            ),
            _ => {
                eprintln!("unknown option");
                process::exit(1);
            }
        };

        let opts = CacheGenerateOpts {
            op,
            engine_opts: engine_opts.unwrap(),
            output: output_file,
        };

        to_exit_code(&report, generate_metadata(opts))
    }
}

//------------------------------------------
