use clap::{value_parser, Arg, ArgGroup};

use std::path::Path;
use std::process;

use crate::cache::metadata_generator::*;
use crate::commands::engine::*;
use crate::commands::utils::*;
use crate::commands::Command;

//------------------------------------------

pub struct CacheGenerateMetadataCommand;

impl CacheGenerateMetadataCommand {
    fn cli<'a>(&self) -> clap::Command<'a> {
        let cmd = clap::Command::new(self.name())
            .color(clap::ColorChoice::Never)
            .version(crate::tools_version!())
            .about("A tool for creating synthetic cache metadata.")
            // flags
            .arg(
                Arg::new("FORMAT")
                    .help("Format the metadata")
                    .long("format")
                    .group("commands"),
            )
            // options
            .arg(
                Arg::new("CACHE_BLOCK_SIZE")
                    .help("Specify the cache block size while formatting")
                    .long("cache-block-size")
                    .value_name("SECTORS")
                    .default_value("128"),
            )
            .arg(
                Arg::new("HOTSPOT_SIZE")
                    .help("Specify the average size of hotspots on the origin")
                    .long("hotspot-size")
                    .value_name("SIZE"),
            )
            .arg(
                Arg::new("NR_CACHE_BLOCKS")
                    .help("Specify the number of cache blocks")
                    .long("nr-cache-blocks")
                    .value_name("NUM")
                    .default_value("10240"),
            )
            .arg(
                Arg::new("NR_ORIGIN_BLOCKS")
                    .help("Specify the number of origin blocks")
                    .long("nr-origin-blocks")
                    .value_name("NUM")
                    .default_value("1048576"),
            )
            .arg(
                Arg::new("PERCENT_DIRTY")
                    .help("Specify the percentage of dirty blocks")
                    .long("percent-dirty")
                    .value_name("NUM")
                    .default_value("50"),
            )
            .arg(
                Arg::new("PERCENT_RESIDENT")
                    .help("Specify the percentage of valid blocks")
                    .long("percent-resident")
                    .value_name("NUM")
                    .default_value("80"),
            )
            .arg(
                Arg::new("METADATA_VERSION")
                    .help("Specify the output metadata version")
                    .long("metadata-version")
                    .value_name("NUM")
                    .possible_values(["1", "2"])
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
                    .value_parser(value_parser!(u32))
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
            .arg(
                Arg::new("SET_CLEAN_SHUTDOWN")
                    .help("Set the 'clean_shutdown' flag")
                    .long("set-clean-shutdown")
                    .value_name("BOOL")
                    .value_parser(value_parser!(bool))
                    .hide_possible_values(true)
                    .min_values(0)
                    .max_values(1)
                    .require_equals(true)
                    .group("commands"),
            )
            .group(ArgGroup::new("commands").required(true));
        engine_args(cmd)
    }
}

impl<'a> Command<'a> for CacheGenerateMetadataCommand {
    fn name(&self) -> &'a str {
        "cache_generate_metadata"
    }

    fn run(&self, args: &mut dyn Iterator<Item = std::ffi::OsString>) -> exitcode::ExitCode {
        let matches = self.cli().get_matches_from(args);

        let output_file = Path::new(matches.value_of("OUTPUT").unwrap());

        let report = mk_report(false);
        let engine_opts = parse_engine_opts(ToolType::Cache, &matches);
        if engine_opts.is_err() {
            return to_exit_code(&report, engine_opts);
        }

        let hotspot_size = matches.value_of("HOTSPOT_SIZE").or(Some("1"));
        let hotspot_size = if let Ok(n) = hotspot_size.unwrap().parse::<usize>() {
            n
        } else {
            report.fatal("unable to parse hotspot size");
            return exitcode::USAGE;
        };

        let opts = CacheGenerateOpts {
            op: if matches.is_present("FORMAT") {
                MetadataOp::Format(CacheFormatOpts {
                    block_size: matches.value_of_t_or_exit::<u32>("CACHE_BLOCK_SIZE"),
                    nr_cache_blocks: matches.value_of_t_or_exit::<u32>("NR_CACHE_BLOCKS"),
                    nr_origin_blocks: matches.value_of_t_or_exit::<u64>("NR_ORIGIN_BLOCKS"),
                    percent_resident: matches.value_of_t_or_exit::<u8>("PERCENT_RESIDENT"),
                    percent_dirty: matches.value_of_t_or_exit::<u8>("PERCENT_DIRTY"),
                    metadata_version: matches.value_of_t_or_exit::<u8>("METADATA_VERSION"),
                    hotspot_size,
                })
            } else if matches.is_present("SET_NEEDS_CHECK") {
                MetadataOp::SetNeedsCheck(
                    *matches.get_one::<bool>("SET_NEEDS_CHECK").unwrap_or(&true),
                )
            } else if matches.is_present("SET_CLEAN_SHUTDOWN") {
                MetadataOp::SetCleanShutdown(
                    *matches
                        .get_one::<bool>("SET_CLEAN_SHUTDOWN")
                        .unwrap_or(&true),
                )
            } else if matches.is_present("SET_SUPERBLOCK_VERSION") {
                MetadataOp::SetSuperblockVersion(
                    *matches.get_one::<u32>("SET_SUPERBLOCK_VERSION").unwrap(),
                )
            } else {
                eprintln!("unknown option");
                process::exit(1);
            },
            engine_opts: engine_opts.unwrap(),
            output: output_file,
        };

        to_exit_code(&report, generate_metadata(opts))
    }
}

//------------------------------------------
