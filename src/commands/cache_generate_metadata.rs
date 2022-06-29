use anyhow::Result;
use clap::{Arg, ArgGroup};

use std::path::Path;
use std::process;

use crate::cache::metadata_generator::*;
use crate::commands::engine::*;
use crate::commands::utils::*;
use crate::commands::Command;

//------------------------------------------

pub enum MetadataOp {
    Format,
    SetNeedsCheck,
}

struct CacheGenerateOpts<'a> {
    op: MetadataOp,
    block_size: u32,
    nr_cache_blocks: u32,
    nr_origin_blocks: u64,
    percent_resident: u8,
    percent_dirty: u8,
    engine_opts: EngineOptions,
    output: &'a Path,
    metadata_version: u8,
}

fn generate_metadata(opts: &CacheGenerateOpts) -> Result<()> {
    let engine = EngineBuilder::new(opts.output, &opts.engine_opts)
        .write(true)
        .build()?;

    match opts.op {
        MetadataOp::Format => {
            let cache_gen = CacheGenerator {
                block_size: opts.block_size,
                nr_cache_blocks: opts.nr_cache_blocks,
                nr_origin_blocks: opts.nr_origin_blocks,
                percent_resident: opts.percent_resident,
                percent_dirty: opts.percent_dirty,
                metadata_version: opts.metadata_version,
            };
            format(engine, &cache_gen)?;
        }
        MetadataOp::SetNeedsCheck => {
            set_needs_check(engine)?;
        }
    }

    Ok(())
}

//------------------------------------------

pub struct CacheGenerateMetadataCommand;

impl CacheGenerateMetadataCommand {
    fn cli<'a>(&self) -> clap::Command<'a> {
        let cmd = clap::Command::new(self.name())
            .color(clap::ColorChoice::Never)
            .version(crate::version::tools_version())
            .about("A tool for creating synthetic cache metadata.")
            .arg(
                Arg::new("FORMAT")
                    .help("Format the metadata")
                    .long("format")
                    .group("commands"),
            )
            .arg(
                Arg::new("SET_NEEDS_CHECK")
                    .help("Set the NEEDS_CHECK flag")
                    .long("set-needs-check")
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
                    .help("Specify the outiput metadata version")
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

        let report = mk_report(matches.is_present("QUIET"));
        let engine_opts = parse_engine_opts(ToolType::Cache, &matches);
        if engine_opts.is_err() {
            return to_exit_code(&report, engine_opts);
        }

        let opts = CacheGenerateOpts {
            op: if matches.is_present("FORMAT") {
                MetadataOp::Format
            } else if matches.is_present("SET_NEEDS_CHECK") {
                MetadataOp::SetNeedsCheck
            } else {
                eprintln!("unknown option");
                process::exit(1);
            },
            block_size: matches.value_of_t_or_exit::<u32>("CACHE_BLOCK_SIZE"),
            nr_cache_blocks: matches.value_of_t_or_exit::<u32>("NR_CACHE_BLOCKS"),
            nr_origin_blocks: matches.value_of_t_or_exit::<u64>("NR_ORIGIN_BLOCKS"),
            percent_resident: matches.value_of_t_or_exit::<u8>("PERCENT_RESIDENT"),
            percent_dirty: matches.value_of_t_or_exit::<u8>("PERCENT_DIRTY"),
            engine_opts: engine_opts.unwrap(),
            output: output_file,
            metadata_version: matches.value_of_t_or_exit::<u8>("METADATA_VERSION"),
        };

        to_exit_code(&report, generate_metadata(&opts))
    }
}

//------------------------------------------
