use anyhow::Result;
use clap::{App, Arg};
use std::path::Path;
use std::process;
use std::sync::Arc;

use crate::cache::metadata_generator::*;
use crate::io_engine::{AsyncIoEngine, IoEngine, SyncIoEngine};

//------------------------------------------

const MAX_CONCURRENT_IO: u32 = 1024;

//------------------------------------------

struct CacheGenerateOpts<'a> {
    block_size: u32,
    nr_cache_blocks: u32,
    nr_origin_blocks: u64,
    percent_resident: u8,
    percent_dirty: u8,
    async_io: bool,
    format: bool,
    set_needs_check: bool,
    output: &'a Path,
}

fn generate_metadata(opts: &CacheGenerateOpts) -> Result<()> {
    let engine: Arc<dyn IoEngine + Send + Sync> = if opts.async_io {
        Arc::new(AsyncIoEngine::new(opts.output, MAX_CONCURRENT_IO, true)?)
    } else {
        let nr_threads = std::cmp::max(8, num_cpus::get() * 2);
        Arc::new(SyncIoEngine::new(opts.output, nr_threads, true)?)
    };

    if opts.format {
        let cache_gen = CacheGenerator {
            block_size: opts.block_size,
            nr_cache_blocks: opts.nr_cache_blocks,
            nr_origin_blocks: opts.nr_origin_blocks,
            percent_resident: opts.percent_resident,
            percent_dirty: opts.percent_dirty,
        };
        format(engine, &cache_gen)?;
    } else if opts.set_needs_check {
        set_needs_check(engine)?;
    } else {
        commit_new_transaction()?;
    }

    Ok(())
}

//------------------------------------------

pub fn run(args: &[std::ffi::OsString]) {
    let parser = App::new("cache_generate_metadata")
        .color(clap::ColorChoice::Never)
        .version(crate::version::tools_version())
        .about("A tool for creating synthetic cache metadata.")
        // flags
        .arg(
            Arg::new("ASYNC_IO")
                .help("Force use of io_uring for synchronous io")
                .long("async-io")
                .hide(true),
        )
        .arg(
            Arg::new("FORMAT")
                .help("Format the metadata")
                .long("format"),
        )
        .arg(
            Arg::new("SET_NEEDS_CHECK")
                .help("Set the NEEDS_CHECK flag")
                .long("set-needs-check"),
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
            Arg::new("OUTPUT")
                .help("Specify the output device")
                .short('o')
                .long("output")
                .value_name("FILE")
                .required(true),
        );

    let matches = parser.get_matches_from(args);
    let output_file = Path::new(matches.value_of("OUTPUT").unwrap());

    let opts = CacheGenerateOpts {
        block_size: matches.value_of_t_or_exit::<u32>("CACHE_BLOCK_SIZE"),
        nr_cache_blocks: matches.value_of_t_or_exit::<u32>("NR_CACHE_BLOCKS"),
        nr_origin_blocks: matches.value_of_t_or_exit::<u64>("NR_ORIGIN_BLOCKS"),
        percent_resident: matches.value_of_t_or_exit::<u8>("PERCENT_RESIDENT"),
        percent_dirty: matches.value_of_t_or_exit::<u8>("PERCENT_DIRTY"),
        async_io: matches.is_present("ASYNC_IO"),
        format: matches.is_present("FORMAT"),
        set_needs_check: matches.is_present("SET_NEEDS_CHECK"),
        output: output_file,
    };

    if let Err(reason) = generate_metadata(&opts) {
        eprintln!("{}", reason);
        process::exit(1)
    }
}

//------------------------------------------
