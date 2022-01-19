use anyhow::{anyhow, Result};
use clap::{App, Arg};
use fixedbitset::FixedBitSet;
use rand::prelude::*;
use std::path::Path;
use std::process;
use std::sync::Arc;

use thinp::cache::ir;
use thinp::cache::ir::MetadataVisitor;
use thinp::cache::restore::Restorer;
use thinp::io_engine::{AsyncIoEngine, IoEngine, SyncIoEngine};
use thinp::pdata::space_map_metadata::core_metadata_sm;
use thinp::write_batcher::WriteBatcher;

//------------------------------------------

const MAX_CONCURRENT_IO: u32 = 1024;

//------------------------------------------

// TODO: introduce a new crate for development tools
struct CacheGenerator {
    block_size: u32,
    nr_cache_blocks: u32,
    nr_origin_blocks: u64,
    percent_resident: u8,
    percent_dirty: u8,
}

trait MetadataGenerator {
    fn generate_metadata(&self, v: &mut dyn MetadataVisitor) -> Result<()>;
}

impl MetadataGenerator for CacheGenerator {
    fn generate_metadata(&self, v: &mut dyn MetadataVisitor) -> Result<()> {
        if self.nr_origin_blocks > usize::MAX as u64 {
            return Err(anyhow!("number of origin blocks exceeds limits"));
        }
        let nr_origin_blocks = self.nr_origin_blocks as usize;

        let sb = ir::Superblock {
            uuid: String::new(),
            block_size: self.block_size,
            nr_cache_blocks: self.nr_cache_blocks,
            policy: String::from("smq"),
            hint_width: 4,
        };

        v.superblock_b(&sb)?;

        let nr_resident = std::cmp::min(
            (self.nr_cache_blocks as u64 * self.percent_resident as u64) / 100,
            self.nr_origin_blocks,
        ) as u32;
        // FIXME: slow & memory demanding
        let mut cblocks = (0..self.nr_cache_blocks).collect::<Vec<u32>>();
        cblocks.shuffle(&mut rand::thread_rng());
        cblocks.truncate(nr_resident as usize);
        cblocks.sort_unstable();

        v.mappings_b()?;
        {
            let mut used = FixedBitSet::with_capacity(nr_origin_blocks);
            let mut rng = rand::thread_rng();
            let mut dirty_rng = rand::thread_rng();
            for cblock in cblocks {
                // FIXME: getting slower as the collision rate raised
                let mut oblock = rng.gen_range(0..nr_origin_blocks);
                while used.contains(oblock) {
                    oblock = rng.gen_range(0..nr_origin_blocks);
                }

                used.set(oblock, true);
                v.mapping(&ir::Map {
                    cblock,
                    oblock: oblock as u64,
                    dirty: dirty_rng.gen_ratio(self.percent_dirty as u32, 100),
                })?;
            }
        }
        v.mappings_e()?;

        v.superblock_e()?;
        v.eof()?;

        Ok(())
    }
}

//------------------------------------------

fn format(engine: Arc<dyn IoEngine + Send + Sync>, gen: &CacheGenerator) -> Result<()> {
    let sm = core_metadata_sm(engine.get_nr_blocks(), u32::MAX);
    let batch_size = engine.get_batch_size();
    let mut w = WriteBatcher::new(engine, sm, batch_size);
    let mut restorer = Restorer::new(&mut w);

    gen.generate_metadata(&mut restorer)
}

fn set_needs_check(engine: Arc<dyn IoEngine + Send + Sync>) -> Result<()> {
    use thinp::cache::superblock::*;

    let mut sb = read_superblock(engine.as_ref(), SUPERBLOCK_LOCATION)?;
    sb.flags.needs_check = true;
    write_superblock(engine.as_ref(), SUPERBLOCK_LOCATION, &sb)
}

fn commit_new_transaction() -> Result<()> {
    // stub
    Ok(())
}

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

fn main() -> Result<()> {
    let parser = App::new("cache_generate_metadata")
        .color(clap::ColorChoice::Never)
        .version(thinp::version::tools_version())
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

    let matches = parser.get_matches();
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

    Ok(())
}

//------------------------------------------
