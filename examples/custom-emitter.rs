use anyhow::Result;
use clap::{value_parser, Arg};
use std::path::Path;

use thinp::commands::engine::*;
use thinp::commands::utils::*;
use thinp::thin::dump::*;
use thinp::thin::ir::{self, Map, MetadataVisitor, Visit};
use thinp::thin::metadata_repair::*;

//-----------------------------

// cargo run --release --example custom-emitter -- --dev-id 1 metadata.bin

// Make sure you use the --release, since debug builds are very slow in Rust.

struct CustomWriter {}

impl MetadataVisitor for CustomWriter {
    fn superblock_b(&mut self, sb: &ir::Superblock) -> Result<Visit> {
        println!("starting superblock");
        println!("superblock: uuid {}", sb.uuid);
        println!("time {}", sb.time);
        println!("transaction {}", sb.transaction);
        println!("flags {:?}", sb.flags);
        println!("version {:?}", sb.version);
        println!("data_block_size {}", sb.data_block_size);
        println!("nr_data_blocks {}", sb.nr_data_blocks);
        println!("metadata_snap {:?}", sb.metadata_snap);
        Ok(Visit::Continue)
    }

    fn superblock_e(&mut self) -> Result<Visit> {
        println!("ending superblock");
        Ok(Visit::Continue)
    }

    fn def_shared_b(&mut self, _name: &str) -> Result<Visit> {
        // There shouldn't be any of these when dumping a single device
        Ok(Visit::Continue)
    }

    fn def_shared_e(&mut self) -> Result<Visit> {
        Ok(Visit::Continue)
    }

    fn device_b(&mut self, d: &ir::Device) -> Result<Visit> {
        println!("starting device");
        println!("dev_id {}", d.dev_id);
        println!("mapped_blocks {}", d.mapped_blocks);
        println!("transaction {}", d.transaction);
        println!("creation_time {}", d.creation_time);
        println!("snap_time {}", d.snap_time);
        Ok(Visit::Continue)
    }

    fn device_e(&mut self) -> Result<Visit> {
        println!("ending device");
        Ok(Visit::Continue)
    }

    fn map(&mut self, m: &Map) -> Result<Visit> {
        println!(
            "map: thin_begin {}, data_begin {}, time {}, len {}",
            m.thin_begin, m.data_begin, m.time, m.len
        );
        Ok(Visit::Continue)
    }

    fn ref_shared(&mut self, _name: &str) -> Result<Visit> {
        // shouldn't happen
        Ok(Visit::Continue)
    }

    fn eof(&mut self) -> Result<Visit> {
        println!("eof");
        Ok(Visit::Continue)
    }
}

pub fn main() -> Result<()> {
    let matches = clap::Command::new("MyApp")
        .arg(
            Arg::new("DEV_ID")
                .long("dev-id")
                .help("Select the device to dump")
                .value_name("DEV_ID")
                .value_parser(value_parser!(u64))
                .required(true),
        )
        .arg(
            Arg::new("INPUT")
                .help("Specify the input device to dump")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::new("OUTPUT")
                .help("Specify the output file rather than stdout")
                .short('o')
                .long("output")
                .value_name("FILE"),
        )
        .get_matches();

    let input_file = Path::new(matches.get_one::<String>("INPUT").unwrap());
    let output_file = matches.get_one::<String>("OUTPUT").map(Path::new);

    let selected_devs: Option<Vec<u64>> = matches
        .get_many::<u64>("DEV_ID")
        .map(|devs| devs.copied().collect());

    let engine_opts = EngineOptions {
        tool: ToolType::Thin,
        engine_type: EngineType::Sync,
        use_metadata_snap: false,
    };

    let report = mk_report(false);

    let opts = ThinDumpOptions {
        input: input_file,
        output: output_file,
        engine_opts: engine_opts,
        report: report,
        repair: false,
        skip_mappings: false,
        overrides: SuperblockOverrides {
            transaction_id: None,
            data_block_size: None,
            nr_data_blocks: None,
        },
        selected_devs,
        format: OutputFormat::XML,
    };

    let out: Box<dyn MetadataVisitor> = Box::new(CustomWriter {});

    dump_with_formatter(opts, out)
}

//-----------------------------
