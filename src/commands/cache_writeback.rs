use atty::Stream;
use clap::Arg;
use std::path::Path;

use std::sync::Arc;

use crate::cache::writeback::{writeback, CacheWritebackOptions};
use crate::commands::engine::*;
use crate::commands::utils::*;
use crate::commands::Command;
use crate::report::*;

pub struct CacheWritebackCommand;

impl CacheWritebackCommand {
    fn cli<'a>(&self) -> clap::Command<'a> {
        let cmd = clap::Command::new(self.name())
            .color(clap::ColorChoice::Never)
            .version(crate::version::tools_version())
            .about("Repair binary cache metadata, and write it to a different device or file")
            .arg(
                Arg::new("QUIET")
                    .help("Suppress output messages, return only exit code.")
                    .short('q')
                    .long("quiet"),
            )
            .arg(
                Arg::new("NO_METADATA_UPDATE")
                    .help("Do not clear the dirty flags in metadata")
                    .long("no-metadata-update"),
            )
            .arg(
                Arg::new("LIST_FAILED_BLOCKS")
                    .help("List any blocks that failed the writeback process")
                    .long("list-failed-blocks"),
            )
            // options
            .arg(
                Arg::new("METADATA_DEV")
                    .help("Specify the cache metadata device")
                    .long("metadata-device")
                    .value_name("FILE")
                    .required(true),
            )
            .arg(
                Arg::new("ORIGIN_DEV")
                    .help("Specify the slow device begin cached")
                    .long("origin-device")
                    .value_name("FILE")
                    .required(true),
            )
            .arg(
                Arg::new("FAST_DEV")
                    .help(
                        "Specify the fast device containing the data that needs to be written back",
                    )
                    .long("fast-device")
                    .value_name("FILE")
                    .required(true),
            )
            .arg(
                Arg::new("ORIGIN_DEV_OFFSET")
                    .help("Specify the data offset within the slow device")
                    .long("origin-device-offset")
                    .value_name("SECTORS"),
            )
            .arg(
                Arg::new("FAST_DEV_OFFSET")
                    .help("Specify the data offset within the fast device")
                    .long("fast-device-offset")
                    .value_name("SECTORS"),
            )
            .arg(
                Arg::new("BUFFER_SIZE_MEG")
                    .help("Specify the size for the data cache, in megabytes")
                    .long("buffer-size-meg")
                    .value_name("MB"),
            );
        engine_args(cmd)
    }
}

impl<'a> Command<'a> for CacheWritebackCommand {
    fn name(&self) -> &'a str {
        "cache_writeback"
    }

    fn run(&self, args: &mut dyn Iterator<Item = std::ffi::OsString>) -> exitcode::ExitCode {
        let matches = self.cli().get_matches_from(args);

        let metadata_dev = Path::new(matches.value_of("METADATA_DEV").unwrap());
        let origin_dev = Path::new(matches.value_of("ORIGIN_DEV").unwrap());
        let fast_dev = Path::new(matches.value_of("FAST_DEV").unwrap());

        let report = if matches.is_present("QUIET") {
            std::sync::Arc::new(mk_quiet_report())
        } else if atty::is(Stream::Stdout) {
            std::sync::Arc::new(mk_progress_bar_report())
        } else {
            Arc::new(mk_simple_report())
        };

        check_input_file(metadata_dev, &report);
        check_input_file(origin_dev, &report);
        check_input_file(fast_dev, &report);

        let engine_opts = parse_engine_opts(ToolType::Cache, &matches);
        if engine_opts.is_err() {
            return to_exit_code(&report, engine_opts);
        }

        let opts = CacheWritebackOptions {
            metadata_dev,
            engine_opts: engine_opts.unwrap(),
            origin_dev,
            fast_dev,
            origin_dev_offset: optional_value_or_exit::<u64>(&matches, "ORIGIN_DEV_OFFSET"),
            fast_dev_offset: optional_value_or_exit::<u64>(&matches, "FAST_DEV_OFFSET"),
            buffer_size: optional_value_or_exit::<usize>(&matches, "BUFFER_SIZE_MEG")
                .map(|v| v * 2048),
            list_failed_blocks: matches.is_present("LIST_FAILED_BLOCKS"),
            update_metadata: !matches.is_present("NO_METADATA_UPDATE"),
            report: report.clone(),
        };

        to_exit_code(&report, writeback(opts))
    }
}
