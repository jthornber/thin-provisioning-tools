use clap::Arg;
use std::path::Path;

use crate::cache::check::{check, CacheCheckOptions};
use crate::cache::writeback::{writeback, CacheWritebackOptions};
use crate::commands::engine::*;
use crate::commands::utils::*;
use crate::commands::Command;
use crate::report::{parse_log_level, verbose_args};

pub struct CacheWritebackCommand;

impl CacheWritebackCommand {
    fn cli<'a>(&self) -> clap::Command<'a> {
        let cmd = clap::Command::new(self.name())
            .color(clap::ColorChoice::Never)
            .version(crate::tools_version!())
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
            )
            .arg(
                Arg::new("RETRY_COUNT")
                    .help("Specify how many times to retry data copying on failed data block")
                    .long("retry-count")
                    .value_name("CNT")
                    .default_value("0"),
            );
        verbose_args(engine_args(cmd))
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

        let report = mk_report(matches.is_present("QUIET"));
        let log_level = match parse_log_level(&matches) {
            Ok(level) => level,
            Err(e) => return to_exit_code::<()>(&report, Err(anyhow::Error::msg(e))),
        };
        report.set_level(log_level);

        if let Err(e) = check_input_file(metadata_dev)
            .and_then(|_| check_input_file(origin_dev))
            .and_then(|_| check_input_file(fast_dev))
        {
            return to_exit_code::<()>(&report, Err(e));
        }

        let engine_opts = match parse_engine_opts(ToolType::Cache, &matches) {
            Ok(opt) => opt,
            Err(_) => return exitcode::USAGE,
        };

        let check_opts = CacheCheckOptions {
            dev: metadata_dev,
            engine_opts: engine_opts.clone(),
            sb_only: false,
            skip_mappings: false,
            skip_hints: false,
            skip_discards: false,
            ignore_non_fatal: false,
            auto_repair: false,
            clear_needs_check: false,
            report: report.clone(),
        };

        if check(check_opts).is_err() {
            report.fatal(
                "metadata contains errors (run cache_check for details).\n\
                perhaps you need to run cache_repair.",
            );
            return exitcode::DATAERR;
        }

        let opts = CacheWritebackOptions {
            metadata_dev,
            engine_opts,
            origin_dev,
            fast_dev,
            origin_dev_offset: optional_value_or_exit::<u64>(&matches, "ORIGIN_DEV_OFFSET"),
            fast_dev_offset: optional_value_or_exit::<u64>(&matches, "FAST_DEV_OFFSET"),
            buffer_size: optional_value_or_exit::<usize>(&matches, "BUFFER_SIZE_MEG")
                .map(|v| v * 2048),
            list_failed_blocks: matches.is_present("LIST_FAILED_BLOCKS"),
            update_metadata: !matches.is_present("NO_METADATA_UPDATE"),
            retry_count: matches.value_of_t_or_exit::<u32>("RETRY_COUNT"),
            report: report.clone(),
        };

        to_exit_code(&report, writeback(opts))
    }
}
