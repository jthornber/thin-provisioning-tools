extern crate clap;

use anyhow::{anyhow, Result};
use clap::{value_parser, Arg, ArgAction, ArgMatches};
use std::path::PathBuf;

use crate::commands::engine::*;
use crate::commands::utils::*;
use crate::commands::Command;
use crate::report::{parse_log_level, verbose_args};
use crate::thin::migrate;
use crate::version::*;

//----------------------------------------------------------

pub struct ThinMigrateCommand;
impl ThinMigrateCommand {
    fn cli(&self) -> clap::Command {
        let cmd = clap::Command::new(self.name())
            .next_display_order(None)
            .version(crate::tools_version!())
            .disable_version_flag(true)
            .about("Migrate a thin volume from one pool to another.")
            .arg(
                Arg::new("QUIET")
                    .help("Suppress output messages, return only exit code.")
                    .short('q')
                    .long("quiet")
                    .action(ArgAction::SetTrue),
            )
            // options
            .arg(
                Arg::new("SOURCE-DEV")
                    .help("Specify the input device or file")
                    .long("source-dev")
                    .value_name("DEVICE"),
            )
            .arg(
                Arg::new("DELTA-ID")
                    .help("Specify a thin id that will be the baseline for calculating deltas")
                    .long("delta-id")
                    .value_name("THIN_ID")
                    .hide(true),
            )
            .arg(
                Arg::new("DEST-DEV")
                    .help("Specify the output device")
                    .long("dest-dev")
                    .value_name("DEVICE"),
            )
            .arg(
                Arg::new("DEST-FILE")
                    .help("Specify the output file")
                    .long("dest-file")
                    .value_name("FILE"),
            )
            .arg(
                Arg::new("BUFFER-SIZE-MEG")
                    .help("Specify the size of the copy buffers, in megabytes")
                    .long("buffer-size-meg")
                    .value_name("MB")
                    .value_parser(value_parser!(usize)),
            )
            .arg(
                Arg::new("ZERO-DEST")
                    .help("Ensure all unwritten regions of the destination are zeroed")
                    .long("zero-dest")
                    .action(ArgAction::SetTrue)
                    .hide(true),
            );
        verbose_args(engine_args(version_args(cmd)))
    }
}

fn get_source(matches: &ArgMatches) -> Result<migrate::SourceArgs> {
    let path_str = matches.get_one::<String>("SOURCE-DEV");
    let delta_id = matches.get_one::<u32>("DELTA-ID").cloned();

    if path_str.is_none() {
        return Err(anyhow!("You must specify a source"));
    }

    let path = PathBuf::from(&path_str.unwrap());

    Ok(migrate::SourceArgs { path, delta_id })
}

fn get_dest(matches: &ArgMatches) -> Result<migrate::DestArgs> {
    if let Some(arg) = matches.get_one::<String>("DEST-DEV") {
        let path = PathBuf::from(arg);
        Ok(migrate::DestArgs::Dev(path))
    } else if let Some(arg) = matches.get_one::<String>("DEST-FILE") {
        let path = PathBuf::from(arg);
        Ok(migrate::DestArgs::File(path))
    } else {
        Err(anyhow!("You must specify a dest"))
    }
}

fn get_buffer_size_sectors(matches: &ArgMatches) -> Option<usize> {
    matches
        .get_one::<usize>("BUFFER-SIZE-MEG")
        .map(|v| *v * 2048)
}

impl<'a> Command<'a> for ThinMigrateCommand {
    fn name(&self) -> &'a str {
        "thin_migrate"
    }

    fn run(&self, args: &mut dyn Iterator<Item = std::ffi::OsString>) -> exitcode::ExitCode {
        let matches = self.cli().get_matches_from(args);
        display_version(&matches);

        let report = mk_report(matches.get_flag("QUIET"));
        let log_level = match parse_log_level(&matches) {
            Ok(level) => level,
            Err(e) => return to_exit_code::<()>(&report, Err(anyhow::Error::msg(e))),
        };
        report.set_level(log_level);

        let source = get_source(&matches);
        if source.is_err() {
            return to_exit_code(&report, source);
        }

        let dest = get_dest(&matches);
        if dest.is_err() {
            return to_exit_code(&report, dest);
        }

        let buffer_size = get_buffer_size_sectors(&matches);

        let zero_dest = matches.get_flag("ZERO-DEST");

        let opts = migrate::ThinMigrateOptions {
            source: source.unwrap(),
            dest: dest.unwrap(),
            zero_dest,
            buffer_size,
            report: report.clone(),
        };

        to_exit_code(&report, migrate::migrate(opts))
    }
}

//----------------------------------------------------------
