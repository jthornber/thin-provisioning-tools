extern crate clap;

use clap::{value_parser, Arg, ArgAction};
use std::path::Path;

use crate::commands::engine::*;
use crate::commands::utils::*;
use crate::commands::Command;
use crate::report::{parse_log_level, verbose_args};
use crate::thin::ls::*;
use crate::version::*;

pub struct ThinLsCommand;

impl ThinLsCommand {
    fn cli(&self) -> clap::Command {
        let cmd = clap::Command::new(self.name())
            .next_display_order(None)
            .version(crate::tools_version!())
            .disable_version_flag(true)
            .about("List thin volumes within a pool")
            .arg(
                Arg::new("NO_HEADERS")
                    .help("Don't output headers")
                    .long("no-headers")
                    .action(ArgAction::SetTrue),
            )
            .arg(
                Arg::new("METADATA_SNAPSHOT")
                    .help("Use metadata snapshot")
                    .short('m')
                    .long("metadata-snap")
                    .action(ArgAction::SetTrue),
            )
            // options
            .arg(
                Arg::new("FORMAT")
                    .help("Give a comma separated list of fields to be output")
                    .short('o')
                    .long("format")
                    .value_delimiter(',')
                    .value_name("FIELDS")
                    .value_parser(value_parser!(OutputField)),
            )
            // arguments
            .arg(
                Arg::new("INPUT")
                    .help("Specify the input device")
                    .required(true)
                    .index(1),
            );
        verbose_args(engine_args(version_args(cmd)))
    }
}

impl<'a> Command<'a> for ThinLsCommand {
    fn name(&self) -> &'a str {
        "thin_ls"
    }

    fn run(&self, args: &mut dyn Iterator<Item = std::ffi::OsString>) -> exitcode::ExitCode {
        use OutputField::*;

        let matches = self.cli().get_matches_from(args);
        display_version(&matches);

        let input_file = Path::new(matches.get_one::<String>("INPUT").unwrap());

        let report = mk_report(false);
        let log_level = match parse_log_level(&matches) {
            Ok(level) => level,
            Err(e) => return to_exit_code::<()>(&report, Err(anyhow::Error::msg(e))),
        };
        report.set_level(log_level);

        if let Err(e) = check_input_file(input_file).and_then(check_file_not_tiny) {
            return to_exit_code::<()>(&report, Err(e));
        }

        let fields = matches.get_many::<OutputField>("FORMAT").map_or_else(
            || vec![DeviceId, Mapped, CreationTime, SnapshottedTime],
            |fmt| fmt.cloned().collect(),
        );

        let engine_opts = parse_engine_opts(ToolType::Thin, &matches);
        if engine_opts.is_err() {
            return to_exit_code(&report, engine_opts);
        }

        let opts = ThinLsOptions {
            input: input_file,
            engine_opts: engine_opts.unwrap(),
            fields,
            no_headers: matches.get_flag("NO_HEADERS"),
            report: report.clone(),
        };

        to_exit_code(&report, ls(opts))
    }
}
