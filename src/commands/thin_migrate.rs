extern crate clap;

use anyhow::{anyhow, Result};
use clap::{Arg, ArgAction, ArgMatches};
use regex::Regex;
use std::path::{Path, PathBuf};

use crate::commands::engine::*;
use crate::commands::utils::*;
use crate::commands::Command;
use crate::report::{parse_log_level, verbose_args};
use crate::thin::migrate;
use crate::version::*;

//----------------------------------------------------------

fn parse_thin_source(input: &str) -> Result<migrate::ThinSource> {
    let re = Regex::new(r"^(.*):(\d+)(?:\.\.(\d+))?$").unwrap();
    let caps = re.captures(input);

    if caps.is_none() {
        return Err(anyhow!("badly formed source-thin"));
    }
    let caps = caps.unwrap();

    let pool = caps.get(1);
    let thin_id_1 = caps.get(2);
    let thin_id_2 = caps.get(3);

    if pool.is_none() {
        return Err(anyhow!("badly formed pool for source-thin"));
    }

    if thin_id_1.is_none() {
        return Err(anyhow!("badly formed thin id for source-thin"));
    }

    let pool = PathBuf::from(pool.unwrap().as_str().to_string());
    let thin_id_1 = thin_id_1.unwrap().as_str().to_string().parse()?;
    match thin_id_2 {
        None => Ok(migrate::ThinSource {
            pool,
            origin_thin_id: None,
            thin_id: thin_id_1,
        }),
        Some(thin_id_2) => {
            let thin_id_2 = thin_id_2.as_str().to_string().parse()?;
            Ok(migrate::ThinSource {
                pool,
                origin_thin_id: Some(thin_id_1),
                thin_id: thin_id_2,
            })
        }
    }
}

fn parse_thin_dest(input: &str) -> Result<migrate::ThinDest> {
    let re = Regex::new(r"^(.*):(\d+)$").unwrap();
    let caps = re.captures(input);

    if caps.is_none() {
        return Err(anyhow!("badly formed dest-thin"));
    }
    let caps = caps.unwrap();

    let pool = caps.get(1);
    let thin_id_1 = caps.get(2);

    if pool.is_none() {
        return Err(anyhow!("badly formed pool for source-thin"));
    }

    if thin_id_1.is_none() {
        return Err(anyhow!("badly formed thin id for source-thin"));
    }

    let pool = PathBuf::from(pool.unwrap().as_str().to_string());
    let thin_id_1 = thin_id_1.unwrap().as_str().to_string().parse()?;
    Ok(migrate::ThinDest {
        pool,
        thin_id: thin_id_1,
    })
}

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
                    .help("Specify the input device")
                    .long("source-dev")
                    .value_name("SOURCE-DEV"),
            )
            .arg(
                Arg::new("SOURCE-THIN")
                    .help("Specify the input thin device")
                    .long("source-thin")
                    .value_name("SOURCE-THIN"),
            )
            .arg(
                Arg::new("DEST-DEV")
                    .help("Specify the output device")
                    .long("dest-dev")
                    .value_name("DEST-DEV"),
            )
            .arg(
                Arg::new("DEST-FILE")
                    .help("Specify the output file")
                    .long("dest-file")
                    .value_name("DEST-FILE"),
            )
            .arg(
                Arg::new("DEST-THIN")
                    .help("Specify the output thin device")
                    .long("dest-thin")
                    .value_name("FILE"),
            )
            .arg(
                Arg::new("ZERO-DEST")
                    .help("Ensure all unwritten regions of the destination are zeroed")
                    .long("zero-dest")
                    .action(ArgAction::SetTrue),
            );
        // FIXME: add option to specify buffer size
        verbose_args(engine_args(version_args(cmd)))
    }
}

fn get_source(matches: &ArgMatches) -> Result<migrate::Source> {
    if let Some(arg) = matches.get_one::<String>("SOURCE-DEV") {
        let path = PathBuf::from(arg);
        Ok(migrate::Source::Dev(path))
    } else if let Some(arg) = matches.get_one::<String>("SOURCE-THIN") {
        let thin = parse_thin_source(arg)?;
        Ok(migrate::Source::Thin(thin))
    } else {
        Err(anyhow!("You must specify a source"))
    }
}

fn get_dest(matches: &ArgMatches) -> Result<migrate::Dest> {
    if let Some(arg) = matches.get_one::<String>("DEST-DEV") {
        let path = PathBuf::from(arg);
        Ok(migrate::Dest::Dev(path))
    } else if let Some(arg) = matches.get_one::<String>("DEST-FILE") {
        let path = PathBuf::from(arg);
        let file = migrate::FileDest { path, create: true };
        Ok(migrate::Dest::File(file))
    } else if let Some(arg) = matches.get_one::<String>("DEST-THIN") {
        let thin = parse_thin_dest(arg)?;
        Ok(migrate::Dest::Thin(thin))
    } else {
        Err(anyhow!("You must specify a dest"))
    }
}

impl<'a> Command<'a> for ThinMigrateCommand {
    fn name(&self) -> &'a str {
        "thin_migrate"
    }

    fn run(&self, args: &mut dyn Iterator<Item = std::ffi::OsString>) -> exitcode::ExitCode {
        let matches = self.cli().get_matches_from(args);
        display_version(&matches);

        let input_file = Path::new(matches.get_one::<String>("INPUT").unwrap());
        let output_file = Path::new(matches.get_one::<String>("OUTPUT").unwrap());

        let report = mk_report(matches.get_flag("QUIET"));
        let log_level = match parse_log_level(&matches) {
            Ok(level) => level,
            Err(e) => return to_exit_code::<()>(&report, Err(anyhow::Error::msg(e))),
        };
        report.set_level(log_level);

        if let Err(e) = check_input_file(input_file)
            .and_then(check_file_not_tiny)
            .and_then(|_| check_output_file(output_file))
        {
            return to_exit_code::<()>(&report, Err(e));
        }

        let engine_opts = parse_engine_opts(ToolType::Thin, &matches);
        if engine_opts.is_err() {
            return to_exit_code(&report, engine_opts);
        }

        let source = get_source(&matches);
        if source.is_err() {
            return to_exit_code(&report, source);
        }

        let dest = get_dest(&matches);
        if dest.is_err() {
            return to_exit_code(&report, dest);
        }

        let zero_dest = matches.get_flag("ZERO-DEST");

        let opts = migrate::ThinMigrateOptions {
            source: source.unwrap(),
            dest: dest.unwrap(),
            zero_dest,
            engine_opts: engine_opts.unwrap(),
            report: report.clone(),
        };

        to_exit_code(&report, migrate::migrate(opts))
    }
}

//----------------------------------------------------------

#[cfg(test)]
mod thin_source {
    use super::*;
    use anyhow::ensure;

    #[test]
    fn parse_single_thin_volume() -> Result<()> {
        let input = "mypool:123";
        let expected = migrate::ThinSource {
            pool: PathBuf::from("mypool".to_string()),
            origin_thin_id: None,
            thin_id: 123,
        };
        ensure!(parse_thin_source(input).unwrap() == expected);
        Ok(())
    }

    #[test]
    fn parse_thin_volume_with_delta() -> Result<()> {
        let input = "anotherpool:with:colon:100..123";
        let expected = migrate::ThinSource {
            pool: PathBuf::from("anotherpool:with:colon".to_string()),
            origin_thin_id: Some(100),
            thin_id: 123,
        };
        ensure!(parse_thin_source(input).unwrap() == expected);
        Ok(())
    }

    #[test]
    fn parse_bad() -> Result<()> {
        let inputs = ["invalidformat", "pool:", ":::", ""];

        for input in inputs {
            ensure!(parse_thin_source(input).is_err());
        }

        Ok(())
    }
}

#[cfg(test)]
mod thin_dest {
    use super::*;
    use anyhow::ensure;

    #[test]
    fn parse_thin_good() -> Result<()> {
        let input = "mypool:123";
        let expected = migrate::ThinDest {
            pool: PathBuf::from("mypool"),
            thin_id: 123,
        };
        ensure!(parse_thin_dest(input).unwrap() == expected);
        Ok(())
    }

    #[test]
    fn parse_bad() -> Result<()> {
        let inputs = vec!["mypool", "mypool:123..456", "one two three", "", "123..456"];

        for input in inputs {
            ensure!(parse_thin_dest(input).is_err());
        }

        Ok(())
    }
}

//----------------------------------------------------------
