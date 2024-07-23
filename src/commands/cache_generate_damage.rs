use clap::{value_parser, Arg, ArgAction, ArgGroup};
use std::path::Path;
use std::process;

use crate::cache::damage_generator::*;
use crate::commands::engine::*;
use crate::commands::utils::*;
use crate::version::*;

//------------------------------------------
use crate::commands::Command;

pub struct CacheGenerateDamageCommand;

impl CacheGenerateDamageCommand {
    fn cli(&self) -> clap::Command {
        let cmd = clap::Command::new(self.name())
            .next_display_order(None)
            .version(crate::tools_version!())
            .disable_version_flag(true)
            .about("A tool for creating damages in cache metadata.")
            .arg(
                Arg::new("CREATE_METADATA_LEAKS")
                    .help("Create leaked metadata blocks")
                    .long("create-metadata-leaks")
                    .action(ArgAction::SetTrue)
                    .requires_all(["EXPECTED", "ACTUAL", "NR_BLOCKS"]),
            )
            // options
            .arg(
                Arg::new("EXPECTED")
                    .help("The expected reference count of damaged blocks")
                    .long("expected")
                    .value_name("REFCONT")
                    .value_parser(value_parser!(u32)),
            )
            .arg(
                Arg::new("ACTUAL")
                    .help("The actual reference count of damaged blocks")
                    .long("actual")
                    .value_name("REFCOUNT")
                    .value_parser(value_parser!(u32)),
            )
            .arg(
                Arg::new("NR_BLOCKS")
                    .help("Specify the number of metadata blocks")
                    .long("nr-blocks")
                    .value_name("NUM")
                    .value_parser(value_parser!(usize)),
            )
            .arg(
                Arg::new("OUTPUT")
                    .help("Specify the output device")
                    .short('o')
                    .long("output")
                    .value_name("FILE")
                    .required(true),
            )
            .group(
                ArgGroup::new("commands")
                    .args(["CREATE_METADATA_LEAKS"])
                    .required(true),
            );
        engine_args(version_args(cmd))
    }
}

impl<'a> Command<'a> for CacheGenerateDamageCommand {
    fn name(&self) -> &'a str {
        "cache_generate_damage"
    }

    fn run(&self, args: &mut dyn Iterator<Item = std::ffi::OsString>) -> exitcode::ExitCode {
        let matches = self.cli().get_matches_from(args);
        display_version(&matches);

        let report = mk_report(false);

        let engine_opts = parse_engine_opts(ToolType::Cache, &matches);
        if engine_opts.is_err() {
            return to_exit_code(&report, engine_opts);
        }

        let op = match matches.get_one::<clap::Id>("commands").unwrap().as_str() {
            "CREATE_METADATA_LEAKS" => DamageOp::CreateMetadataLeaks {
                nr_blocks: *matches.get_one::<usize>("NR_BLOCKS").unwrap(),
                expected_rc: *matches.get_one::<u32>("EXPECTED").unwrap(),
                actual_rc: *matches.get_one::<u32>("ACTUAL").unwrap(),
            },
            _ => {
                eprintln!("unknown option");
                process::exit(1);
            }
        };

        let opts = CacheDamageOpts {
            engine_opts: engine_opts.unwrap(),
            op,
            output: Path::new(matches.get_one::<String>("OUTPUT").unwrap()),
        };

        to_exit_code(&report, damage_metadata(opts))
    }
}

//------------------------------------------
