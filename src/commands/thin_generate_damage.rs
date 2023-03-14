use clap::{Arg, ArgGroup};
use std::path::Path;
use std::process;

use crate::commands::engine::*;
use crate::commands::utils::*;
use crate::thin::damage_generator::*;

//------------------------------------------
use crate::commands::Command;

pub struct ThinGenerateDamageCommand;

impl ThinGenerateDamageCommand {
    fn cli<'a>(&self) -> clap::Command<'a> {
        let cmd = clap::Command::new(self.name())
            .color(clap::ColorChoice::Never)
            .version(crate::tools_version!())
            .about("A tool for creating synthetic thin metadata.")
            .arg(
                Arg::new("CREATE_METADATA_LEAKS")
                    .help("Create leaked metadata blocks")
                    .long("create-metadata-leaks")
                    .requires_all(&["EXPECTED", "ACTUAL", "NR_BLOCKS"])
                    .group("commands"),
            )
            // options
            .arg(
                Arg::new("EXPECTED")
                    .help("The expected reference count of damaged blocks")
                    .long("expected")
                    .value_name("REFCONT"),
            )
            .arg(
                Arg::new("ACTUAL")
                    .help("The actual reference count of damaged blocks")
                    .long("actual")
                    .value_name("REFCOUNT"),
            )
            .arg(
                Arg::new("NR_BLOCKS")
                    .help("Specify the number of metadata blocks")
                    .long("nr-blocks")
                    .value_name("NUM"),
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

impl<'a> Command<'a> for ThinGenerateDamageCommand {
    fn name(&self) -> &'a str {
        "thin_generate_damage"
    }

    fn run(&self, args: &mut dyn Iterator<Item = std::ffi::OsString>) -> exitcode::ExitCode {
        let matches = self.cli().get_matches_from(args);

        let report = mk_report(false);

        let engine_opts = parse_engine_opts(ToolType::Thin, &matches);
        if engine_opts.is_err() {
            return to_exit_code(&report, engine_opts);
        }

        let opts = ThinDamageOpts {
            engine_opts: engine_opts.unwrap(),
            op: if matches.is_present("CREATE_METADATA_LEAKS") {
                DamageOp::CreateMetadataLeaks {
                    nr_blocks: matches.value_of_t_or_exit::<usize>("NR_BLOCKS"),
                    expected_rc: matches.value_of_t_or_exit::<u32>("EXPECTED"),
                    actual_rc: matches.value_of_t_or_exit::<u32>("ACTUAL"),
                }
            } else {
                eprintln!("unknown option");
                process::exit(1);
            },
            output: Path::new(matches.value_of("OUTPUT").unwrap()),
        };

        to_exit_code(&report, damage_metadata(opts))
    }
}

//------------------------------------------
