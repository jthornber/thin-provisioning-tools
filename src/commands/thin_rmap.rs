extern crate clap;

use anyhow::anyhow;
use clap::Arg;
use std::ops::Range;
use std::path::Path;

use std::str::FromStr;

use crate::commands::engine::*;
use crate::commands::utils::*;
use crate::commands::Command;
use crate::thin::rmap::*;

//------------------------------------------

struct RangeU64 {
    start: u64,
    end: u64,
}

impl FromStr for RangeU64 {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut iter = s.split("..");
        let start = iter.next().ok_or_else(|| anyhow!("badly formed region"))?;
        let end = iter.next().ok_or_else(|| anyhow!("badly formed region"))?;
        if iter.next().is_some() {
            return Err(anyhow!("badly formed region"));
        }
        Ok(RangeU64 {
            start: start.parse::<u64>()?,
            end: end.parse::<u64>()?,
        })
    }
}

//------------------------------------------

pub struct ThinRmapCommand;

impl ThinRmapCommand {
    fn cli<'a>(&self) -> clap::Command<'a> {
        let cmd = clap::Command::new(self.name())
            .color(clap::ColorChoice::Never)
            .version(crate::tools_version!())
            .about("Output reverse map of a thin provisioned region of blocks")
            // options
            .arg(
                // FIXME: clap doesn't support placing the index argument
                //        after the multiple arguments, e.g.,
                //        thin_rmap --region 0..1 --region 2..3 tmeta.bin
                Arg::new("REGION")
                    .help("Specify range of blocks on the data device")
                    .long("region")
                    .multiple_occurrences(true)
                    .required(true)
                    .value_name("BLOCK_RANGE"),
            )
            // arguments
            .arg(
                Arg::new("INPUT")
                    .help("Specify the input device")
                    .required(true)
                    .index(1),
            );
        engine_args(cmd)
    }
}

impl<'a> Command<'a> for ThinRmapCommand {
    fn name(&self) -> &'a str {
        "thin_rmap"
    }

    fn run(&self, args: &mut dyn Iterator<Item = std::ffi::OsString>) -> exitcode::ExitCode {
        let matches = self.cli().get_matches_from(args);
        let input_file = Path::new(matches.value_of("INPUT").unwrap());

        let report = mk_report(false);

        if let Err(e) = check_input_file(input_file).and_then(check_file_not_tiny) {
            return to_exit_code::<()>(&report, Err(e));
        }

        // FIXME: get rid of the intermediate RangeU64 struct
        let regions: Vec<Range<u64>> = matches
            .values_of_t_or_exit::<RangeU64>("REGION")
            .iter()
            .map(|v| Range::<u64> {
                start: v.start,
                end: v.end,
            })
            .collect();

        let engine_opts = parse_engine_opts(ToolType::Thin, &matches);
        if engine_opts.is_err() {
            return to_exit_code(&report, engine_opts);
        }

        let opts = ThinRmapOptions {
            input: input_file,
            engine_opts: engine_opts.unwrap(),
            regions,
            report: report.clone(),
        };

        to_exit_code(&report, rmap(opts))
    }
}

//------------------------------------------
