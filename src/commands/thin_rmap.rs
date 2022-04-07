extern crate clap;

use anyhow::anyhow;
use clap::{Arg, Command};
use std::ops::Range;
use std::path::Path;
use std::process;
use std::str::FromStr;

use crate::commands::utils::*;
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

pub fn run(args: &[std::ffi::OsString]) {
    let parser = Command::new("thin_rmap")
        .color(clap::ColorChoice::Never)
        .version(crate::version::tools_version())
        .about("Output reverse map of a thin provisioned region of blocks")
        // flags
        .arg(
            Arg::new("ASYNC_IO")
                .help("Force use of io_uring for synchronous io")
                .long("async-io")
                .hide(true),
        )
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

    let matches = parser.get_matches_from(args);
    let input_file = Path::new(matches.value_of("INPUT").unwrap());

    let report = mk_report(false);
    check_input_file(input_file, &report);
    check_file_not_tiny(input_file, &report);

    // FIXME: get rid of the intermediate RangeU64 struct
    let regions: Vec<Range<u64>> = matches
        .values_of_t_or_exit::<RangeU64>("REGION")
        .iter()
        .map(|v| Range::<u64> {
            start: v.start,
            end: v.end,
        })
        .collect();

    let opts = ThinRmapOptions {
        input: input_file,
        async_io: matches.is_present("ASYNC_IO"),
        regions,
        report: report.clone(),
    };

    if let Err(reason) = rmap(opts) {
        report.fatal(&format!("{}", reason));
        process::exit(1);
    }
}

//------------------------------------------
