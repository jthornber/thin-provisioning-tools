use anyhow::{anyhow, ensure, Result};
use std::ffi::OsString;
use std::path::Path;
use std::process::exit;
use thinp::commands::*;

fn name_eq(name: &Path, cmd: &str) -> bool {
    name == Path::new(cmd)
}

fn main_() -> Result<()> {
    let mut args = std::env::args_os();
    ensure!(args.len() > 0);

    let mut os_name = args.next().unwrap();
    let mut name = Path::new(&os_name);
    name = Path::new(name.file_name().unwrap());

    if name == Path::new("pdata_tools") {
        os_name = args.next().unwrap();
        name = Path::new(&os_name);
    }

    let mut new_args = vec![OsString::from(&name)];
    for a in args.into_iter() {
        new_args.push(a);
    }

    if name_eq(name, "cache_check") {
        cache_check::run(&new_args);
    } else if name_eq(name, "cache_dump") {
        cache_dump::run(&new_args);
    } else if name_eq(name, "cache_metadata_size") {
        cache_metadata_size::run(&new_args);
    } else if name_eq(name, "cache_repair") {
        cache_repair::run(&new_args);
    } else if name_eq(name, "cache_restore") {
        cache_restore::run(&new_args);
    } else if name_eq(name, "era_check") {
        era_check::run(&new_args);
    } else if name_eq(name, "era_dump") {
        era_dump::run(&new_args);
    } else if name_eq(name, "era_restore") {
        era_restore::run(&new_args);
    } else if name_eq(name, "thin_check") {
        thin_check::run(&new_args);
    } else if name_eq(name, "thin_dump") {
        thin_dump::run(&new_args);
    } else if name_eq(name, "thin_metadata_pack") {
        thin_metadata_pack::run(&new_args);
    } else if name_eq(name, "thin_metadata_unpack") {
        thin_metadata_unpack::run(&new_args);
    } else if name_eq(name, "thin_repair") {
        thin_repair::run(&new_args);
    } else if name_eq(name, "thin_restore") {
        thin_restore::run(&new_args);
    } else if name_eq(name, "thin_shrink") {
        thin_shrink::run(&new_args);
    } else {
        return Err(anyhow!("unrecognised command"));
    }

    Ok(())
}

fn main() {
    let code = match main_() {
        Ok(()) => 0,
        Err(_) => {
            // We don't print out the error since -q may be set
            // eprintln!("{}", e);
            1
        }
    };

    exit(code)
}
