use anyhow::{anyhow, ensure, Result};
use std::ffi::OsString;
use std::path::Path;
use std::process::exit;
use thinp_dev::commands::*;

fn main_() -> Result<()> {
    let mut args = std::env::args_os();
    ensure!(args.len() > 0);

    let mut os_name = args.next().unwrap();
    let mut name = Path::new(&os_name);
    name = Path::new(name.file_name().unwrap());

    if name == Path::new("pdata_tools_dev") {
        os_name = args.next().unwrap();
        name = Path::new(&os_name);
    }

    let mut new_args = vec![OsString::from(&name)];
    for a in args {
        new_args.push(a);
    }

    match name.as_os_str().to_str() {
        Some("cache_generate_metadata") => cache_generate_metadata::run(&new_args),
        Some("thin_explore") => thin_explore::run(&new_args),
        _ => return Err(anyhow!("unrecognised command")),
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
