use std::ffi::OsStr;
use std::io;
use std::path::Path;
use std::process::exit;

use thinp::commands::*;

fn get_basename(path: &OsStr) -> &Path {
    let p = Path::new(path);
    Path::new(p.file_name().unwrap())
}

fn register_commands<'a>() -> Vec<Box<dyn Command<'a>>> {
    vec![
        Box::new(cache_check::CacheCheckCommand),
        Box::new(cache_dump::CacheDumpCommand),
        Box::new(cache_metadata_size::CacheMetadataSizeCommand),
        Box::new(cache_repair::CacheRepairCommand),
        Box::new(cache_restore::CacheRestoreCommand),
        Box::new(cache_writeback::CacheWritebackCommand),
        Box::new(era_check::EraCheckCommand),
        Box::new(era_dump::EraDumpCommand),
        Box::new(era_invalidate::EraInvalidateCommand),
        Box::new(era_repair::EraRepairCommand),
        Box::new(era_restore::EraRestoreCommand),
        Box::new(thin_check::ThinCheckCommand),
        Box::new(thin_delta::ThinDeltaCommand),
        Box::new(thin_dump::ThinDumpCommand),
        Box::new(thin_ls::ThinLsCommand),
        Box::new(thin_metadata_pack::ThinMetadataPackCommand),
        Box::new(thin_metadata_size::ThinMetadataSizeCommand),
        Box::new(thin_metadata_unpack::ThinMetadataUnpackCommand),
        Box::new(thin_repair::ThinRepairCommand),
        Box::new(thin_restore::ThinRestoreCommand),
        Box::new(thin_rmap::ThinRmapCommand),
        Box::new(thin_shrink::ThinShrinkCommand),
        Box::new(thin_trim::ThinTrimCommand),
    ]
}

fn usage(commands: &[Box<dyn Command>]) {
    eprintln!("Usage: <command> <args>");
    eprintln!("commands:");
    commands.iter().for_each(|c| eprintln!("  {}", c.name()));
}

fn main_() -> io::Result<()> {
    let commands = register_commands();
    let mut args = std::env::args_os().peekable();

    args.next_if(|path| get_basename(path) == Path::new("pdata_tools"));
    let cmd = args.peek().ok_or_else(|| {
        usage(&commands);
        io::Error::from_raw_os_error(libc::EINVAL)
    })?;

    if let Some(i) = commands.iter().position(|c| cmd == c.name()) {
        commands[i].run(&mut args)
    } else {
        eprintln!("unrecognised command");
        usage(&commands);
        Err(io::Error::from_raw_os_error(libc::EINVAL))
    }
}

fn main() {
    let code = match main_() {
        Ok(()) => 0,
        Err(e) => {
            // We don't print out the error since -q may be set
            // eprintln!("{}", e);
            e.raw_os_error().unwrap_or(1)
        }
    };

    exit(code)
}
