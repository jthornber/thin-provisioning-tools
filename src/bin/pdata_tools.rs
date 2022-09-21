use std::ffi::OsStr;
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

fn main_() -> exitcode::ExitCode {
    let commands = register_commands();
    let mut args = std::env::args_os().peekable();

    args.next_if(|path| get_basename(path) == Path::new("pdata_tools"));
    let cmd = args.peek();

    if cmd.is_none() {
        usage(&commands);
        return exitcode::USAGE;
    };

    if let Some(c) = commands
        .iter()
        .find(|c| get_basename(cmd.unwrap()) == Path::new(c.name()))
    {
        c.run(&mut args)
    } else {
        eprintln!("unrecognised command");
        usage(&commands);
        exitcode::USAGE
    }
}

fn main() {
    exit(main_())
}
