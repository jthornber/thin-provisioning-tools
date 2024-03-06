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
        Box::new(era_generate_metadata::EraGenerateMetadataCommand),
        Box::new(cache_generate_metadata::CacheGenerateMetadataCommand),
        Box::new(cache_generate_damage::CacheGenerateDamageCommand),
        Box::new(thin_explore::ThinExploreCommand),
        Box::new(thin_generate_metadata::ThinGenerateMetadataCommand),
        Box::new(thin_generate_damage::ThinGenerateDamageCommand),
        Box::new(thin_stat::ThinStatCommand),
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

    args.next_if(|path| get_basename(path) == Path::new("pdata_tools_dev"));
    let cmd = args.peek();

    if cmd.is_none() {
        usage(&commands);
        return exitcode::USAGE;
    };

    if let Some(c) = commands.iter().find(|c| cmd.unwrap() == c.name()) {
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
