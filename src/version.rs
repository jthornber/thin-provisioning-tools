use clap::ArgMatches;
use std::io::Write;

//------------------------------------------

#[macro_export]
macro_rules! tools_version {
    () => {
        env!("CARGO_PKG_VERSION")
    };
}

pub fn version_args(cmd: clap::Command) -> clap::Command {
    use clap::Arg;

    cmd.arg(
        Arg::new("VERSION")
            .help("Print version")
            .short('V')
            .long("version")
            .exclusive(true)
            .action(clap::ArgAction::SetTrue),
    )
}

pub fn display_version(matches: &ArgMatches) {
    if matches.get_flag("VERSION") {
        let mut stdout = std::io::stdout();
        // ignore broken pipe errors
        let _ = stdout.write_all(tools_version!().as_bytes());
        let _ = stdout.write_all(b"\n");
        let _ = stdout.flush();

        std::process::exit(0);
    }
}

//------------------------------------------
