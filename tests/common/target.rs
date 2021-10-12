use std::ffi::OsString;
use std::path::PathBuf;

use crate::common::process::*;

//------------------------------------------

pub fn cpp_cmd<S, I>(cmd: S, args: I) -> Command
where
    S: Into<OsString>,
    I: IntoIterator,
    I::Item: Into<OsString>,
{
    let mut bin = PathBuf::from("bin");
    bin.push(Into::<OsString>::into(cmd));

    let mut args_ = Vec::new();
    for a in args {
        args_.push(Into::<OsString>::into(a));
    }

    Command::new(Into::<OsString>::into(bin.as_path()), args_)
}

pub fn rust_cmd<S, I>(cmd: S, args: I) -> Command
where
    S: Into<OsString>,
    I: IntoIterator,
    I::Item: Into<OsString>,
{
    const RUST_PATH: &str = env!(concat!("CARGO_BIN_EXE_", "pdata_tools"));

    let mut all_args = vec![Into::<OsString>::into(cmd)];
    for a in args {
        all_args.push(Into::<OsString>::into(a));
    }

    Command::new(Into::<OsString>::into(RUST_PATH), all_args)
}

pub fn thin_check_cmd<I>(args: I) -> Command
where
    I: IntoIterator,
    I::Item: Into<OsString>,
{
    rust_cmd("thin_check", args)
}

pub fn thin_rmap_cmd<I>(args: I) -> Command
where
    I: IntoIterator,
    I::Item: Into<OsString>,
{
    cpp_cmd("thin_rmap", args)
}

pub fn thin_generate_metadata_cmd<I>(args: I) -> Command
where
    I: IntoIterator,
    I::Item: Into<OsString>,
{
    cpp_cmd("thin_generate_metadata", args)
}

pub fn thin_generate_mappings_cmd<I>(args: I) -> Command
where
    I: IntoIterator,
    I::Item: Into<OsString>,
{
    cpp_cmd("thin_generate_mappings", args)
}

pub fn thin_generate_damage_cmd<I>(args: I) -> Command
where
    I: IntoIterator,
    I::Item: Into<OsString>,
{
    cpp_cmd("thin_generate_damage", args)
}

pub fn thin_restore_cmd<I>(args: I) -> Command
where
    I: IntoIterator,
    I::Item: Into<OsString>,
{
    // rust_cmd("thin_restore", args)
    cpp_cmd("thin_restore", args)
}

pub fn thin_repair_cmd<I>(args: I) -> Command
where
    I: IntoIterator,
    I::Item: Into<OsString>,
{
    rust_cmd("thin_restore", args)
}

pub fn thin_dump_cmd<I>(args: I) -> Command
where
    I: IntoIterator,
    I::Item: Into<OsString>,
{
    rust_cmd("thin_dump", args)
}

pub fn thin_delta_cmd<I>(args: I) -> Command
where
    I: IntoIterator,
    I::Item: Into<OsString>,
{
    cpp_cmd("thin_delta", args)
}

pub fn thin_metadata_pack_cmd<I>(args: I) -> Command
where
    I: IntoIterator,
    I::Item: Into<OsString>,
{
    rust_cmd("thin_metadata_pack", args)
}

pub fn thin_metadata_unpack_cmd<I>(args: I) -> Command
where
    I: IntoIterator,
    I::Item: Into<OsString>,
{
    rust_cmd("thin_metadata_unpack", args)
}

pub fn cache_check_cmd<I>(args: I) -> Command
where
    I: IntoIterator,
    I::Item: Into<OsString>,
{
    rust_cmd("cache_check", args)
}

pub fn cache_dump_cmd<I>(args: I) -> Command
where
    I: IntoIterator,
    I::Item: Into<OsString>,
{
    rust_cmd("cache_dump", args)
}

pub fn cache_metadata_size_cmd<I>(args: I) -> Command
where
    I: IntoIterator,
    I::Item: Into<OsString>,
{
    rust_cmd("cache_metadata_size", args)
}

pub fn cache_restore_cmd<I>(args: I) -> Command
where
    I: IntoIterator,
    I::Item: Into<OsString>,
{
    rust_cmd("cache_restore", args)
}

pub fn cache_repair_cmd<I>(args: I) -> Command
where
    I: IntoIterator,
    I::Item: Into<OsString>,
{
    rust_cmd("cache_repair", args)
}

pub fn era_check_cmd<I>(args: I) -> Command
where
    I: IntoIterator,
    I::Item: Into<OsString>,
{
    rust_cmd("era_check", args)
}

pub fn era_dump_cmd<I>(args: I) -> Command
where
    I: IntoIterator,
    I::Item: Into<OsString>,
{
    rust_cmd("era_dump", args)
}

pub fn era_restore_cmd<I>(args: I) -> Command
where
    I: IntoIterator,
    I::Item: Into<OsString>,
{
    rust_cmd("era_restore", args)
}

pub fn era_repair_cmd<I>(args: I) -> Command
where
    I: IntoIterator,
    I::Item: Into<OsString>,
{
    rust_cmd("era_repair", args)
}

//------------------------------------------

pub mod msg {
    pub const FILE_NOT_FOUND: &str = "Couldn't find input file";
    pub const MISSING_INPUT_ARG: &str = "The following required arguments were not provided"; // TODO: be specific
    pub const MISSING_OUTPUT_ARG: &str = "The following required arguments were not provided"; // TODO: be specific
    pub const BAD_SUPERBLOCK: &str = "bad checksum in superblock";

    pub fn bad_option_hint(option: &str) -> String {
        format!("Found argument '{}' which wasn't expected", option)
    }
}

//------------------------------------------
