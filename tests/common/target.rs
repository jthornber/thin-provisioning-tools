//------------------------------------------

#[macro_export]
macro_rules! path_to_cpp {
    ($name: literal) => {
        concat!("bin/", $name)
    };
}

#[macro_export]
macro_rules! path_to_rust {
    ($name: literal) => {
        env!(concat!("CARGO_BIN_EXE_", $name))
    };
}

#[cfg(not(feature = "rust_tests"))]
#[macro_export]
macro_rules! path_to {
    ($name: literal) => {
        path_to_cpp!($name)
    };
}

#[cfg(feature = "rust_tests")]
#[macro_export]
macro_rules! path_to {
    ($name: literal) => {
        path_to_rust!($name)
    };
}

//------------------------------------------

pub const CACHE_CHECK: &str = path_to!("cache_check");
pub const CACHE_DUMP: &str = path_to!("cache_dump");
pub const CACHE_RESTORE: &str = path_to!("cache_restore");

pub const THIN_CHECK: &str = path_to!("thin_check");
pub const THIN_DELTA: &str = path_to_cpp!("thin_delta"); // TODO: rust version
pub const THIN_DUMP: &str = path_to!("thin_dump");
pub const THIN_METADATA_PACK: &str = path_to_rust!("thin_metadata_pack"); // rust-only
pub const THIN_METADATA_UNPACK: &str = path_to_rust!("thin_metadata_unpack"); // rust-only
pub const THIN_REPAIR: &str = path_to!("thin_repair");
pub const THIN_RESTORE: &str = path_to!("thin_restore");
pub const THIN_RMAP: &str = path_to_cpp!("thin_rmap"); // TODO: rust version
pub const THIN_GENERATE_METADATA: &str = path_to_cpp!("thin_generate_metadata"); // cpp-only
pub const THIN_GENERATE_MAPPINGS: &str = path_to_cpp!("thin_generate_mappings"); // cpp-only
pub const THIN_GENERATE_DAMAGE: &str = path_to_cpp!("thin_generate_damage"); // cpp-only

//------------------------------------------

pub mod cpp_msg {
    pub const FILE_NOT_FOUND: &str = "No such file or directory";
    pub const MISSING_INPUT_ARG: &str = "No input file provided";
    pub const MISSING_OUTPUT_ARG: &str = "No output file provided";
    pub const BAD_SUPERBLOCK: &str = "bad checksum in superblock";

    pub fn bad_option_hint(option: &str) -> String {
        format!("unrecognized option '{}'", option)
    }
}

pub mod rust_msg {
    pub const FILE_NOT_FOUND: &str = "Couldn't find input file";
    pub const MISSING_INPUT_ARG: &str = "The following required arguments were not provided"; // TODO: be specific
    pub const MISSING_OUTPUT_ARG: &str = "The following required arguments were not provided"; // TODO: be specific
    pub const BAD_SUPERBLOCK: &str = "bad checksum in superblock";

    pub fn bad_option_hint(option: &str) -> String {
        format!("Found argument '{}' which wasn't expected", option)
    }
}

#[cfg(not(feature = "rust_tests"))]
pub use cpp_msg as msg;
#[cfg(feature = "rust_tests")]
pub use rust_msg as msg;

//------------------------------------------
