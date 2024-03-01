// suppress all the false alarms by cargo test
// https://github.com/rust-lang/rust/issues/46379
#![allow(dead_code)]

pub mod cache;
pub mod cache_xml_generator;
pub mod common_args;
pub mod era;
pub mod era_xml_generator;
pub mod fixture;
pub mod input_arg;
pub mod output_option;
pub mod piping;
pub mod process;
pub mod program;
pub mod target;
pub mod test_dir;
pub mod thin;
pub mod thin_xml_generator;
