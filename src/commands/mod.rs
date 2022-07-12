pub mod cache_check;
pub mod cache_dump;
pub mod cache_metadata_size;
pub mod cache_repair;
pub mod cache_restore;
pub mod cache_writeback;
pub mod engine;
pub mod era_check;
pub mod era_dump;
pub mod era_invalidate;
pub mod era_repair;
pub mod era_restore;
pub mod thin_check;
pub mod thin_delta;
pub mod thin_dump;
pub mod thin_ls;
pub mod thin_metadata_pack;
pub mod thin_metadata_size;
pub mod thin_metadata_unpack;
pub mod thin_repair;
pub mod thin_restore;
pub mod thin_rmap;
pub mod thin_shrink;
pub mod thin_trim;
pub mod utils;

#[cfg(feature = "devtools")]
pub mod cache_generate_metadata;
#[cfg(feature = "devtools")]
pub mod thin_explore;
#[cfg(feature = "devtools")]
pub mod thin_generate_damage;
#[cfg(feature = "devtools")]
pub mod thin_generate_metadata;
#[cfg(feature = "devtools")]
pub mod thin_stat;

pub trait Command<'a> {
    fn name(&self) -> &'a str;
    fn run(&self, args: &mut dyn Iterator<Item = std::ffi::OsString>) -> exitcode::ExitCode;
}
