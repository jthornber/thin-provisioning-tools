pub mod base;
pub mod buffer;
pub mod buffer_pool;
pub mod gaps;
pub mod spindle;
pub mod stream_reader_common;
pub mod sync;
pub mod sync_stream_reader;
pub mod utils;

pub use crate::io_engine::base::*;
pub use crate::io_engine::spindle::SpindleIoEngine;
pub use crate::io_engine::sync::SyncIoEngine;
pub use crate::io_engine::sync_stream_reader::SyncStreamReader;

#[cfg(feature = "io_uring")]
pub mod async_;

#[cfg(feature = "io_uring")]
pub mod async_stream_reader;

#[cfg(feature = "io_uring")]
pub use crate::io_engine::async_::AsyncIoEngine;

#[cfg(test)]
pub mod core;

#[cfg(test)]
pub mod ramdisk;
