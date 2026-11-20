//! FFI interfaces for snapshot construction and metadata access.

mod builder;
#[cfg(feature = "declarative-plans")]
mod data_files_to_delete;
pub mod hint;

pub use builder::{
    ExclusiveSnapshotBuilder, FfiIncrementalReplay, FfiSnapshotBuilder, free_snapshot_builder,
    get_snapshot_builder, get_snapshot_builder_from, snapshot_builder_build,
    snapshot_builder_with_correlation_id, snapshot_builder_with_incremental_crc_replay,
    snapshot_builder_with_log_tail, snapshot_builder_with_max_catalog_version,
    snapshot_builder_with_version,
};
#[cfg(feature = "declarative-plans")]
pub use data_files_to_delete::{FfiDataFilesToDeleteMode, snapshot_data_files_to_delete_plan};
