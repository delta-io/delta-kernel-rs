//! FFI interfaces for snapshot construction and retained state.

mod builder;
pub mod hint;

pub use builder::{
    free_snapshot_builder, get_snapshot_builder, get_snapshot_builder_from, snapshot_builder_build,
    snapshot_builder_with_incremental_crc_replay, snapshot_builder_with_log_tail,
    snapshot_builder_with_max_catalog_version, snapshot_builder_with_version,
    ExclusiveSnapshotBuilder, FfiIncrementalReplay, FfiSnapshotBuilder,
};
