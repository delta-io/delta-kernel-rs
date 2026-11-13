//! FFI interfaces for snapshot construction and retained state.

use crate::checkpoint::FfiSelectedCheckpointType;
use crate::{Handle, OptionalValue, SharedSnapshot};

mod builder;
pub mod hint;

pub use builder::{
    ExclusiveSnapshotBuilder, FfiIncrementalReplay, FfiSnapshotBuilder, free_snapshot_builder,
    get_snapshot_builder, get_snapshot_builder_from, snapshot_builder_build,
    snapshot_builder_with_incremental_crc_replay, snapshot_builder_with_log_tail,
    snapshot_builder_with_max_catalog_version, snapshot_builder_with_version,
};

/// Returns the selected checkpoint specification, when known, without engine I/O.
/// Returns absent if no checkpoint was selected, or `Unresolved` if its type was not retained
/// during construction. The observation remains fixed for the snapshot's lifetime.
///
/// # Safety
/// `snapshot` must be valid and remain owned by the caller throughout the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn snapshot_checkpoint_type(
    snapshot: Handle<SharedSnapshot>,
) -> OptionalValue<FfiSelectedCheckpointType> {
    unsafe { snapshot.as_ref() }
        .checkpoint_type()
        .map(Into::into)
        .into()
}
