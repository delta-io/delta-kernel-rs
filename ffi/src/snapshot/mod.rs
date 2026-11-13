//! FFI interfaces for snapshot construction and metadata access.

#[cfg(feature = "declarative-plans")]
use crate::checkpoint::FfiSelectedCheckpointType;
#[cfg(feature = "declarative-plans")]
use crate::error::{ExternResult, IntoExternResult};
#[cfg(feature = "declarative-plans")]
use crate::{Handle, OptionalValue, SharedExternEngine, SharedSnapshot};

mod builder;
pub mod hint;

pub use builder::{
    ExclusiveSnapshotBuilder, FfiIncrementalReplay, FfiSnapshotBuilder, free_snapshot_builder,
    get_snapshot_builder, get_snapshot_builder_from, snapshot_builder_build,
    snapshot_builder_with_incremental_crc_replay, snapshot_builder_with_log_tail,
    snapshot_builder_with_max_catalog_version, snapshot_builder_with_version,
};

/// Returns the selected checkpoint's V1 or V2 specification, or absent if none was selected.
///
/// Returns without I/O when the type is known. Otherwise, resolves checkpoint information through
/// the engine's declarative executor and shares the result with checkpoint-info getters and scan
/// planning. Failed discovery can be retried.
///
/// # Safety
/// The snapshot and engine handles are borrowed and must remain valid for this call.
/// During discovery, the executor must not request checkpoint information recursively on the
/// same snapshot: that call would wait for initialization already in progress.
#[unsafe(no_mangle)]
#[cfg(feature = "declarative-plans")]
pub unsafe extern "C" fn snapshot_checkpoint_type(
    snapshot: Handle<SharedSnapshot>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<OptionalValue<FfiSelectedCheckpointType>> {
    let snapshot = unsafe { snapshot.as_ref() };
    let engine = unsafe { engine.as_ref() };
    let result = snapshot
        .reader()
        .get_checkpoint_type(engine.engine().as_ref())
        .map(|checkpoint_type| checkpoint_type.map(Into::into).into());
    unsafe { result.into_extern_result(&engine) }
}
