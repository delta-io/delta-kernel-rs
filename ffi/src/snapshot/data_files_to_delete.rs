//! FFI entry point for Snapshot data-file cleanup plans.

use std::time::Duration;

use delta_kernel::snapshot::{DataFilesToDeleteMode, DataFilesToDeleteOptions, SnapshotRef};
use delta_kernel::{Engine, KernelError, Operation, Version};

use crate::error::{ExternResult, IntoExternResult};
use crate::handle::Handle;
use crate::{KernelOwnedBytes, OptionalValue, SharedExternEngine, SharedSnapshot};

/// Candidate source for [`snapshot_data_files_to_delete_plan`].
///
/// `Lite` uses `anchor_version` to choose the range; `Full` requires that argument to be `None`.
///
/// cbindgen:prefix-with-name=true
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FfiDataFilesToDeleteMode {
    /// Discover old files by listing the table root.
    Full,
    /// Discover candidates by scanning log actions after `anchor_version`.
    Lite,
}

/// Build a Snapshot data-file cleanup plan and return proto-serialized QueryPlan bytes.
///
/// The returned paths are absolute URLs. Kernel does not delete them; the caller must apply
/// connector-specific protection and path filters before deletion. When no plan exists, returns
/// [`OptionalValue::None`]. Free returned bytes with [`crate::free_kernel_bytes`].
///
/// # Safety
///
/// `snapshot` and `engine` must be valid handles. `mode` and both `OptionalValue` tags must be
/// valid C enum variants. The function borrows both handles but clones the underlying snapshot
/// reference for the duration of plan construction.
#[cfg(feature = "declarative-plans")]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn snapshot_data_files_to_delete_plan(
    snapshot: Handle<SharedSnapshot>,
    engine: Handle<SharedExternEngine>,
    mode: FfiDataFilesToDeleteMode,
    now_ms: i64,
    deleted_file_retention_ms: OptionalValue<u64>,
    anchor_version: OptionalValue<u64>,
) -> ExternResult<OptionalValue<KernelOwnedBytes>> {
    let snapshot = unsafe { snapshot.clone_as_arc() };
    let extern_engine = unsafe { engine.as_ref() };
    let inner_engine = extern_engine.engine();
    let result = snapshot_data_files_to_delete_plan_impl(
        &snapshot,
        inner_engine.as_ref(),
        mode,
        now_ms,
        deleted_file_retention_ms,
        anchor_version,
    );
    unsafe { result.into_extern_result(&extern_engine) }
}

#[cfg(feature = "declarative-plans")]
fn snapshot_data_files_to_delete_plan_impl(
    snapshot: &SnapshotRef,
    engine: &dyn Engine,
    mode: FfiDataFilesToDeleteMode,
    now_ms: i64,
    deleted_file_retention_ms: OptionalValue<u64>,
    anchor_version: OptionalValue<u64>,
) -> delta_kernel::Result<OptionalValue<KernelOwnedBytes>> {
    let anchor_version: Option<Version> = anchor_version.into();
    let mode = match mode {
        FfiDataFilesToDeleteMode::Full if anchor_version.is_some() => {
            return Err(KernelError::generic(
                "anchor_version is only valid for LITE candidate discovery",
            )
            .into());
        }
        FfiDataFilesToDeleteMode::Full => DataFilesToDeleteMode::Full,
        FfiDataFilesToDeleteMode::Lite => DataFilesToDeleteMode::Lite { anchor_version },
    };
    let options = DataFilesToDeleteOptions::new(now_ms, mode);
    let options = match deleted_file_retention_ms.into() {
        Some(retention_ms) => {
            options.with_deleted_file_retention(Duration::from_millis(retention_ms))
        }
        None => options,
    };
    let plan = snapshot.get_data_files_to_delete(engine, &options)?;
    Ok(plan
        .map(|plan| Operation::QueryPlan(plan).to_proto_bytes().into())
        .into())
}

#[cfg(all(test, feature = "declarative-plans"))]
mod tests {
    use delta_kernel::plans::proto::operation as proto_op;
    use prost::Message as _;
    use test_utils::{TestAction, actions_to_string};

    use super::{FfiDataFilesToDeleteMode, snapshot_data_files_to_delete_plan};
    use crate::error::FFIKernelError;
    use crate::ffi_test_utils::{assert_extern_result_error_contains, ok_or_panic, setup_snapshot};
    use crate::{OptionalValue, free_engine, free_snapshot};

    #[tokio::test]
    async fn returns_serialized_query_plan() {
        let (engine, snapshot) = setup_snapshot(actions_to_string(vec![
            TestAction::Metadata,
            TestAction::Add("part-1.parquet".to_owned()),
        ]))
        .await
        .unwrap();

        let result = unsafe {
            snapshot_data_files_to_delete_plan(
                snapshot.shallow_copy(),
                engine.shallow_copy(),
                FfiDataFilesToDeleteMode::Full,
                i64::MAX,
                OptionalValue::None,
                OptionalValue::None,
            )
        };
        let bytes = match ok_or_panic(result) {
            OptionalValue::Some(bytes) => unsafe { bytes.into_vec() },
            OptionalValue::None => panic!("expected a FULL plan"),
        };
        let operation = proto_op::Operation::decode(bytes.as_slice()).unwrap();
        assert!(matches!(
            operation.op,
            Some(proto_op::operation::Op::QueryPlan(_))
        ));

        unsafe {
            free_snapshot(snapshot);
            free_engine(engine);
        }
    }

    #[tokio::test]
    async fn rejects_full_mode_with_lite_anchor() {
        let (engine, snapshot) = setup_snapshot(actions_to_string(vec![
            TestAction::Metadata,
            TestAction::Add("part-1.parquet".to_owned()),
        ]))
        .await
        .unwrap();

        let result = unsafe {
            snapshot_data_files_to_delete_plan(
                snapshot.shallow_copy(),
                engine.shallow_copy(),
                FfiDataFilesToDeleteMode::Full,
                i64::MAX,
                OptionalValue::None,
                OptionalValue::Some(0),
            )
        };
        assert_extern_result_error_contains(
            result,
            FFIKernelError::GenericError,
            "anchor_version is only valid for LITE",
        );

        unsafe {
            free_snapshot(snapshot);
            free_engine(engine);
        }
    }

    #[tokio::test]
    async fn returns_none_for_empty_lite_range() {
        let (engine, snapshot) = setup_snapshot(actions_to_string(vec![
            TestAction::Metadata,
            TestAction::Add("part-1.parquet".to_owned()),
        ]))
        .await
        .unwrap();

        let result = unsafe {
            snapshot_data_files_to_delete_plan(
                snapshot.shallow_copy(),
                engine.shallow_copy(),
                FfiDataFilesToDeleteMode::Lite,
                i64::MAX,
                OptionalValue::None,
                OptionalValue::Some(0),
            )
        };
        assert!(matches!(ok_or_panic(result), OptionalValue::None));

        unsafe {
            free_snapshot(snapshot);
            free_engine(engine);
        }
    }
}
