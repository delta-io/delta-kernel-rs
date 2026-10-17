//! Borrowed protobuf transport for snapshot reconstruction and scan planning.

use std::sync::Arc;

use delta_kernel::snapshot::externalized::{snapshot_from_source, SourceSnapshotState};
use delta_kernel::KernelResult;
use url::Url;

use super::{invalid, FfiSnapshotHint};
use crate::error::{ExternResult, IntoExternResult};
use crate::handle::Handle;
use crate::{
    KernelBytesSlice, KernelStringSlice, NullableCvoid, SharedExternEngine, SharedSnapshot,
    TryFromStringSlice,
};

/// Receives bytes borrowed only for this callback. Return false to reject delivery.
///
/// The receiver must copy anything retained and must not unwind across the native frame.
pub type VisitKernelBytesFn =
    unsafe extern "C" fn(context: NullableCvoid, bytes: KernelBytesSlice) -> bool;

/// Exports the complete source snapshot schema as the existing schema protobuf.
///
/// The callback runs once on success. No schema handle or encoded buffer remains in Rust.
/// Returns an error if the callback rejects delivery.
///
/// # Safety
///
/// Both handles are borrowed and must remain valid throughout the call. The callback must not
/// release them, retain its borrowed input, or unwind.
#[no_mangle]
pub unsafe extern "C" fn snapshot_schema_to_proto(
    snapshot: Handle<SharedSnapshot>,
    engine: Handle<SharedExternEngine>,
    context: NullableCvoid,
    visitor: VisitKernelBytesFn,
) -> ExternResult<bool> {
    let snapshot = unsafe { snapshot.as_ref() };
    let engine = unsafe { engine.as_ref() };
    let bytes = delta_kernel::plans::proto::encode_schema(snapshot.schema().as_ref());
    unsafe { deliver(&bytes, context, visitor) }.into_extern_result(&engine)
}

/// Reconstructs an owned snapshot from an unchanged exported hint and complete schema protobuf.
///
/// Inputs are copied and remain owned by the caller. Release the returned handle with
/// `free_snapshot`. Built scans retain their own snapshot reference and can be reused after this
/// handle is released. Configuration and snapshot-load checks run; source-proven structural schema
/// and log-shape checks are not repeated.
///
/// # Safety
///
/// `value` must be non-null and all nested input memory must remain readable for this call. Enum
/// tags must be valid. `table_root`, `value`, and `schema` must be unchanged state exported from
/// one validated snapshot by this same Kernel build, and must remain immutable. This API must
/// not be used for connector-authored hints. The engine handle is borrowed and must remain valid
/// throughout the call. The metadata must include its original schema string.
#[no_mangle]
pub unsafe extern "C" fn snapshot_hint_reconstruct_trusted(
    table_root: KernelStringSlice,
    value: *const FfiSnapshotHint,
    schema: KernelBytesSlice,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<SharedSnapshot>> {
    let engine = unsafe { engine.as_ref() };
    let result = (|| {
        let value =
            unsafe { value.as_ref() }.ok_or_else(|| invalid("snapshot hint must not be null"))?;
        let root: &str = unsafe { TryFromStringSlice::try_from_slice(&table_root) }?;
        let table_root = Url::parse(root)?;
        let logical_schema =
            delta_kernel::plans::proto::decode_trusted_schema(unsafe { schema.try_as_slice() }?)?;
        let log_paths = unsafe { value.log_paths.trusted_snapshot_log_paths(&table_root) }?;
        let state = SourceSnapshotState {
            table_root,
            version: value.version,
            publication_watermark: value.publication_watermark.into(),
            log_paths,
            protocol: unsafe { value.protocol.try_to_kernel() }?,
            metadata: unsafe { value.metadata.try_to_kernel() }?,
            logical_schema,
            last_checkpoint: unsafe { value.last_checkpoint.as_ref() }
                .map(|value| unsafe { value.try_to_kernel() })
                .transpose()?,
            crc: unsafe { value.crc.as_ref() }
                .map(|value| unsafe { value.try_to_kernel() }.map(Arc::new))
                .transpose()?,
            freshness: value.freshness.into(),
        };
        snapshot_from_source(state).map(Into::into)
    })();
    result.into_extern_result(&engine)
}

unsafe fn deliver(
    bytes: &[u8],
    context: NullableCvoid,
    visitor: VisitKernelBytesFn,
) -> KernelResult<bool> {
    if unsafe { visitor(context, KernelBytesSlice::new_unsafe(bytes)) } {
        Ok(true)
    } else {
        Err(invalid("byte callback rejected delivery"))
    }
}
