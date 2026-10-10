//! Borrowed protobuf transport for request-local snapshot planning.

use delta_kernel::scan::externalized::{declarative_metadata_plan, SourceSnapshotScanState};
use delta_kernel::KernelResult;
use url::Url;

use super::{invalid, FfiSnapshotHint};
use crate::error::{ExternResult, IntoExternResult};
use crate::handle::Handle;
use crate::scan::SharedScan;
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

/// Builds a default metadata plan from an unchanged exported snapshot hint and schema protobuf.
///
/// All transferred components and reconstructed native state live only for this call. The full
/// schema is constructed. Table/protocol compatibility and scan-support checks run; source-proven
/// schema structural and log-segment checks are not repeated. The callback receives an Operation
/// encoded by Kernel's standard protobuf serializer. Returns true after delivery and false when
/// there is no plan. Errors do not deliver plan bytes.
///
/// # Safety
///
/// `value` must be non-null and all nested input memory must remain readable for this call. Enum
/// tags must be valid. `table_root`, `value`, and `schema` must be unchanged state exported from
/// one validated snapshot by this same Kernel build, and must remain immutable. This API must
/// not be used for connector-authored hints. The engine handle is borrowed. The callback must not
/// retain its borrowed bytes, release the engine, or unwind.
/// `value.metadata.schema_string` is ignored and may be an empty slice: the complete schema
/// is supplied by `schema`.
#[no_mangle]
pub unsafe extern "C" fn snapshot_hint_declarative_metadata_plan_trusted(
    table_root: KernelStringSlice,
    value: *const FfiSnapshotHint,
    schema: KernelBytesSlice,
    engine: Handle<SharedExternEngine>,
    context: NullableCvoid,
    visitor: VisitKernelBytesFn,
) -> ExternResult<bool> {
    let engine = unsafe { engine.as_ref() };
    let result = (|| {
        let value =
            unsafe { value.as_ref() }.ok_or_else(|| invalid("snapshot hint must not be null"))?;
        let root: &str = unsafe { TryFromStringSlice::try_from_slice(&table_root) }?;
        let table_root = Url::parse(root)?;
        let logical_schema =
            delta_kernel::plans::proto::decode_trusted_schema(unsafe { schema.try_as_slice() }?)?;
        let log_paths = unsafe { value.log_paths.trusted_snapshot_log_paths(&table_root) }?;
        let state = SourceSnapshotScanState {
            table_root,
            version: value.version,
            publication_watermark: value.publication_watermark.into(),
            log_paths,
            protocol: unsafe { value.protocol.try_to_kernel() }?,
            metadata: unsafe { value.metadata.try_to_kernel_with_schema(String::new()) }?,
            logical_schema,
            last_checkpoint: unsafe { value.last_checkpoint.as_ref() }
                .map(|value| unsafe { value.try_to_kernel() })
                .transpose()?,
        };
        let Some(plan) = declarative_metadata_plan(state, engine.engine().as_ref())? else {
            return Ok(false);
        };
        let bytes = delta_kernel::Operation::QueryPlan(plan).to_proto_bytes();
        unsafe { deliver(&bytes, context, visitor) }
    })();
    result.into_extern_result(&engine)
}

/// Delivers a retained scan's metadata plan through the same borrowed byte callback.
///
/// Returns true after delivery, false for no plan, and an error if planning or delivery fails.
/// The callback receives the standard protobuf Operation, not an owned native byte allocation.
///
/// # Safety
///
/// Both handles are borrowed. The callback must not release them, retain borrowed bytes, or
/// unwind. They must remain valid until the downcall returns.
#[no_mangle]
pub unsafe extern "C" fn scan_declarative_metadata_plan_visit(
    scan: Handle<SharedScan>,
    engine: Handle<SharedExternEngine>,
    context: NullableCvoid,
    visitor: VisitKernelBytesFn,
) -> ExternResult<bool> {
    let scan = unsafe { scan.as_ref() };
    let engine = unsafe { engine.as_ref() };
    let result = scan
        .declarative_metadata_scan_plan(engine.engine().as_ref())
        .and_then(|plan| match plan {
            Some(plan) => {
                let bytes = delta_kernel::Operation::QueryPlan(plan).to_proto_bytes();
                unsafe { deliver(&bytes, context, visitor) }
            }
            None => Ok(false),
        });
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
