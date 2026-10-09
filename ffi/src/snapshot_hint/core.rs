//! A small native handle for connector-owned snapshot state.

use std::sync::Arc;

use delta_kernel::snapshot::{SnapshotLogState, SnapshotScanState};
use delta_kernel::{KernelResult, Version};
use delta_kernel_ffi_macros::handle_descriptor;
use url::Url;

#[cfg(feature = "declarative-plans")]
use super::state::BorrowedSnapshotScanState;
#[cfg(feature = "declarative-plans")]
use super::FfiSnapshotScanState;
use super::{invalid, validate_handoff, BorrowedSnapshotState, FfiSnapshotHint};
use crate::error::{AllocateErrorFn, ExternResult, IntoExternResult};
use crate::handle::Handle;
use crate::log_path::FfiLogPath;
use crate::{
    kernel_string_slice, SharedExternEngine, SharedMetadata, SharedProtocol, SharedSchema,
    SharedSnapshot,
};

/// Native identity retained after the connector takes ownership of snapshot components.
#[derive(Debug)]
pub struct SnapshotCore {
    table_root: Url,
    version: Version,
    latest: bool,
    generation: u64,
    #[cfg_attr(not(feature = "declarative-plans"), allow(dead_code))]
    trusted_externalization: bool,
}

/// Shared handle for a snapshot whose component state is held by its connector.
#[handle_descriptor(target=SnapshotCore, mutable=false, sized=true)]
pub struct SharedSnapshotCore;

/// Visits one borrowed batch of log paths. The callback must copy any data it retains.
pub type VisitSnapshotLogPathsFn = unsafe extern "C" fn(
    context: *mut std::ffi::c_void,
    paths: *const FfiLogPath,
    count: usize,
) -> bool;

/// Whether this snapshot was confirmed latest when it was built.
///
/// # Safety
///
/// `snapshot` must be a valid borrowed handle.
#[no_mangle]
pub unsafe extern "C" fn snapshot_is_built_as_latest(snapshot: Handle<SharedSnapshot>) -> bool {
    unsafe { snapshot.as_ref() }.is_built_as_latest()
}

/// Copy the exact log paths retained by a snapshot through bounded borrowed batches.
///
/// Returns false when the connector callback rejects a batch. Neither the callback nor the
/// connector may retain the array or any nested string pointer after the callback returns.
///
/// # Safety
///
/// `snapshot` must be a valid borrowed handle and `visitor` must be safe to call for this method's
/// duration.
#[no_mangle]
pub unsafe extern "C" fn snapshot_visit_log_paths(
    snapshot: Handle<SharedSnapshot>,
    context: *mut std::ffi::c_void,
    visitor: VisitSnapshotLogPathsFn,
) -> bool {
    let snapshot = unsafe { snapshot.as_ref() };
    let mut accepted = true;
    let result = snapshot.visit_log_paths(&mut |batch| {
        let paths: Vec<FfiLogPath> = batch
            .iter()
            .map(|path| {
                let file = path.file_meta();
                let location = file.location.as_str();
                FfiLogPath::new(
                    kernel_string_slice!(location),
                    file.last_modified,
                    file.size,
                )
            })
            .collect();
        accepted = unsafe { visitor(context, paths.as_ptr(), paths.len()) };
        if accepted {
            Ok(())
        } else {
            Err(invalid("host rejected snapshot log-path batch"))
        }
    });
    accepted && result.is_ok()
}

unsafe fn core_from_snapshot(
    snapshot: &Handle<SharedSnapshot>,
    generation: u64,
    trusted_externalization: bool,
) -> Arc<SnapshotCore> {
    let owned = unsafe { snapshot.as_ref() };
    Arc::new(SnapshotCore {
        table_root: owned.table_root().clone(),
        version: owned.version(),
        latest: owned.is_built_as_latest(),
        generation,
        trusted_externalization,
    })
}

/// Validate host state and construct a small core without consuming `snapshot`.
///
/// Java must release the original snapshot only after this call succeeds. On failure, both the
/// native snapshot and host state remain caller-owned. `generation` is a caller-minted immutable
/// identity checked on every subsequent borrow.
///
/// # Safety
///
/// Both handles are borrowed and must be valid. `value` and its nested pointers must be valid
/// for this call. The connector must keep the state immutable for the core's lifetime.
#[no_mangle]
pub unsafe extern "C" fn snapshot_externalize_core(
    snapshot: Handle<SharedSnapshot>,
    value: &FfiSnapshotHint,
    generation: u64,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<SharedSnapshotCore>> {
    let owned = unsafe { snapshot.as_ref() };
    let state = BorrowedSnapshotState {
        hint: value,
        table_root: owned.table_root(),
    };
    let engine_ref = unsafe { engine.as_ref() };
    let result = validate_handoff(owned, &state)
        .map(|_| unsafe { core_from_snapshot(&snapshot, generation, false) }.into());
    result.into_extern_result(&engine_ref)
}

/// Construct a small core after the connector has proven that its immutable state was the exact
/// state used to build `snapshot`.
///
/// This avoids copying that state again at handoff. Callers without such a proof must use
/// [`snapshot_externalize_core`], which compares every component.
///
/// # Safety
///
/// `snapshot` must be a valid borrowed handle. The caller must retain the exact immutable state
/// used to build it and associate that state with `generation` for the core's lifetime.
#[no_mangle]
pub unsafe extern "C" fn snapshot_externalize_validated_core(
    snapshot: Handle<SharedSnapshot>,
    generation: u64,
) -> Handle<SharedSnapshotCore> {
    unsafe { core_from_snapshot(&snapshot, generation, false) }.into()
}

/// Externalize a core that can plan from portable state validated by the source snapshot.
///
/// Unlike [`snapshot_externalize_validated_core`], this grants access to trusted planning. It does
/// not retain the parsed table configuration after the source snapshot is released.
///
/// # Safety
///
/// `snapshot` is borrowed and must reference a valid snapshot for this call. The caller must retain
/// the exact immutable ordered log paths and scan state exported from this source snapshot,
/// associate them with `generation`, and return the same state on every pass for the core's
/// lifetime. State from a different snapshot or generation must never be used with the core.
#[no_mangle]
pub unsafe extern "C" fn snapshot_externalize_trusted_core(
    snapshot: Handle<SharedSnapshot>,
    generation: u64,
) -> Handle<SharedSnapshotCore> {
    unsafe { core_from_snapshot(&snapshot, generation, true) }.into()
}

/// Release a connector-backed snapshot core. The connector still owns its host state.
///
/// # Safety
///
/// `core` must be a valid, owned handle and must not be used after this call.
#[no_mangle]
pub unsafe extern "C" fn free_snapshot_core(core: Handle<SharedSnapshotCore>) {
    core.drop_handle();
}

/// The validated version held in the small native core.
///
/// # Safety
///
/// `core` must be a valid borrowed handle.
#[no_mangle]
pub unsafe extern "C" fn snapshot_core_version(core: Handle<SharedSnapshotCore>) -> Version {
    unsafe { core.as_ref() }.version
}

fn borrowed_state<'a>(
    core: &'a SnapshotCore,
    value: &'a FfiSnapshotHint,
    generation: u64,
) -> KernelResult<BorrowedSnapshotState<'a>> {
    if generation != core.generation
        || value.version != core.version
        || matches!(value.freshness, super::FfiSnapshotHintFreshness::Latest) != core.latest
    {
        return Err(invalid(
            "host snapshot generation, version, or freshness changed",
        ));
    }
    Ok(BorrowedSnapshotState {
        hint: value,
        table_root: &core.table_root,
    })
}

/// Read the logical schema from connector-owned state for this call only.
/// The returned schema handle owns its Rust copy and must be freed with `free_schema`.
///
/// # Safety
///
/// Handles are borrowed. `value` and all nested storage must be valid for this call.
#[no_mangle]
pub unsafe extern "C" fn snapshot_core_logical_schema(
    core: Handle<SharedSnapshotCore>,
    value: &FfiSnapshotHint,
    generation: u64,
    allocate_error: AllocateErrorFn,
) -> ExternResult<Handle<SharedSchema>> {
    let core = unsafe { core.as_ref() };
    let result = borrowed_state(core, value, generation)
        .and_then(|state| state.logical_schema())
        .map(Into::into);
    result.into_extern_result(&allocate_error)
}

/// Read the protocol from connector-owned state for this call only.
/// The returned handle must be freed with `free_protocol`.
///
/// # Safety
///
/// Handles are borrowed. `value` and all nested storage must be valid for this call.
#[no_mangle]
pub unsafe extern "C" fn snapshot_core_get_protocol(
    core: Handle<SharedSnapshotCore>,
    value: &FfiSnapshotHint,
    generation: u64,
    allocate_error: AllocateErrorFn,
) -> ExternResult<Handle<SharedProtocol>> {
    let core = unsafe { core.as_ref() };
    borrowed_state(core, value, generation)
        .and_then(|state| state.protocol())
        .map(|protocol| Arc::new(protocol).into())
        .into_extern_result(&allocate_error)
}

/// Read the metadata from connector-owned state for this call only.
/// The returned handle must be freed with `free_metadata`.
///
/// # Safety
///
/// Handles are borrowed. `value` and all nested storage must be valid for this call.
#[no_mangle]
pub unsafe extern "C" fn snapshot_core_get_metadata(
    core: Handle<SharedSnapshotCore>,
    value: &FfiSnapshotHint,
    generation: u64,
    allocate_error: AllocateErrorFn,
) -> ExternResult<Handle<SharedMetadata>> {
    let core = unsafe { core.as_ref() };
    borrowed_state(core, value, generation)
        .and_then(|state| state.metadata())
        .map(|metadata| Arc::new(metadata).into())
        .into_extern_result(&allocate_error)
}

/// Build a declarative scan plan from one scoped borrow of connector state.
///
/// This prototype holds only the planning inputs needed for this call. It supports the default
/// full-table scan; projection and predicate variants are separate future API work.
///
/// # Safety
///
/// Handles are borrowed. `value` and all nested storage must be valid for this call.
#[cfg(feature = "declarative-plans")]
#[no_mangle]
pub unsafe extern "C" fn snapshot_core_declarative_metadata_plan(
    core: Handle<SharedSnapshotCore>,
    value: &FfiSnapshotScanState,
    generation: u64,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<crate::OptionalValue<crate::KernelOwnedBytes>> {
    let core = unsafe { core.as_ref() };
    let extern_engine = unsafe { engine.as_ref() };
    let inner_engine = extern_engine.engine();
    let result = metadata_plan(core, value, generation, inner_engine.as_ref(), None);
    result.into_extern_result(&extern_engine)
}

#[cfg(feature = "declarative-plans")]
fn metadata_plan(
    core: &SnapshotCore,
    value: &FfiSnapshotScanState,
    generation: u64,
    engine: &dyn delta_kernel::Engine,
    metadata: Option<delta_kernel::actions::Metadata>,
) -> KernelResult<crate::OptionalValue<crate::KernelOwnedBytes>> {
    validate_scan_identity(core, value, generation)?;
    let state = BorrowedSnapshotScanState {
        value,
        table_root: &core.table_root,
    };
    let plan = match metadata {
        Some(metadata) => {
            delta_kernel::scan::declarative_metadata_scan_plan_from_state_with_metadata(
                &state, metadata, engine,
            )?
        }
        None => delta_kernel::scan::declarative_metadata_scan_plan_from_state(&state, engine)?,
    };
    Ok(plan
        .map(|plan| {
            delta_kernel::Operation::QueryPlan(plan)
                .to_proto_bytes()
                .into()
        })
        .into())
}

/// Build a declarative scan plan from portable state validated by the source snapshot.
///
/// Unlike the ordinary planning entry point, this reconstructs ephemeral native configuration
/// without repeating source-proven compatibility checks. Only a core created by
/// [`snapshot_externalize_trusted_core`] grants access to this path. Generation, version,
/// freshness, FFI bounds, and scan-operation checks still run. Portable metadata and log paths
/// are parsed, but no substantial native snapshot state survives the call.
///
/// # Safety
///
/// Handles are borrowed. `value` and its log-path storage must be valid for this call. `value` must
/// be the exact immutable scan state exported from the source snapshot used to create `core`, must
/// be associated with the same `generation`, and must return the same ordered paths on both passes.
#[cfg(feature = "declarative-plans")]
#[no_mangle]
pub unsafe extern "C" fn snapshot_core_declarative_metadata_plan_trusted(
    core: Handle<SharedSnapshotCore>,
    value: &FfiSnapshotScanState,
    generation: u64,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<crate::OptionalValue<crate::KernelOwnedBytes>> {
    let core = unsafe { core.as_ref() };
    let extern_engine = unsafe { engine.as_ref() };
    let result = trusted_metadata_plan(
        core,
        value,
        generation,
        None,
        extern_engine.engine().as_ref(),
    );
    result.into_extern_result(&extern_engine)
}

#[cfg(feature = "declarative-plans")]
fn trusted_metadata_plan(
    core: &SnapshotCore,
    value: &FfiSnapshotScanState,
    generation: u64,
    metadata: Option<delta_kernel::actions::Metadata>,
    engine: &dyn delta_kernel::Engine,
) -> KernelResult<crate::OptionalValue<crate::KernelOwnedBytes>> {
    validate_scan_identity(core, value, generation)?;
    if !core.trusted_externalization {
        return Err(invalid(
            "trusted planning requires a trusted externalized snapshot core",
        ));
    }
    let state = BorrowedSnapshotScanState {
        value,
        table_root: &core.table_root,
    };
    let metadata = metadata.map_or_else(|| state.metadata(), Ok)?;
    let plan = delta_kernel::scan::declarative_metadata_scan_plan_from_trusted_state(
        &state, metadata, engine,
    )?;
    Ok(plan
        .map(|plan| {
            delta_kernel::Operation::QueryPlan(plan)
                .to_proto_bytes()
                .into()
        })
        .into())
}

#[cfg(feature = "declarative-plans")]
fn validate_scan_identity(
    core: &SnapshotCore,
    value: &FfiSnapshotScanState,
    generation: u64,
) -> KernelResult<()> {
    if generation != core.generation
        || value.version != core.version
        || matches!(value.freshness, super::FfiSnapshotHintFreshness::Latest) != core.latest
    {
        return Err(invalid(
            "host snapshot generation, version, or freshness changed",
        ));
    }
    Ok(())
}

/// A preallocated schema transfer buffer. It does not retain connector pointers.
pub struct SnapshotSchemaUpload {
    bytes: KernelResult<Vec<u8>>,
    expected: usize,
}

/// An exclusively owned schema upload, consumed by planning or explicitly freed.
#[handle_descriptor(target=SnapshotSchemaUpload, mutable=true, sized=true)]
pub struct ExclusiveSnapshotSchemaUpload;

/// Reserve schema storage before entering a pinned Java-array call.
/// Allocation errors are deferred until planning; appending to a failed upload returns false.
#[no_mangle]
pub extern "C" fn snapshot_schema_upload_new(
    length: usize,
) -> Handle<ExclusiveSnapshotSchemaUpload> {
    let mut bytes = Vec::new();
    let reserved = bytes.try_reserve_exact(length);
    Box::new(SnapshotSchemaUpload {
        bytes: reserved.map(|()| bytes).map_err(|e| invalid(e.to_string())),
        expected: length,
    })
    .into()
}

/// Copy at most 64 KiB from a call-scoped borrowed array into reserved schema storage.
/// Returns false for a failed reservation, oversized chunk, or excess input. Never calls the
/// connector, allocates, parses the schema, or retains the input pointer.
///
/// # Safety
/// The upload handle is exclusively borrowed. `bytes` must address `length` readable bytes and
/// remain immutable for this call. No other thread may access the upload during the call.
#[no_mangle]
pub unsafe extern "C" fn snapshot_schema_upload_append(
    mut upload: Handle<ExclusiveSnapshotSchemaUpload>,
    bytes: *const u8,
    length: usize,
) -> bool {
    let upload = unsafe { upload.as_mut() };
    let Ok(output) = &mut upload.bytes else {
        return false;
    };
    if bytes.is_null() || length > 65536 || length > upload.expected - output.len() {
        return false;
    }
    output.extend_from_slice(unsafe { std::slice::from_raw_parts(bytes, length) });
    true
}

/// Discard an upload without planning.
///
/// # Safety
/// Consumes a valid owned handle unconditionally; the caller must not use it again.
#[no_mangle]
pub unsafe extern "C" fn free_snapshot_schema_upload(
    upload: Handle<ExclusiveSnapshotSchemaUpload>,
) {
    upload.drop_handle();
}

impl SnapshotSchemaUpload {
    fn finish_bytes(self) -> KernelResult<Vec<u8>> {
        let bytes = self.bytes?;
        if bytes.len() != self.expected {
            return Err(invalid("Incomplete schema upload"));
        }
        Ok(bytes)
    }

    pub(super) fn finish(self) -> KernelResult<String> {
        String::from_utf8(self.finish_bytes()?).map_err(|e| invalid(e.to_string()))
    }
}

/// Plan using a transferred schema, without a second full schema string in FFI staging.
/// All scan validation runs here, after the connector has released pinned array access.
/// `value.metadata.schema_string` must be empty; the upload supplies that field.
///
/// # Safety
/// Consumes `upload` unconditionally, including on errors. Other handles and `value` are borrowed
/// and all nested storage must be valid for this call. No pinned JVM arrays may remain acquired:
/// this call may invoke Java engine and error callbacks.
#[cfg(feature = "declarative-plans")]
#[no_mangle]
pub unsafe extern "C" fn snapshot_core_declarative_metadata_plan_with_schema(
    core: Handle<SharedSnapshotCore>,
    value: &FfiSnapshotScanState,
    generation: u64,
    upload: Handle<ExclusiveSnapshotSchemaUpload>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<crate::OptionalValue<crate::KernelOwnedBytes>> {
    let upload = unsafe { upload.into_inner() };
    let core = unsafe { core.as_ref() };
    let engine = unsafe { engine.as_ref() };
    let result = (|| {
        if value.metadata.schema_string.len != 0 {
            return Err(invalid("Schema supplied both inline and as an upload"));
        }
        let schema = upload.finish()?;
        let metadata = unsafe { value.metadata.try_to_kernel_with_schema(schema) }?;
        metadata_plan(
            core,
            value,
            generation,
            engine.engine().as_ref(),
            Some(metadata),
        )
    })();
    result.into_extern_result(&engine)
}

/// Trusted planning variant that consumes a transferred schema.
///
/// The completed upload is parsed to materialize an ephemeral table configuration. Compatibility
/// checks already performed by the source snapshot are not repeated.
///
/// # Safety
/// Consumes `upload` unconditionally, including on errors. Other handles and `value` are borrowed
/// and its log-path storage must be valid for this call. `value` and the uploaded schema bytes must
/// be the exact immutable scan state exported from the source snapshot used to create `core`, must
/// be associated with the same `generation`, and must return the same ordered paths on both passes.
#[cfg(feature = "declarative-plans")]
#[no_mangle]
pub unsafe extern "C" fn snapshot_core_declarative_metadata_plan_trusted_with_schema(
    core: Handle<SharedSnapshotCore>,
    value: &FfiSnapshotScanState,
    generation: u64,
    upload: Handle<ExclusiveSnapshotSchemaUpload>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<crate::OptionalValue<crate::KernelOwnedBytes>> {
    let upload = unsafe { upload.into_inner() };
    let core = unsafe { core.as_ref() };
    let engine = unsafe { engine.as_ref() };
    let result = (|| {
        if value.metadata.schema_string.len != 0 {
            return Err(invalid("Schema supplied both inline and as an upload"));
        }
        let schema = upload.finish()?;
        let metadata = unsafe { value.metadata.try_to_kernel_with_schema(schema) }?;
        trusted_metadata_plan(
            core,
            value,
            generation,
            Some(metadata),
            engine.engine().as_ref(),
        )
    })();
    result.into_extern_result(&engine)
}

#[cfg(test)]
mod upload_tests {
    use super::*;

    #[test]
    fn uploaded_schema_keeps_storage_and_rejects_invalid_chunks() {
        let upload = snapshot_schema_upload_new(4);
        let pointer = unsafe { upload.as_ref() }.bytes.as_ref().unwrap().as_ptr();
        assert!(unsafe { snapshot_schema_upload_append(upload.shallow_copy(), b"ab".as_ptr(), 2) });
        assert!(!unsafe {
            snapshot_schema_upload_append(upload.shallow_copy(), b"xyz".as_ptr(), 3)
        });
        assert!(unsafe { snapshot_schema_upload_append(upload.shallow_copy(), b"cd".as_ptr(), 2) });
        let result = unsafe { upload.into_inner() }.finish().unwrap();
        assert_eq!(result, "abcd");
        assert_eq!(result.as_ptr(), pointer);
    }

    #[test]
    fn incomplete_and_invalid_utf8_uploads_fail() {
        for bytes in [b"a".as_slice(), &[0xff, 0xff]] {
            let upload = snapshot_schema_upload_new(2);
            assert!(unsafe {
                snapshot_schema_upload_append(upload.shallow_copy(), bytes.as_ptr(), bytes.len())
            });
            assert!(unsafe { upload.into_inner() }.finish().is_err());
        }
        unsafe { free_snapshot_schema_upload(snapshot_schema_upload_new(10)) };
    }
}
