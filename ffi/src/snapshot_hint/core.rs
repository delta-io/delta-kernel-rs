//! A small native handle for connector-owned snapshot state.

use std::sync::Arc;

use delta_kernel::snapshot::SnapshotState;
use delta_kernel::{KernelError, KernelResult, Version};
use delta_kernel_ffi_macros::handle_descriptor;
use url::Url;

use super::{
    invalid, validate_handoff, BorrowedSnapshotLogState, BorrowedSnapshotState, FfiSnapshotHint,
    FfiSnapshotLogState,
};
use crate::error::{AllocateErrorFn, ExternResult, IntoExternResult};
use crate::handle::Handle;
use crate::{SharedExternEngine, SharedMetadata, SharedProtocol, SharedSchema, SharedSnapshot};

/// Native identity retained after the connector takes ownership of snapshot components.
#[derive(Debug)]
pub struct SnapshotCore {
    table_root: Url,
    version: Version,
    latest: bool,
    generation: u64,
    #[cfg(feature = "declarative-plans")]
    metadata_scan: Option<delta_kernel::scan::ValidatedMetadataScan>,
}

/// Shared handle for a snapshot whose component state is held by its connector.
#[handle_descriptor(target=SnapshotCore, mutable=false, sized=true)]
pub struct SharedSnapshotCore;

unsafe fn core_from_snapshot(
    snapshot: &Handle<SharedSnapshot>,
    generation: u64,
) -> Arc<SnapshotCore> {
    let owned = unsafe { snapshot.as_ref() };
    Arc::new(SnapshotCore {
        table_root: owned.table_root().clone(),
        version: owned.version(),
        latest: owned.is_built_as_latest(),
        generation,
        // A snapshot can be readable through getters but unscannable (e.g. empty schema).
        // Preserve that behavior: unsuccessful validation uses the existing fallible path.
        #[cfg(feature = "declarative-plans")]
        metadata_scan: delta_kernel::scan::ValidatedMetadataScan::try_new(&unsafe {
            snapshot.clone_as_arc()
        })
        .ok(),
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
        .map(|_| unsafe { core_from_snapshot(&snapshot, generation) }.into());
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
    unsafe { core_from_snapshot(&snapshot, generation) }.into()
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

/// Whether this core can plan a metadata scan from [`FfiSnapshotLogState`].
///
/// # Safety
///
/// `core` must be a valid borrowed handle.
#[cfg(feature = "declarative-plans")]
#[no_mangle]
pub unsafe extern "C" fn snapshot_core_has_validated_metadata_plan(
    core: Handle<SharedSnapshotCore>,
) -> bool {
    unsafe { core.as_ref() }.metadata_scan.is_some()
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

fn borrowed_log_state<'a>(
    core: &'a SnapshotCore,
    value: &'a FfiSnapshotLogState,
    generation: u64,
) -> KernelResult<BorrowedSnapshotLogState<'a>> {
    if generation != core.generation
        || value.version != core.version
        || matches!(value.freshness, super::FfiSnapshotHintFreshness::Latest) != core.latest
    {
        return Err(invalid(
            "host snapshot generation, version, or freshness changed",
        ));
    }
    Ok(BorrowedSnapshotLogState {
        value,
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
    value: &FfiSnapshotHint,
    generation: u64,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<crate::OptionalValue<crate::KernelOwnedBytes>> {
    let core = unsafe { core.as_ref() };
    let extern_engine = unsafe { engine.as_ref() };
    let inner_engine = extern_engine.engine();
    let result = borrowed_state(core, value, generation).and_then(|state| {
        let plan = match &core.metadata_scan {
            Some(validated) => validated.plan(&state, inner_engine.as_ref())?,
            None => delta_kernel::scan::declarative_metadata_scan_plan_from_state(
                &state,
                inner_engine.as_ref(),
            )?,
        };
        Ok(plan
            .map(|plan| {
                delta_kernel::Operation::QueryPlan(plan)
                    .to_proto_bytes()
                    .into()
            })
            .into())
    });
    result.into_extern_result(&extern_engine)
}

/// Build a validated declarative scan plan from borrowed transaction-log state.
///
/// Callers must first check [`snapshot_core_has_validated_metadata_plan`]. The scoped value omits
/// protocol, metadata, table schema, and CRC state because validated metadata planning cannot use
/// them.
///
/// # Safety
///
/// Handles are borrowed. `value` and all nested storage must be valid for this call.
#[cfg(feature = "declarative-plans")]
#[no_mangle]
pub unsafe extern "C" fn snapshot_core_declarative_metadata_plan_from_log_state(
    core: Handle<SharedSnapshotCore>,
    value: &FfiSnapshotLogState,
    generation: u64,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<crate::OptionalValue<crate::KernelOwnedBytes>> {
    let core = unsafe { core.as_ref() };
    let extern_engine = unsafe { engine.as_ref() };
    let inner_engine = extern_engine.engine();
    let result = borrowed_log_state(core, value, generation).and_then(|state| {
        let validated = core.metadata_scan.as_ref().ok_or_else(|| {
            KernelError::internal_error("snapshot core does not contain a validated metadata scan")
        })?;
        Ok(validated
            .plan(&state, inner_engine.as_ref())?
            .map(|plan| {
                delta_kernel::Operation::QueryPlan(plan)
                    .to_proto_bytes()
                    .into()
            })
            .into())
    });
    result.into_extern_result(&extern_engine)
}
