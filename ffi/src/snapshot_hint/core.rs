//! A small native handle for connector-owned snapshot state.

use std::sync::Arc;

use delta_kernel::snapshot::SnapshotState;
use delta_kernel::{DeltaResult, Version};
use delta_kernel_ffi_macros::handle_descriptor;
use url::Url;

use super::{invalid, validate_handoff, BorrowedSnapshotState, FfiSnapshotHint};
#[cfg(feature = "declarative-plans")]
use super::{FfiSnapshotHintFreshness, SnapshotHint, SnapshotHintFreshness};
use crate::error::{ExternResult, IntoExternResult};
use crate::handle::Handle;
use crate::{SharedExternEngine, SharedMetadata, SharedProtocol, SharedSchema, SharedSnapshot};

/// Native identity retained after the connector takes ownership of snapshot components.
#[derive(Debug)]
pub struct SnapshotCore {
    table_root: Url,
    version: Version,
    latest: bool,
    generation: u64,
}

/// Shared handle for a snapshot whose component state is held by its connector.
#[handle_descriptor(target=SnapshotCore, mutable=false, sized=true)]
pub struct SharedSnapshotCore;

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
    let engine_impl = engine_ref.engine();
    let result = validate_handoff(owned, &state, engine_impl.as_ref()).map(|_| {
        Arc::new(SnapshotCore {
            table_root: owned.table_root().clone(),
            version: owned.version(),
            latest: owned.is_built_as_latest(),
            generation,
        })
        .into()
    });
    result.into_extern_result(&engine_ref)
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
) -> DeltaResult<BorrowedSnapshotState<'a>> {
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
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<SharedSchema>> {
    let core = unsafe { core.as_ref() };
    let result = borrowed_state(core, value, generation)
        .and_then(|state| state.logical_schema())
        .map(Into::into);
    result.into_extern_result(unsafe { &engine.as_ref() })
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
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<SharedProtocol>> {
    let core = unsafe { core.as_ref() };
    borrowed_state(core, value, generation)
        .and_then(|state| state.protocol())
        .map(|protocol| Arc::new(protocol).into())
        .into_extern_result(unsafe { &engine.as_ref() })
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
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<SharedMetadata>> {
    let core = unsafe { core.as_ref() };
    borrowed_state(core, value, generation)
        .and_then(|state| state.metadata())
        .map(|metadata| Arc::new(metadata).into())
        .into_extern_result(unsafe { &engine.as_ref() })
}

/// Build a declarative scan plan from one scoped borrow of connector state.
///
/// This prototype holds the expanded snapshot and scan only during this call. It supports the
/// default full-table scan; projection and predicate variants are separate future API work.
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
        let mut paths = Vec::new();
        state.visit_log_paths(&mut |batch| {
            paths.extend_from_slice(batch);
            Ok(())
        })?;
        let freshness = if matches!(value.freshness, FfiSnapshotHintFreshness::Latest) {
            SnapshotHintFreshness::Latest
        } else {
            SnapshotHintFreshness::Unverified
        };
        let hint = SnapshotHint::try_new(
            state.version(),
            paths,
            state.protocol()?,
            state.metadata()?,
            state.last_checkpoint()?,
            state.crc()?,
            freshness,
        )?;
        let snapshot = delta_kernel::Snapshot::builder_for(state.table_root().as_str())
            .with_snapshot_hint(hint)
            .build(inner_engine.as_ref())?;
        let scan = snapshot.scan_builder().build()?;
        let plan = scan.declarative_metadata_scan_plan(inner_engine.as_ref())?;
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
