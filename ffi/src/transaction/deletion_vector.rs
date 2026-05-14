//! FFI surface for deletion-vector update transactions.
//!
//! Engines build a descriptor map from connector-authored DVs, then pass it with scan metadata to
//! [`transaction_update_deletion_vectors`] to stage the remove/add action pairs.

use std::collections::HashMap;
use std::os::raw::c_int;

use delta_kernel::actions::deletion_vector::{DeletionVectorDescriptor, DeletionVectorStorageType};
use delta_kernel::transaction::Transaction;
use delta_kernel::{DeltaResult, Error};
use delta_kernel_ffi_macros::handle_descriptor;

use super::ExclusiveTransaction;
use crate::error::{ExternResult, IntoExternResult};
use crate::handle::Handle;
use crate::scan::SharedScanMetadataIterator;
use crate::{KernelStringSlice, SharedExternEngine, TryFromStringSlice};

// =============================================================================
// DvDescriptorMap: opaque map handle
// =============================================================================

/// Owns a map from data-file path to the new [`DeletionVectorDescriptor`] for that file.
/// Engines build the map by inserting descriptors and pass it to
/// [`transaction_update_deletion_vectors`].
pub struct DvDescriptorMap {
    inner: HashMap<String, DeletionVectorDescriptor>,
}

/// Mutable handle for a deletion vector descriptor map.
#[handle_descriptor(target=DvDescriptorMap, mutable=true, sized=true)]
pub struct ExclusiveDvDescriptorMap;

/// Mutable handle for a [`DeletionVectorDescriptor`] crossing the FFI boundary.
#[handle_descriptor(target=DeletionVectorDescriptor, mutable=true, sized=true)]
pub struct ExclusiveDvDescriptor;

/// Allocate an empty deletion vector descriptor map. The returned handle must be released
/// either by [`free_dv_descriptor_map`] or by [`transaction_update_deletion_vectors`]
/// (which consumes the map).
#[no_mangle]
pub extern "C" fn dv_descriptor_map_new() -> Handle<ExclusiveDvDescriptorMap> {
    Box::new(DvDescriptorMap {
        inner: HashMap::new(),
    })
    .into()
}

/// Free a deletion vector descriptor map handle.
///
/// # Safety
///
/// Caller must pass a valid handle previously returned by [`dv_descriptor_map_new`].
#[no_mangle]
pub unsafe extern "C" fn free_dv_descriptor_map(map: Handle<ExclusiveDvDescriptorMap>) {
    map.drop_handle();
}

/// Free a deletion vector descriptor handle. Only call this if the descriptor has not
/// been moved into a map via [`dv_descriptor_map_insert`].
///
/// # Safety
///
/// Caller must pass a valid descriptor handle.
#[no_mangle]
pub unsafe extern "C" fn free_dv_descriptor(descriptor: Handle<ExclusiveDvDescriptor>) {
    descriptor.drop_handle();
}

// =============================================================================
// DV descriptor construction
// =============================================================================

/// Storage type for a [`DeletionVectorDescriptor`], mirroring the protocol-level
/// `storageType` field. Values match the protocol's single-character codes:
/// - `PersistedRelative` (`'u'`): the DV is stored on disk; path is reconstructed from the prefix +
///   z85-encoded UUID held in `path_or_inline_dv`
/// - `Inline` (`'i'`): the deletion vector is stored inline in the log
/// - `PersistedAbsolute` (`'p'`): the DV is stored at the absolute URL given by `path_or_inline_dv`
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KernelDvStorageType {
    /// `'u'` -- persisted DV with a relative `<prefix><z85-uuid>` reference.
    PersistedRelative = 0,
    /// `'i'` -- inline DV stored directly in the log.
    Inline = 1,
    /// `'p'` -- persisted DV with an absolute URL reference.
    PersistedAbsolute = 2,
}

impl From<KernelDvStorageType> for DeletionVectorStorageType {
    fn from(value: KernelDvStorageType) -> Self {
        match value {
            KernelDvStorageType::PersistedRelative => Self::PersistedRelative,
            KernelDvStorageType::Inline => Self::Inline,
            KernelDvStorageType::PersistedAbsolute => Self::PersistedAbsolute,
        }
    }
}

impl TryFrom<c_int> for KernelDvStorageType {
    type Error = Error;

    fn try_from(value: c_int) -> DeltaResult<Self> {
        match value {
            0 => Ok(Self::PersistedRelative),
            1 => Ok(Self::Inline),
            2 => Ok(Self::PersistedAbsolute),
            _ => Err(Error::generic(format!(
                "invalid deletion vector storage type: {value}"
            ))),
        }
    }
}

/// Construct a [`DeletionVectorDescriptor`] from raw fields, for engines that author DV
/// files themselves and want to install them via [`transaction_update_deletion_vectors`].
///
/// Field validation (storage-type rules, non-negative size/cardinality/offset, etc.) is
/// performed by [`DeletionVectorDescriptor::try_new`]; see its docs for the full contract.
/// Pass `has_offset = false` to omit the offset.
///
/// For persisted DVs, `offset` is the byte offset within the DV file at which the DV's
/// 4-byte big-endian size prefix begins. For a single-DV file, this is usually `1`
/// (skipping the version byte). Omitting the offset is only appropriate for single-DV files
/// where the size prefix begins immediately after the version byte.
///
/// # Safety
///
/// Caller must pass valid string slice and engine handle.
#[no_mangle]
pub unsafe extern "C" fn dv_descriptor_new(
    storage_type: c_int,
    path_or_inline_dv: KernelStringSlice,
    has_offset: bool,
    offset: i32,
    size_in_bytes: i32,
    cardinality: i64,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveDvDescriptor>> {
    let engine = unsafe { engine.as_ref() };
    let path = unsafe { TryFromStringSlice::try_from_slice(&path_or_inline_dv) };
    let result = KernelDvStorageType::try_from(storage_type).and_then(|storage_type| {
        dv_descriptor_new_impl(
            storage_type,
            path,
            has_offset,
            offset,
            size_in_bytes,
            cardinality,
        )
    });
    result.into_extern_result(&engine)
}

fn dv_descriptor_new_impl(
    storage_type: KernelDvStorageType,
    path: DeltaResult<&str>,
    has_offset: bool,
    offset: i32,
    size_in_bytes: i32,
    cardinality: i64,
) -> DeltaResult<Handle<ExclusiveDvDescriptor>> {
    let descriptor = DeletionVectorDescriptor::try_new(
        storage_type.into(),
        path?,
        has_offset.then_some(offset),
        size_in_bytes,
        cardinality,
    )?;
    Ok(Box::new(descriptor).into())
}

/// Insert a deletion vector descriptor into the map under the given data file path.
/// Consumes the descriptor handle on success. On error (e.g. invalid `data_file_path`),
/// the descriptor handle is left untouched and must still be released by the caller via
/// [`free_dv_descriptor`].
///
/// `data_file_path` must be the data-file path exactly as it appears in the scan
/// metadata produced by the kernel (the Add file action's `path` field). The kernel
/// matches against this string when applying the DV update; a typo causes
/// [`transaction_update_deletion_vectors`] to return an error.
/// Re-inserting a descriptor for an existing path replaces the previous descriptor.
///
/// # Safety
///
/// Caller must pass valid handles. The descriptor handle is consumed only on success.
#[no_mangle]
pub unsafe extern "C" fn dv_descriptor_map_insert(
    mut map: Handle<ExclusiveDvDescriptorMap>,
    data_file_path: KernelStringSlice,
    descriptor: Handle<ExclusiveDvDescriptor>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<bool> {
    let map_ref = unsafe { map.as_mut() };
    let engine_ref = unsafe { engine.as_ref() };
    // Parse the path BEFORE taking ownership of the descriptor: if parsing fails the
    // descriptor must remain valid so the caller can free it (otherwise we get a UAF
    // when they retry or clean up).
    let path_result = unsafe { TryFromStringSlice::try_from_slice(&data_file_path) };
    // Return value is a meaningless placeholder; cbindgen drops `ExternResult<()>` to a
    // bare `ExternResult` with no tag/payload, which is unusable from C. `bool` is the
    // existing convention for void-like fallible FFI functions (e.g. `visit_domain_metadata`).
    dv_descriptor_map_insert_impl(map_ref, path_result, descriptor)
        .map(|_| true)
        .into_extern_result(&engine_ref)
}

fn dv_descriptor_map_insert_impl(
    map: &mut DvDescriptorMap,
    data_file_path: DeltaResult<&str>,
    descriptor: Handle<ExclusiveDvDescriptor>,
) -> DeltaResult<()> {
    let path = data_file_path?;
    let owned_path = path.to_string();
    let descriptor = unsafe { descriptor.into_inner() };
    map.inner.insert(owned_path, *descriptor);
    Ok(())
}

// =============================================================================
// Apply DV updates to a transaction
// =============================================================================

/// Stage deletion-vector update actions on the transaction. For every entry in `dv_map`
/// the kernel emits a Remove + Add action pair on commit (the Add carries the new DV
/// descriptor; row-level statistics from the original Add are preserved).
/// Matched scan metadata must include an accurate `numRecords` statistic because the Delta
/// protocol requires it for files with deletion vectors.
///
/// Consumes `dv_map`. On success, drains `scan_iter` so subsequent calls to
/// `scan_metadata_next` return `false`; on error, the iterator may be partially consumed.
/// The engine should pass an iterator that covers at least every file path mentioned in the
/// map; extra files are ignored. If the map references a path that does not appear in the
/// iterator, the call returns an error and leaves the transaction unchanged.
///
/// This stages data-changing DV updates by default. Call
/// [`crate::transaction::set_data_change`] first for maintenance operations that should commit
/// with `dataChange = false`.
///
/// # Safety
///
/// Caller must pass valid handles. The transaction handle is borrowed in place and
/// remains valid after this call; the caller is expected to follow with `commit` (or
/// `free_transaction`) on the same handle. The DV map handle is consumed and must not
/// be used or freed after this call. The scan iterator handle is borrowed but drained
/// or partially drained (the iterator's mutex is held for the duration of this call),
/// and afterwards may only be passed to `free_scan_metadata_iter`.
#[no_mangle]
pub unsafe extern "C" fn transaction_update_deletion_vectors(
    mut txn: Handle<ExclusiveTransaction>,
    dv_map: Handle<ExclusiveDvDescriptorMap>,
    scan_iter: Handle<SharedScanMetadataIterator>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<bool> {
    let txn_ref = unsafe { txn.as_mut() };
    let dv_map = unsafe { dv_map.into_inner() };
    let scan_iter_ref = unsafe { scan_iter.as_ref() };
    let engine_ref = unsafe { engine.as_ref() };
    // See `dv_descriptor_map_insert` for why this returns `bool` instead of `()`.
    transaction_update_deletion_vectors_impl(txn_ref, *dv_map, scan_iter_ref)
        .map(|_| true)
        .into_extern_result(&engine_ref)
}

fn transaction_update_deletion_vectors_impl(
    txn: &mut Transaction,
    dv_map: DvDescriptorMap,
    scan_iter: &crate::scan::ScanMetadataIterator,
) -> DeltaResult<()> {
    let mut guard = scan_iter.lock_iter()?;
    let files_iter = Transaction::scan_metadata_to_engine_data(&mut **guard);
    txn.update_deletion_vectors(dv_map.inner, files_iter)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kernel_dv_storage_type_maps_to_kernel_storage_type() {
        let cases = [
            (
                KernelDvStorageType::PersistedRelative,
                DeletionVectorStorageType::PersistedRelative,
            ),
            (
                KernelDvStorageType::Inline,
                DeletionVectorStorageType::Inline,
            ),
            (
                KernelDvStorageType::PersistedAbsolute,
                DeletionVectorStorageType::PersistedAbsolute,
            ),
        ];
        for (ffi, kernel) in cases {
            assert_eq!(DeletionVectorStorageType::from(ffi), kernel);
        }
    }

    #[test]
    fn kernel_dv_storage_type_rejects_invalid_discriminant() {
        let err = KernelDvStorageType::try_from(42).expect_err("expected error");
        assert!(
            err.to_string()
                .contains("invalid deletion vector storage type"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn dv_descriptor_new_round_trips_fields() {
        // Smoke test that the FFI wrapper forwards fields to the kernel constructor
        // without dropping or reordering anything. Validation cases live with
        // `DeletionVectorDescriptor::try_new` in the kernel.
        let handle = dv_descriptor_new_impl(
            KernelDvStorageType::PersistedAbsolute,
            Ok("file:///tmp/dv.bin"),
            true,
            7,
            42,
            9,
        )
        .expect("descriptor should be valid");
        let descriptor = unsafe { handle.into_inner() };

        assert_eq!(
            descriptor.storage_type,
            DeletionVectorStorageType::PersistedAbsolute
        );
        assert_eq!(descriptor.path_or_inline_dv, "file:///tmp/dv.bin");
        assert_eq!(descriptor.offset, Some(7));
        assert_eq!(descriptor.size_in_bytes, 42);
        assert_eq!(descriptor.cardinality, 9);
    }

    #[test]
    fn dv_descriptor_new_surfaces_kernel_validation_error() {
        // Verify the FFI wrapper propagates errors raised by
        // `DeletionVectorDescriptor::try_new` instead of producing a handle.
        let err = dv_descriptor_new_impl(KernelDvStorageType::Inline, Ok("ABC"), true, 0, 4, 1)
            .err()
            .expect("expected validation error");
        assert!(err.to_string().contains("inline"), "unexpected: {err}");
    }

    #[test]
    fn dv_descriptor_map_insert_error_leaves_descriptor_freeable() {
        let mut map = DvDescriptorMap {
            inner: HashMap::new(),
        };
        let descriptor =
            dv_descriptor_new_impl(KernelDvStorageType::Inline, Ok("ABC"), false, 0, 4, 1)
                .expect("descriptor should be valid");

        let result = dv_descriptor_map_insert_impl(
            &mut map,
            Err(Error::generic("bad data file path")),
            descriptor.shallow_copy(),
        );

        assert!(
            result.is_err(),
            "insert should fail before consuming descriptor"
        );
        assert!(map.inner.is_empty());
        unsafe { free_dv_descriptor(descriptor) };
    }
}
