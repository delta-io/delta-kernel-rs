//! FFI surface for deletion-vector update transactions.
//!
//! Three pieces:
//! 1. An opaque [`DvDescriptorMap`] (path -> [`DeletionVectorDescriptor`]) that the engine
//!    populates with the DVs it wants to install.
//! 2. Helpers to build [`DeletionVectorDescriptor`] handles -- either by writing fresh DV files via
//!    the kernel ([`transaction_write_deletion_vector`]) or by constructing them manually from
//!    engine-authored DV file metadata ([`dv_descriptor_new`]).
//! 3. [`transaction_update_deletion_vectors`] consumes the map, drains the supplied scan-metadata
//!    iterator, and stages remove/add action pairs on the transaction.

use std::collections::HashMap;

use delta_kernel::actions::deletion_vector::{DeletionVectorDescriptor, DeletionVectorStorageType};
use delta_kernel::transaction::Transaction;
use delta_kernel::{DeltaResult, Error};
use delta_kernel_ffi_macros::handle_descriptor;
use url::Url;

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

/// Mutable handle for an [`DvDescriptorMap`].
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
///   base85-encoded UUID held in `path_or_inline_dv`
/// - `Inline` (`'i'`): the deletion vector is stored inline in the log
/// - `PersistedAbsolute` (`'p'`): the DV is stored at the absolute URL given by `path_or_inline_dv`
#[repr(i32)]
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

/// Construct a [`DeletionVectorDescriptor`] from raw fields, for engines that author DV
/// files themselves and want to install them via [`transaction_update_deletion_vectors`].
/// Engines that delegate the file format to kernel should call
/// [`transaction_write_deletion_vector`] instead.
///
/// Per the Delta protocol "Deletion Vector Descriptor Schema":
/// - `Inline` descriptors MUST NOT carry an offset; pass `has_offset = false`.
/// - `PersistedRelative` descriptors carry the random prefix followed by a 20-character
///   base85-encoded UUID, so `path_or_inline_dv` MUST be at least 20 characters long.
/// - `PersistedAbsolute` descriptors MUST contain a parseable URL.
/// - `size_in_bytes` MUST be non-negative.
/// - `cardinality` MUST be non-negative.
///
/// For persisted DVs, `offset` is the byte offset within the DV file at which the DV's
/// 4-byte big-endian size prefix begins. A single-DV file produced by
/// [`transaction_write_deletion_vector`] uses offset `1` (skipping the version byte).
///
/// # Safety
///
/// Caller must pass valid string slice and engine handle.
#[no_mangle]
pub unsafe extern "C" fn dv_descriptor_new(
    storage_type: KernelDvStorageType,
    path_or_inline_dv: KernelStringSlice,
    has_offset: bool,
    offset: i32,
    size_in_bytes: i32,
    cardinality: i64,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveDvDescriptor>> {
    let engine = unsafe { engine.as_ref() };
    let path = unsafe { TryFromStringSlice::try_from_slice(&path_or_inline_dv) };
    dv_descriptor_new_impl(
        storage_type,
        path,
        has_offset,
        offset,
        size_in_bytes,
        cardinality,
    )
    .into_extern_result(&engine)
}

fn dv_descriptor_new_impl(
    storage_type: KernelDvStorageType,
    path: DeltaResult<&str>,
    has_offset: bool,
    offset: i32,
    size_in_bytes: i32,
    cardinality: i64,
) -> DeltaResult<Handle<ExclusiveDvDescriptor>> {
    if size_in_bytes < 0 {
        return Err(Error::generic(
            "deletion vector size_in_bytes must be non-negative",
        ));
    }
    if cardinality < 0 {
        return Err(Error::generic(
            "deletion vector cardinality must be non-negative",
        ));
    }
    let path = path?.to_string();
    match storage_type {
        KernelDvStorageType::Inline => {
            if has_offset {
                return Err(Error::generic(
                    "inline deletion vectors must not carry an offset",
                ));
            }
        }
        KernelDvStorageType::PersistedRelative => {
            if path.len() < 20 {
                return Err(Error::generic(
                    "persisted-relative DV path must be at least 20 chars (z85-encoded uuid)",
                ));
            }
        }
        KernelDvStorageType::PersistedAbsolute => {
            Url::parse(&path).map_err(|e| {
                Error::generic(format!(
                    "persisted-absolute DV path must parse as a URL: {e}"
                ))
            })?;
        }
    }
    Ok(Box::new(DeletionVectorDescriptor {
        storage_type: storage_type.into(),
        path_or_inline_dv: path,
        offset: has_offset.then_some(offset),
        size_in_bytes,
        cardinality,
    })
    .into())
}

/// Frame raw RoaringTreemap bytes into a Delta deletion-vector file, write it via the
/// engine's storage handler, and return an owned [`DeletionVectorDescriptor`] handle.
///
/// `roaring_bytes` is a portable 64-bit RoaringTreemap (the format produced by
/// `RoaringTreemap::serialize_into` in the Rust crate, or `RoaringBitmap.serialize` with
/// the portable variant in other languages). Native (non-portable) roaring serialization
/// is reserved by the spec and will round-trip-fail when read back. The kernel adds the
/// Delta DV framing (version byte, big-endian size prefix, magic number, big-endian
/// CRC32) and chooses the file path.
///
/// `random_prefix` is treated as a single path component (no leading or trailing
/// slashes). Pass an empty slice to write at the table root. A short non-empty prefix
/// can help spread load across object-store partitions.
///
/// The kernel does not verify `cardinality` against the bitmap; passing a wrong value
/// produces a descriptor with incorrect cardinality.
///
/// The returned descriptor has [`KernelDvStorageType::PersistedRelative`] storage type
/// and must be released either by [`free_dv_descriptor`] or by inserting it into a map
/// via [`dv_descriptor_map_insert`].
///
/// # Safety
///
/// Caller must pass valid handles. `roaring_bytes` must point to at least
/// `roaring_bytes_len` initialized bytes (or be null when `roaring_bytes_len == 0`).
/// `random_prefix` must be a valid (possibly empty) string slice. The transaction
/// handle is borrowed and remains valid after this call; the caller must follow up
/// with `commit` or `free_transaction`.
#[no_mangle]
pub unsafe extern "C" fn transaction_write_deletion_vector(
    txn: Handle<ExclusiveTransaction>,
    engine: Handle<SharedExternEngine>,
    roaring_bytes: *const u8,
    roaring_bytes_len: usize,
    cardinality: i64,
    random_prefix: KernelStringSlice,
) -> ExternResult<Handle<ExclusiveDvDescriptor>> {
    let extern_engine = unsafe { engine.as_ref() };
    // The kernel helper takes &self, so we only need shared access to the transaction.
    let txn_ref: &Transaction = unsafe { txn.as_ref() };
    let prefix = unsafe { TryFromStringSlice::try_from_slice(&random_prefix) };
    // SAFETY: caller's contract requires `roaring_bytes` to point to at least
    // `roaring_bytes_len` initialized bytes when not null; null is treated as empty.
    let bytes_slice: &[u8] = if roaring_bytes.is_null() || roaring_bytes_len == 0 {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(roaring_bytes, roaring_bytes_len) }
    };
    transaction_write_deletion_vector_impl(txn_ref, extern_engine, bytes_slice, cardinality, prefix)
        .into_extern_result(&extern_engine)
}

fn transaction_write_deletion_vector_impl(
    txn: &Transaction,
    extern_engine: &dyn crate::ExternEngine,
    roaring_bytes: &[u8],
    cardinality: i64,
    random_prefix: DeltaResult<&str>,
) -> DeltaResult<Handle<ExclusiveDvDescriptor>> {
    let engine = extern_engine.engine();
    let descriptor =
        txn.write_deletion_vector(engine.as_ref(), roaring_bytes, cardinality, random_prefix?)?;
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
) -> ExternResult<()> {
    let map_ref = unsafe { map.as_mut() };
    let engine_ref = unsafe { engine.as_ref() };
    // Parse the path BEFORE taking ownership of the descriptor: if parsing fails the
    // descriptor must remain valid so the caller can free it (otherwise we get a UAF
    // when they retry or clean up).
    let path_result = unsafe { TryFromStringSlice::try_from_slice(&data_file_path) };
    let result: DeltaResult<()> = (|| {
        let path: &str = path_result?;
        let owned_path = path.to_string();
        // Now take ownership of the descriptor.
        let descriptor = unsafe { descriptor.into_inner() };
        map_ref.inner.insert(owned_path, *descriptor);
        Ok(())
    })();
    result.into_extern_result(&engine_ref)
}

// =============================================================================
// Apply DV updates to a transaction
// =============================================================================

/// Stage deletion-vector update actions on the transaction. For every entry in `dv_map`
/// the kernel emits a Remove + Add action pair on commit (the Add carries the new DV
/// descriptor; row-level statistics from the original Add are preserved).
///
/// Consumes `dv_map`. Drains `scan_iter`: after this call the iterator handle yields
/// `false` from `scan_metadata_next`. The engine should pass an iterator that covers
/// at least every file path mentioned in the map; extra files are ignored. If the map
/// references a path that does not appear in the iterator, the call returns an error
/// and the transaction is left unchanged.
///
/// # Safety
///
/// Caller must pass valid handles. The transaction handle is borrowed in place and
/// remains valid after this call; the caller is expected to follow with `commit` (or
/// `free_transaction`) on the same handle. The DV map handle is consumed and must not
/// be used or freed after this call. The scan iterator handle is borrowed but drained
/// (the iterator's mutex is held for the duration of this call), and afterwards may
/// only be passed to `free_scan_metadata_iter`.
#[no_mangle]
pub unsafe extern "C" fn transaction_update_deletion_vectors(
    mut txn: Handle<ExclusiveTransaction>,
    dv_map: Handle<ExclusiveDvDescriptorMap>,
    scan_iter: Handle<SharedScanMetadataIterator>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<()> {
    let txn_ref = unsafe { txn.as_mut() };
    let dv_map = unsafe { dv_map.into_inner() };
    let scan_iter_ref = unsafe { scan_iter.as_ref() };
    let engine_ref = unsafe { engine.as_ref() };
    transaction_update_deletion_vectors_impl(txn_ref, *dv_map, scan_iter_ref)
        .into_extern_result(&engine_ref)
}

fn transaction_update_deletion_vectors_impl(
    txn: &mut Transaction,
    dv_map: DvDescriptorMap,
    scan_iter: &crate::scan::ScanMetadataIterator,
) -> DeltaResult<()> {
    let mut guard = scan_iter.lock_iter()?;
    // Adapt &mut Box<dyn Iterator<ScanMetadata>> -> &mut dyn Iterator ->
    // Iterator<FilteredEngineData>. The &mut adapter borrows from `guard`, which we keep alive
    // for the call.
    let scan_meta_iter: &mut (dyn Iterator<Item = DeltaResult<delta_kernel::scan::ScanMetadata>>
              + Send) = &mut **guard;
    let files_iter = Transaction::scan_metadata_to_engine_data(scan_meta_iter);
    txn.update_deletion_vectors(dv_map.inner, files_iter)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify every `KernelDvStorageType` variant maps to the matching kernel storage type.
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

    /// `dv_descriptor_new_impl` rejects an inline descriptor with `has_offset = true`.
    #[test]
    fn dv_descriptor_new_rejects_inline_with_offset() {
        let err = dv_descriptor_new_impl(KernelDvStorageType::Inline, Ok("ABC"), true, 0, 4, 1)
            .err()
            .expect("expected error");
        assert!(err.to_string().contains("inline"), "unexpected: {err}");
    }

    /// `dv_descriptor_new_impl` rejects a persisted-relative descriptor with too short a path.
    #[test]
    fn dv_descriptor_new_rejects_short_relative_path() {
        let err = dv_descriptor_new_impl(
            KernelDvStorageType::PersistedRelative,
            Ok("short"),
            true,
            1,
            4,
            1,
        )
        .err()
        .expect("expected error");
        assert!(err.to_string().contains("20 chars"), "unexpected: {err}");
    }

    /// `dv_descriptor_new_impl` rejects a persisted-absolute descriptor with a non-URL path.
    #[test]
    fn dv_descriptor_new_rejects_non_url_absolute_path() {
        let err = dv_descriptor_new_impl(
            KernelDvStorageType::PersistedAbsolute,
            Ok("not a url"),
            true,
            1,
            4,
            1,
        )
        .err()
        .expect("expected error");
        assert!(err.to_string().contains("URL"), "unexpected: {err}");
    }

    /// `dv_descriptor_new_impl` rejects negative size and cardinality.
    #[test]
    fn dv_descriptor_new_rejects_negative_size_and_cardinality() {
        let err = dv_descriptor_new_impl(KernelDvStorageType::Inline, Ok("ABC"), false, 0, -1, 0)
            .err()
            .expect("expected error");
        assert!(
            err.to_string().contains("size_in_bytes"),
            "unexpected: {err}"
        );

        let err = dv_descriptor_new_impl(KernelDvStorageType::Inline, Ok("ABC"), false, 0, 4, -1)
            .err()
            .expect("expected error");
        assert!(err.to_string().contains("cardinality"), "unexpected: {err}");
    }
}
