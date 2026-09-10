//! Reusable borrowed C FFI representations of Delta state.

use std::collections::HashMap;

use delta_kernel::actions::{
    CheckpointMetadata, DomainMetadata, Metadata, Protocol, SetTransaction, Sidecar,
};
use delta_kernel::crc::{try_new_file_size_histogram, FileSizeHistogram};
use delta_kernel::{DeltaResult, Error};

use crate::{KernelStringSlice, TryFromStringSlice};

/// Maps invalid pointer or array layouts to an error appropriate for the calling FFI API.
pub(crate) type InvalidInput = fn(String) -> Error;

/// Borrowed optional UTF-8 string.
#[repr(C)]
pub struct FfiOptionalString {
    /// Whether `value` is present.
    pub has_value: bool,
    /// Borrowed string value. Ignored when `has_value` is false.
    pub value: KernelStringSlice,
}

/// Borrowed optional signed 64-bit integer.
#[repr(C)]
pub struct FfiOptionalI64 {
    /// Whether `value` is present.
    pub has_value: bool,
    /// Integer value. Ignored when `has_value` is false.
    pub value: i64,
}

/// Borrowed optional unsigned 64-bit integer.
#[repr(C)]
pub struct FfiOptionalU64 {
    /// Whether `value` is present.
    pub has_value: bool,
    /// Integer value. Ignored when `has_value` is false.
    pub value: u64,
}

/// Borrowed array of UTF-8 strings.
#[repr(C)]
pub struct FfiStringArray {
    /// Pointer to `len` string slices, or null when `len` is zero.
    pub ptr: *const KernelStringSlice,
    /// Number of strings in the array.
    pub len: usize,
}

/// Borrowed optional array of UTF-8 strings.
#[repr(C)]
pub struct FfiOptionalStringArray {
    /// Whether the array is present. A present empty array differs from an absent array.
    pub has_value: bool,
    /// Borrowed array value. Ignored when `has_value` is false.
    pub value: FfiStringArray,
}

/// One borrowed UTF-8 map entry.
#[repr(C)]
pub struct FfiStringMapEntry {
    /// Entry key.
    pub key: KernelStringSlice,
    /// Entry value.
    pub value: KernelStringSlice,
}

/// Borrowed array of UTF-8 map entries.
#[repr(C)]
pub struct FfiStringMap {
    /// Pointer to `len` entries, or null when `len` is zero.
    pub ptr: *const FfiStringMapEntry,
    /// Number of entries.
    pub len: usize,
}

/// Borrowed optional UTF-8 map.
#[repr(C)]
pub struct FfiOptionalStringMap {
    /// Whether the map is present. A present empty map differs from an absent map.
    pub has_value: bool,
    /// Borrowed map value. Ignored when `has_value` is false.
    pub value: FfiStringMap,
}

/// Borrowed array of signed 64-bit integers.
#[repr(C)]
pub struct FfiI64Array {
    /// Pointer to `len` integers, or null when `len` is zero.
    pub ptr: *const i64,
    /// Number of integers.
    pub len: usize,
}

/// Borrowed Delta protocol state.
#[repr(C)]
pub struct FfiProtocol {
    /// Minimum reader protocol version.
    pub min_reader_version: i32,
    /// Minimum writer protocol version.
    pub min_writer_version: i32,
    /// Optional reader feature list.
    pub reader_features: FfiOptionalStringArray,
    /// Optional writer feature list.
    pub writer_features: FfiOptionalStringArray,
}

/// Borrowed Delta metadata state.
#[repr(C)]
pub struct FfiMetadata {
    /// Table identifier.
    pub id: KernelStringSlice,
    /// Optional table name.
    pub name: FfiOptionalString,
    /// Optional table description.
    pub description: FfiOptionalString,
    /// Data format provider.
    pub format_provider: KernelStringSlice,
    /// Data format options.
    pub format_options: FfiStringMap,
    /// Canonical Delta schema string.
    pub schema_string: KernelStringSlice,
    /// Logical partition column names.
    pub partition_columns: FfiStringArray,
    /// Optional metadata creation time in milliseconds since the Unix epoch.
    pub created_time: FfiOptionalI64,
    /// Table configuration entries.
    pub configuration: FfiStringMap,
}

/// Borrowed Delta set-transaction action.
#[repr(C)]
pub struct FfiSetTransaction {
    /// Application identifier.
    pub app_id: KernelStringSlice,
    /// Application-specific transaction version.
    pub version: i64,
    /// Optional last-updated time in milliseconds since the Unix epoch.
    pub last_updated: FfiOptionalI64,
}

/// Borrowed Delta domain-metadata action.
#[repr(C)]
pub struct FfiDomainMetadata {
    /// Domain identifier.
    pub domain: KernelStringSlice,
    /// Domain configuration payload.
    pub configuration: KernelStringSlice,
    /// Whether this action removes the domain.
    pub removed: bool,
}

/// Borrowed Delta checkpoint-metadata action.
#[repr(C)]
pub struct FfiCheckpointMetadata {
    /// Checkpoint version.
    pub version: i64,
    /// Optional action tags.
    pub tags: FfiOptionalStringMap,
}

/// Borrowed Delta checkpoint sidecar action.
#[repr(C)]
pub struct FfiSidecar {
    /// Sidecar path.
    pub path: KernelStringSlice,
    /// Sidecar size in bytes.
    pub size_in_bytes: i64,
    /// Sidecar modification time in milliseconds since the Unix epoch.
    pub modification_time: i64,
    /// Optional sidecar tags.
    pub tags: FfiOptionalStringMap,
}

/// Borrowed file-size histogram state.
#[repr(C)]
pub struct FfiFileSizeHistogram {
    /// Sorted lower boundary of every histogram bin.
    pub sorted_bin_boundaries: FfiI64Array,
    /// File count in every histogram bin.
    pub file_counts: FfiI64Array,
    /// Total bytes in every histogram bin.
    pub total_bytes: FfiI64Array,
}

/// Borrowed array of Delta checkpoint sidecar actions.
#[repr(C)]
pub struct FfiSidecarArray {
    /// Pointer to `len` actions, or null when `len` is zero.
    pub ptr: *const FfiSidecar,
    /// Number of actions.
    pub len: usize,
}

/// Borrowed array of Delta set-transaction actions.
#[repr(C)]
pub struct FfiSetTransactionArray {
    /// Pointer to `len` actions, or null when `len` is zero.
    pub ptr: *const FfiSetTransaction,
    /// Number of actions.
    pub len: usize,
}

/// Borrowed array of Delta domain-metadata actions.
#[repr(C)]
pub struct FfiDomainMetadataArray {
    /// Pointer to `len` actions, or null when `len` is zero.
    pub ptr: *const FfiDomainMetadata,
    /// Number of actions.
    pub len: usize,
}

pub(crate) fn optional_value<T>(
    has_value: bool,
    value: impl FnOnce() -> DeltaResult<T>,
) -> DeltaResult<Option<T>> {
    has_value.then(value).transpose()
}

pub(crate) unsafe fn raw_slice<'a, T>(
    ptr: *const T,
    len: usize,
    name: &str,
    invalid_input: InvalidInput,
) -> DeltaResult<&'a [T]> {
    if len == 0 {
        return Ok(&[]);
    }
    if ptr.is_null() {
        return Err(invalid_input(format!(
            "{name} pointer is null with length {len}"
        )));
    }
    Ok(unsafe { std::slice::from_raw_parts(ptr, len) })
}

pub(crate) unsafe fn optional_array<T, U>(
    has_value: bool,
    ptr: *const T,
    len: usize,
    name: &str,
    invalid_input: InvalidInput,
    map: impl FnMut(&T) -> DeltaResult<U>,
) -> DeltaResult<Option<Vec<U>>> {
    optional_value(has_value, || {
        unsafe { raw_slice(ptr, len, name, invalid_input) }?
            .iter()
            .map(map)
            .collect()
    })
}

pub(crate) unsafe fn string(value: &KernelStringSlice) -> DeltaResult<String> {
    let value: &str = unsafe { TryFromStringSlice::try_from_slice(value) }?;
    Ok(value.to_string())
}

pub(crate) unsafe fn optional_string(value: &FfiOptionalString) -> DeltaResult<Option<String>> {
    optional_value(value.has_value, || unsafe { string(&value.value) })
}

pub(crate) fn optional_i64(value: &FfiOptionalI64) -> Option<i64> {
    value.has_value.then_some(value.value)
}

pub(crate) unsafe fn strings(
    value: &FfiStringArray,
    invalid_input: InvalidInput,
) -> DeltaResult<Vec<String>> {
    unsafe { raw_slice(value.ptr, value.len, "string array", invalid_input) }?
        .iter()
        .map(|value| unsafe { string(value) })
        .collect()
}

pub(crate) unsafe fn optional_strings(
    value: &FfiOptionalStringArray,
    invalid_input: InvalidInput,
) -> DeltaResult<Option<Vec<String>>> {
    optional_value(value.has_value, || unsafe {
        strings(&value.value, invalid_input)
    })
}

pub(crate) unsafe fn string_map(
    value: &FfiStringMap,
    invalid_input: InvalidInput,
) -> DeltaResult<HashMap<String, String>> {
    let entries = unsafe { raw_slice(value.ptr, value.len, "string map", invalid_input) }?;
    let mut result = HashMap::with_capacity(entries.len());
    for entry in entries {
        let key = unsafe { string(&entry.key) }?;
        let value = unsafe { string(&entry.value) }?;
        if result.insert(key.clone(), value).is_some() {
            return Err(invalid_input(format!("duplicate map key: {key}")));
        }
    }
    Ok(result)
}

pub(crate) unsafe fn optional_string_map(
    value: &FfiOptionalStringMap,
    invalid_input: InvalidInput,
) -> DeltaResult<Option<HashMap<String, String>>> {
    optional_value(value.has_value, || unsafe {
        string_map(&value.value, invalid_input)
    })
}

pub(crate) unsafe fn protocol(
    value: &FfiProtocol,
    invalid_input: InvalidInput,
) -> DeltaResult<Protocol> {
    Protocol::try_new(
        value.min_reader_version,
        value.min_writer_version,
        unsafe { optional_strings(&value.reader_features, invalid_input) }?,
        unsafe { optional_strings(&value.writer_features, invalid_input) }?,
    )
}

pub(crate) unsafe fn metadata(
    value: &FfiMetadata,
    invalid_input: InvalidInput,
) -> DeltaResult<Metadata> {
    Ok(Metadata::from_parts(
        unsafe { string(&value.id) }?,
        unsafe { optional_string(&value.name) }?,
        unsafe { optional_string(&value.description) }?,
        unsafe { string(&value.format_provider) }?,
        unsafe { string_map(&value.format_options, invalid_input) }?,
        unsafe { string(&value.schema_string) }?,
        unsafe { strings(&value.partition_columns, invalid_input) }?,
        optional_i64(&value.created_time),
        unsafe { string_map(&value.configuration, invalid_input) }?,
    ))
}

pub(crate) unsafe fn set_transaction(value: &FfiSetTransaction) -> DeltaResult<SetTransaction> {
    Ok(SetTransaction::new(
        unsafe { string(&value.app_id) }?,
        value.version,
        optional_i64(&value.last_updated),
    ))
}

pub(crate) unsafe fn domain_metadata(value: &FfiDomainMetadata) -> DeltaResult<DomainMetadata> {
    let domain = unsafe { string(&value.domain) }?;
    let configuration = unsafe { string(&value.configuration) }?;
    Ok(if value.removed {
        DomainMetadata::remove(domain, configuration)
    } else {
        DomainMetadata::new(domain, configuration)
    })
}

pub(crate) unsafe fn checkpoint_metadata(
    value: &FfiCheckpointMetadata,
    invalid_input: InvalidInput,
) -> DeltaResult<CheckpointMetadata> {
    Ok(CheckpointMetadata::new(value.version, unsafe {
        optional_string_map(&value.tags, invalid_input)
    }?))
}

pub(crate) unsafe fn sidecar(
    value: &FfiSidecar,
    invalid_input: InvalidInput,
) -> DeltaResult<Sidecar> {
    Ok(Sidecar::new(
        unsafe { string(&value.path) }?,
        value.size_in_bytes,
        value.modification_time,
        unsafe { optional_string_map(&value.tags, invalid_input) }?,
    ))
}

pub(crate) unsafe fn file_size_histogram(
    value: &FfiFileSizeHistogram,
    invalid_input: InvalidInput,
) -> DeltaResult<FileSizeHistogram> {
    try_new_file_size_histogram(
        unsafe {
            raw_slice(
                value.sorted_bin_boundaries.ptr,
                value.sorted_bin_boundaries.len,
                "integer array",
                invalid_input,
            )
        }?
        .to_vec(),
        unsafe {
            raw_slice(
                value.file_counts.ptr,
                value.file_counts.len,
                "integer array",
                invalid_input,
            )
        }?
        .to_vec(),
        unsafe {
            raw_slice(
                value.total_bytes.ptr,
                value.total_bytes.len,
                "integer array",
                invalid_input,
            )
        }?
        .to_vec(),
    )
}
