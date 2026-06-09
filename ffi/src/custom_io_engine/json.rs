//! Custom [`JsonHandler`] driven by an engine-supplied vtable.
//!
//! Reads go through the column-materialization wire format: the engine parses
//! JSON files into its own batch representation and hands the kernel opaque
//! batch handles. For each batch the kernel issues a `materialize_columns`
//! upcall (via the `arrow_bridge` module) to pull the requested leaf columns
//! into Rust-readable [`ColumnBuffers`], assembles an `ArrowEngineData`
//! matching the physical schema, and lets the default Arrow evaluation
//! handler take over. Writes serialize the kernel's Arrow batches to the
//! Arrow C Data Interface and hand them to the engine to persist.
//!
//! ## Column-materialization protocol
//!
//! When converting a batch, the kernel:
//! 1. Builds a [`VisitRowsRequest`] with the leaf column paths it wants.
//! 2. Calls `materialize_columns` -> [`VisitRowsResult`] with one [`ColumnBuffers`] per path.
//! 3. Converts the buffers into Arrow arrays (see the `arrow_bridge` module).
//! 4. Calls `free_columns` to release the engine's materialization buffers.
//!
//! Buffer ownership: the engine allocates, the kernel borrows during the
//! conversion, then asks the engine to free. This keeps the per-batch FFI
//! cost to a single upcall while leaving memory ownership unambiguous.

use std::ffi::c_void;
use std::sync::Arc;

use delta_kernel::schema::SchemaRef;
use delta_kernel::{
    DeltaResult, EngineData, Error, FileDataReadResultIterator, FileMeta as KernelFileMeta,
    FilteredEngineData, JsonHandler, PredicateRef, ScopedDeltaResultIterator,
};
use url::Url;

use crate::custom_io_engine::{to_ffi_file_metas, CustomReadResult};
use crate::engine_data::ArrowFFIData;
use crate::handle::Handle;
use crate::{KernelStringSlice, SharedSchema};

// ============================================================================
// Column-materialization wire format
// ============================================================================

/// Tagged kind for a single materialized column. Maps directly to the
/// kernel's primitive types; nested types carry sub-columns in the
/// `children` field of [`ColumnBuffers`].
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnTypeTag {
    Bool = 0,
    Byte = 1,
    Short = 2,
    Int = 3,
    Long = 4,
    Float = 5,
    Double = 6,
    /// Days since epoch, i32.
    Date = 7,
    /// Microseconds since epoch, i64.
    Timestamp = 8,
    /// Microseconds since epoch (no timezone), i64.
    TimestampNtz = 9,
    /// 16-byte two's-complement integer; precision/scale carried separately.
    Decimal = 10,
    String = 11,
    Binary = 12,
    /// Variable-length list. `primary` holds `i32 * (row_count + 1)` offsets;
    /// `children` holds exactly one child column for the element type.
    List = 13,
    /// String->X map. `primary` holds offsets; `children` holds exactly two
    /// child columns: `[keys, values]`.
    Map = 14,
    /// Struct of N fields. `children` holds N child columns in field order.
    Struct = 15,
    /// Sentinel used by error paths when no payload is produced.
    Null = 255,
}

/// A single column's materialized representation, returned by the engine for
/// one requested column path. The layout is intentionally Arrow-shaped
/// (offsets + concatenated bytes for variable-width types) but does not
/// require an Arrow library on either side.
#[repr(C)]
#[derive(Debug)]
pub struct ColumnBuffers {
    pub type_tag: u8,
    /// Number of logical rows in this column.
    pub row_count: usize,
    /// `ceil(row_count / 8)` bytes, LSB-first, `1` = valid (Arrow convention).
    /// Null when no nulls are present.
    pub null_bitmap: *const u8,
    pub null_bitmap_len: usize,
    /// Type-dependent primary buffer:
    /// - Primitives: `row_count * sizeof(T)` packed values, native endian.
    /// - String / Binary / List / Map: `(row_count + 1) * 4` bytes of i32 offsets.
    /// - Struct: unused.
    pub primary: *const u8,
    pub primary_len: usize,
    /// Type-dependent auxiliary buffer:
    /// - String / Binary: concatenated value bytes (last offset = total len).
    /// - All other types: unused.
    pub aux: *const u8,
    pub aux_len: usize,
    /// Decimal-only precision + scale.
    pub decimal_precision: u8,
    pub decimal_scale: u8,
    /// Nested-type child columns: List = 1, Map = 2, Struct = N, else null.
    pub children: *const ColumnBuffers,
    pub children_len: usize,
}

// SAFETY: pointer fields are opaque to Rust; the engine owns the underlying
// memory and keeps it valid for the duration of a `materialize_columns` call.
unsafe impl Send for ColumnBuffers {}
unsafe impl Sync for ColumnBuffers {}

/// Argument to `materialize_columns`: the batch and the leaf paths to extract.
#[repr(C)]
pub struct VisitRowsRequest {
    /// Engine-managed batch handle previously returned by `iter_next`.
    pub batch_handle: *mut c_void,
    /// Number of column paths.
    pub paths_len: usize,
    /// Column paths in dotted form, e.g. `"add.deletionVector.storageType"`.
    pub paths: *const KernelStringSlice,
}

/// Result of `materialize_columns`: one [`ColumnBuffers`] per requested path,
/// in the same order.
#[repr(C)]
pub struct VisitRowsResult {
    /// Array of `columns_len` column buffers.
    pub columns: *const ColumnBuffers,
    pub columns_len: usize,
    /// `0` on success; non-zero means an engine error and `columns` is ignored.
    pub error: u32,
}

/// Function-pointer table for materializing columns out of, and releasing,
/// engine-owned JSON batches.
#[repr(C)]
pub struct CustomEngineDataCallbacks {
    /// Materialize the requested column paths into Rust-readable memory.
    pub materialize_columns: extern "C" fn(
        engine_state: *mut c_void,
        request: *const VisitRowsRequest,
        out: *mut VisitRowsResult,
    ),
    /// Release the buffers from a previous `materialize_columns` call.
    pub free_columns:
        extern "C" fn(engine_state: *mut c_void, columns: *const ColumnBuffers, columns_len: usize),
    /// Free a batch handle when its wrapper drops.
    pub free_batch: extern "C" fn(engine_state: *mut c_void, batch_handle: *mut c_void),
}

// SAFETY: plain function pointers; the engine owns any referenced state.
unsafe impl Send for CustomEngineDataCallbacks {}
unsafe impl Sync for CustomEngineDataCallbacks {}

// ============================================================================
// CustomEngineData -- carrier for an engine-owned JSON batch
// ============================================================================

/// Shared state carried by a [`CustomEngineData`] so it can upcall for column
/// materialization and release the batch on drop.
pub(crate) struct EngineDataState {
    pub(crate) callbacks: CustomEngineDataCallbacks,
    pub(crate) engine_state: *mut c_void,
}

// SAFETY: same justification as the vtable structs.
unsafe impl Send for EngineDataState {}
unsafe impl Sync for EngineDataState {}

/// A transient handle to an engine-owned JSON batch. Not an [`EngineData`]
/// itself: the kernel converts it to an `ArrowEngineData` via the
/// `arrow_bridge` module and drops it (freeing the engine batch) in the same
/// step. Exposes its fields to the bridge module.
pub struct CustomEngineData {
    pub(crate) batch_handle: *mut c_void,
    pub(crate) state: Arc<EngineDataState>,
}

impl Drop for CustomEngineData {
    fn drop(&mut self) {
        if !self.batch_handle.is_null() {
            (self.state.callbacks.free_batch)(self.state.engine_state, self.batch_handle);
        }
    }
}

// ============================================================================
// JSON handler vtable
// ============================================================================

/// Result of [`CustomJsonCallbacks::iter_next`].
#[repr(C)]
pub struct CustomEngineDataResult {
    /// Engine-managed batch handle. Null iff `done == true`.
    pub batch: *mut c_void,
    /// Set to `true` when no more batches remain.
    pub done: bool,
    /// `0` on success.
    pub error: u32,
}

/// Function-pointer table the connector supplies to drive the JSON handler.
#[repr(C)]
pub struct CustomJsonCallbacks {
    /// Engine-managed opaque pointer threaded through every callback.
    pub engine_state: *mut c_void,
    /// Begin reading the given JSON files. The engine **consumes** the
    /// `physical_schema` handle exactly once (via `free_schema` on the FFI
    /// side, or [`Handle::into_inner`] when implemented in Rust). On success
    /// the engine writes a non-null iterator state.
    pub read_json_files: extern "C" fn(
        engine_state: *mut c_void,
        files: *const crate::engine_funcs::FileMeta,
        files_len: usize,
        physical_schema: Handle<SharedSchema>,
        out: *mut CustomReadResult,
    ),
    /// Pump the next batch from a file-read iterator.
    pub iter_next: extern "C" fn(
        engine_state: *mut c_void,
        iter_state: *mut c_void,
        out: *mut CustomEngineDataResult,
    ),
    /// Release a file-read iterator state.
    pub iter_free: extern "C" fn(engine_state: *mut c_void, iter_state: *mut c_void),
    /// Atomically write a single newline-delimited JSON file from the supplied
    /// Arrow batches. The engine serializes each batch's rows to JSON objects.
    /// If `overwrite` is false and the file exists, the engine must report a
    /// non-zero status. The `batches` array is valid only for the duration of
    /// this call; the engine must consume each batch in place (per the Arrow C
    /// Data Interface, releasing each array it imports).
    pub write_json_file: extern "C" fn(
        engine_state: *mut c_void,
        path: KernelStringSlice,
        batches: *mut ArrowFFIData,
        batches_len: usize,
        overwrite: bool,
        out_error: *mut u32,
    ),
    /// Engine-data callbacks for batches this handler produces.
    pub engine_data_callbacks: CustomEngineDataCallbacks,
    /// Free the engine state when the last handler reference drops.
    pub free_engine_state: extern "C" fn(engine_state: *mut c_void),
}

// SAFETY: plain function pointers plus an opaque engine pointer.
unsafe impl Send for CustomJsonCallbacks {}
unsafe impl Sync for CustomJsonCallbacks {}

/// Shared state for the JSON handler. The engine state is freed exactly once,
/// when the last `Arc` reference (handler + any in-flight batch) drops.
struct JsonHandlerState {
    callbacks: CustomJsonCallbacks,
    /// Shared with every [`CustomEngineData`] this handler produces so the
    /// engine state outlives in-flight batches.
    engine_data_state: Arc<EngineDataState>,
}

impl Drop for JsonHandlerState {
    fn drop(&mut self) {
        (self.callbacks.free_engine_state)(self.callbacks.engine_state);
    }
}

/// [`JsonHandler`] that forwards reads and writes to a connector-supplied
/// vtable, bridging engine-owned batches to Arrow on the read path.
pub struct CustomJsonHandler {
    state: Arc<JsonHandlerState>,
}

impl CustomJsonHandler {
    pub(crate) fn new(callbacks: CustomJsonCallbacks) -> Self {
        let engine_state = callbacks.engine_state;
        let engine_data_callbacks = CustomEngineDataCallbacks {
            ..callbacks.engine_data_callbacks
        };
        let state = Arc::new(JsonHandlerState {
            engine_data_state: Arc::new(EngineDataState {
                callbacks: engine_data_callbacks,
                engine_state,
            }),
            callbacks,
        });
        Self { state }
    }
}

impl JsonHandler for CustomJsonHandler {
    fn parse_json(
        &self,
        _json_strings: Box<dyn EngineData>,
        _output_schema: SchemaRef,
    ) -> DeltaResult<Box<dyn EngineData>> {
        // Log replay reads JSON via `read_json_files`; `parse_json` only fires
        // when the kernel already has JSON strings in memory. Not yet wired.
        Err(Error::generic(
            "CustomJsonHandler::parse_json is not supported; log replay uses read_json_files.",
        ))
    }

    fn read_json_files(
        &self,
        files: &[KernelFileMeta],
        physical_schema: SchemaRef,
        _predicate: Option<PredicateRef>,
    ) -> DeltaResult<FileDataReadResultIterator> {
        let ffi_files = to_ffi_file_metas(files)?;
        // Hand the engine its own clone of the schema; keep the original for
        // the per-batch Arrow conversion. The engine consumes the handle.
        let schema_handle: Handle<SharedSchema> = physical_schema.clone().into();
        let mut result = CustomReadResult {
            iter_state: std::ptr::null_mut(),
            error: 0,
        };
        (self.state.callbacks.read_json_files)(
            self.state.callbacks.engine_state,
            ffi_files.as_ptr(),
            ffi_files.len(),
            schema_handle,
            &mut result,
        );
        if result.error != 0 {
            return Err(Error::generic(format!(
                "CustomJsonHandler::read_json_files signalled error code {}",
                result.error
            )));
        }
        if result.iter_state.is_null() {
            return Err(Error::generic(
                "CustomJsonHandler::read_json_files returned a null iterator state on success",
            ));
        }
        Ok(Box::new(CustomJsonIter {
            iter_state: result.iter_state,
            handler: self.state.clone(),
            physical_schema,
            done: false,
        }))
    }

    fn write_json_file(
        &self,
        path: &Url,
        data: ScopedDeltaResultIterator<'_, FilteredEngineData>,
        overwrite: bool,
    ) -> DeltaResult<()> {
        // Apply each batch's selection vector, then export the filtered rows to
        // the Arrow C Data Interface for the engine to serialize and persist.
        let mut batches: Vec<ArrowFFIData> = Vec::new();
        for filtered in data {
            let engine_data = filtered?.apply_selection_vector()?;
            batches.push(ArrowFFIData::try_from_engine_data(engine_data)?);
        }

        // SAFETY: `path.as_str()` outlives this synchronous upcall.
        let slice = unsafe { KernelStringSlice::new_unsafe(path.as_str()) };
        let mut error: u32 = 0;
        (self.state.callbacks.write_json_file)(
            self.state.callbacks.engine_state,
            slice,
            batches.as_mut_ptr(),
            batches.len(),
            overwrite,
            &mut error,
        );
        // `batches` drops here. Each `ArrowFFIData`'s release callback runs
        // unless the engine already consumed (and nulled) it, matching the
        // `free_arrow_ffi_data` contract -- no double free, no leak.
        if error != 0 {
            return Err(Error::generic(format!(
                "CustomJsonHandler::write_json_file signalled error code {error} for {path}"
            )));
        }
        Ok(())
    }
}

/// Iterator over JSON-derived batches. Pumps the engine's `iter_next`, wraps
/// each batch handle in a [`CustomEngineData`], and converts it to an
/// `ArrowEngineData` matching `physical_schema` so the kernel's default
/// evaluation handler can operate on it.
struct CustomJsonIter {
    iter_state: *mut c_void,
    handler: Arc<JsonHandlerState>,
    physical_schema: SchemaRef,
    done: bool,
}

// SAFETY: same justification as `CustomJsonCallbacks`.
unsafe impl Send for CustomJsonIter {}

impl Drop for CustomJsonIter {
    fn drop(&mut self) {
        if !self.iter_state.is_null() {
            (self.handler.callbacks.iter_free)(
                self.handler.callbacks.engine_state,
                self.iter_state,
            );
        }
    }
}

impl Iterator for CustomJsonIter {
    type Item = DeltaResult<Box<dyn EngineData>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let mut res = CustomEngineDataResult {
            batch: std::ptr::null_mut(),
            done: false,
            error: 0,
        };
        (self.handler.callbacks.iter_next)(
            self.handler.callbacks.engine_state,
            self.iter_state,
            &mut res,
        );
        if res.error != 0 {
            self.done = true;
            return Some(Err(Error::generic(format!(
                "CustomJsonIter::next signalled error code {}",
                res.error
            ))));
        }
        if res.done {
            self.done = true;
            return None;
        }
        if res.batch.is_null() {
            self.done = true;
            return Some(Err(Error::generic(
                "CustomJsonIter::next returned a null batch with done=false",
            )));
        }
        // Build a transient carrier so the bridge can issue the
        // materialize_columns upcall, then drop it to free the engine batch.
        let batch = CustomEngineData {
            batch_handle: res.batch,
            state: self.handler.engine_data_state.clone(),
        };
        Some(
            crate::arrow_bridge::custom_engine_data_to_arrow_engine_data(
                &batch,
                &self.physical_schema,
            ),
        )
    }
}
