//! Architectural skeleton for the remaining engine handlers (JSON, Storage,
//! Evaluation) plumbed through the callback engine. Pairs with
//! [`crate::callback_engine`], which already exposes `JavaParquetCallbacks`.
//!
//! ============================================================================
//!  Why this exists
//! ============================================================================
//!
//! The parquet handler escapes the hardest piece of the engine trait: the
//! kernel never inspects parquet output, so an opaque pass-through `EngineData`
//! is enough. Every other handler that **produces** `EngineData` has a
//! different burden -- the kernel calls `visit_rows` on the result.
//!
//! A real Java implementation needs:
//!
//! 1. A new `EngineData` impl ([`JavaEngineData`]) that holds an opaque Java handle and supports
//!    `visit_rows` by upcalling into Java for column data.
//! 2. A column-materialization wire format ([`ColumnBuffers`], [`ColumnTypeTag`]) that lets Java
//!    return per-column null bitmaps + typed value buffers without depending on Arrow.
//! 3. Per-handler vtables ([`JavaJsonCallbacks`], [`JavaStorageCallbacks`],
//!    [`JavaEvaluationCallbacks`]) that mirror the existing parquet pattern.
//! 4. A factory ([`make_callback_engine_full`]) that accepts optional handler vtables -- a null
//!    vtable means "fall back to the inner engine for this handler". The existing
//!    `make_callback_engine` is a thin wrapper around this with everything-but-parquet null.
//!
//! All implementations in this file are **stubs**: they wire the FFI surface
//! and define the trait shape, but every method that would produce real data
//! returns `Error::generic("not yet implemented")`. Filling them in is a
//! follow-up effort -- ~600 LOC each for JSON and Storage, ~1500-2000 for
//! Evaluation depending on expression vocabulary.
//!
//! ============================================================================
//!  Column-materialization protocol
//! ============================================================================
//!
//! When the kernel calls `JavaEngineData::visit_rows(column_paths, visitor)`:
//!
//! 1. Rust builds a [`VisitRowsRequest`] with the batch handle and the column paths the visitor
//!    wants (e.g. `["add.path", "add.size", "add.deletionVector.storageType"]`).
//! 2. Rust calls the engine's `materialize_columns` callback (in
//!    [`JavaJsonCallbacks::engine_data_callbacks`]).
//! 3. Java navigates its parsed batch, extracts the requested columns, and writes a
//!    [`ColumnBuffers`] for each one into Rust-readable memory.
//! 4. Rust wraps each `ColumnBuffers` in a [`GetData`]-implementing adapter, builds a `&[&dyn
//!    GetData]`, and invokes `visitor.visit(row_count, &adapters)`.
//! 5. After `visit` returns, Rust calls `free_columns` to let Java release the materialization
//!    buffers.
//!
//! Buffer ownership: Java allocates, Rust borrows during the visit call,
//! then asks Java to free. This avoids per-cell FFI cost (one upcall per
//! batch) while keeping memory ownership clear.
//!
//! See `~/.claude/notes/java-engine-trait-upcalls.md` for the full design
//! discussion and the option-2 deep dive.

use std::ffi::c_void;
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use delta_kernel::engine_data::{GetData, ListItem, MapItem, RowVisitor, StringArrayAccessor};
use delta_kernel::expressions::{ArrayData, ColumnName};
use delta_kernel::schema::{DataType, SchemaRef};
use delta_kernel::{
    DeltaResult, EngineData, Error, EvaluationHandler, ExpressionEvaluator, ExpressionRef,
    FileDataReadResultIterator, FileMeta as KernelFileMeta, FileSlice, FilteredEngineData,
    JsonHandler, ParquetHandler, PredicateEvaluator, PredicateRef, StorageHandler,
};
use url::Url;

use crate::callback_engine::{JavaParquetCallbacks, JavaParquetHandler};
use crate::engine_funcs::FileMeta;
use crate::handle::Handle;
use crate::{
    AllocateErrorFn, ExternResult, IntoExternResult, KernelStringSlice, SharedExternEngine,
};

// ============================================================================
// Column materialization wire format
// ============================================================================

/// Tagged kind for a single materialized column. Maps directly to the kernel's
/// [`DataType`] primitives; nested types use the `children` field of
/// [`ColumnBuffers`] to hold sub-columns.
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
    /// Variable-length list. `primary` holds offsets `i32 * (row_count + 1)`;
    /// `children` holds exactly one child column for the element type.
    List = 13,
    /// String->X map. `primary` holds offsets; `children` holds exactly two
    /// child columns: `[keys: ColumnBuffers, values: ColumnBuffers]`.
    Map = 14,
    /// Struct of N fields. `children` holds N child columns in field order.
    Struct = 15,
    /// Sentinel used by error paths when no payload is produced.
    Null = 255,
}

/// Single column's materialized representation, returned by Java for one
/// requested column path. Layout is intentionally Arrow-shaped (offsets +
/// concatenated bytes for variable-width types) but does not require Arrow
/// libraries on either side.
#[repr(C)]
#[derive(Debug)]
pub struct ColumnBuffers {
    pub type_tag: u8,
    /// Number of logical rows in this column.
    pub row_count: usize,
    /// `ceil(row_count / 8)` bytes, LSB-first. Null when no nulls are
    /// present. Engines may always emit a bitmap.
    pub null_bitmap: *const u8,
    pub null_bitmap_len: usize,
    /// Type-dependent primary buffer:
    /// - Primitives: `row_count * sizeof(T)` packed values, native endian.
    /// - String / Binary: `(row_count + 1) * 4` bytes of i32 offsets.
    /// - List / Map: `(row_count + 1) * 4` bytes of i32 offsets.
    /// - Struct: unused.
    pub primary: *const u8,
    pub primary_len: usize,
    /// Type-dependent auxiliary buffer:
    /// - String / Binary: concatenated value bytes (last offset = total len).
    /// - All other types: unused.
    pub aux: *const u8,
    pub aux_len: usize,
    /// Decimal-only: precision + scale.
    pub decimal_precision: u8,
    pub decimal_scale: u8,
    /// Nested-type child columns:
    /// - List: 1 child (element type).
    /// - Map: 2 children (key, value).
    /// - Struct: N children (one per field).
    /// - All other types: null.
    pub children: *const ColumnBuffers,
    pub children_len: usize,
}

// SAFETY: pointer fields are opaque to Rust; engines own the underlying memory
// and must keep it valid for the duration of a `materialize_columns` call.
unsafe impl Send for ColumnBuffers {}
unsafe impl Sync for ColumnBuffers {}

/// Argument to the `materialize_columns` callback: a request from Rust for
/// per-column buffers covering the listed paths.
#[repr(C)]
pub struct VisitRowsRequest {
    /// Engine-managed batch handle previously returned by `read_json_files`,
    /// `parse_json`, or an evaluator's `evaluate`.
    pub batch_handle: *mut c_void,
    /// Number of column paths.
    pub paths_len: usize,
    /// Column paths in dotted form, e.g. `"add.deletionVector.storageType"`.
    /// Engines walk these to extract the requested column.
    pub paths: *const KernelStringSlice,
}

/// Result of `materialize_columns`: an array of [`ColumnBuffers`], one per
/// requested path, in the same order.
#[repr(C)]
pub struct VisitRowsResult {
    /// Array of `paths_len` column buffers.
    pub columns: *const ColumnBuffers,
    pub columns_len: usize,
    /// `0` on success, non-zero indicates an engine error and `columns` is
    /// ignored.
    pub error: u32,
}

// ============================================================================
// Engine-data-side vtable (shared by Json / Eval / future handlers that
// produce `EngineData`)
// ============================================================================

/// Function-pointer table the engine supplies for materializing arbitrary
/// columns out of a Java-owned batch.
#[repr(C)]
pub struct JavaEngineDataCallbacks {
    /// Materialize the requested column paths into Rust-readable memory.
    pub materialize_columns: extern "C" fn(
        engine_state: *mut c_void,
        request: *const VisitRowsRequest,
        out: *mut VisitRowsResult,
    ),
    /// Release the buffers from a previous `materialize_columns` call.
    pub free_columns:
        extern "C" fn(engine_state: *mut c_void, columns: *const ColumnBuffers, columns_len: usize),
    /// Apply a selection vector to a batch, producing a new batch handle.
    /// Engines that can't filter on their side return a non-zero error.
    pub apply_selection_vector: extern "C" fn(
        engine_state: *mut c_void,
        batch_handle: *mut c_void,
        selection: *const u8, // packed bools, ceil(len/8)
        selection_len: usize,
        out: *mut SelectionResult,
    ),
    /// Append columns to a batch. Bulk-write APIs need this; read-path
    /// engines may stub it with an error.
    pub append_columns: extern "C" fn(
        engine_state: *mut c_void,
        batch_handle: *mut c_void,
        // Schema describing the appended columns -- Rust passes a JSON form
        // the engine can deserialize. NULL on free.
        schema_json: KernelStringSlice,
        // Pre-materialized columns for the new fields (one per top-level
        // field, in schema order).
        columns: *const ColumnBuffers,
        columns_len: usize,
        out: *mut SelectionResult,
    ),
    /// Free a batch handle when its `JavaEngineData` wrapper drops.
    pub free_batch: extern "C" fn(engine_state: *mut c_void, batch_handle: *mut c_void),
    /// Get the row count for a batch.
    pub batch_len: extern "C" fn(engine_state: *mut c_void, batch_handle: *mut c_void) -> usize,
    /// Check if a column path exists in this batch.
    pub has_field: extern "C" fn(
        engine_state: *mut c_void,
        batch_handle: *mut c_void,
        path: KernelStringSlice,
    ) -> bool,
}

unsafe impl Send for JavaEngineDataCallbacks {}
unsafe impl Sync for JavaEngineDataCallbacks {}

/// Result of [`JavaEngineDataCallbacks::apply_selection_vector`] /
/// [`JavaEngineDataCallbacks::append_columns`].
#[repr(C)]
pub struct SelectionResult {
    pub new_batch_handle: *mut c_void,
    pub error: u32,
}

// ============================================================================
// JSON handler vtable
// ============================================================================

/// JSON handler vtable. Mirrors [`JavaParquetCallbacks`] in shape: produce an
/// iterator state, pump batches via `iter_next`, free via `iter_free` and
/// `free_data`.
#[repr(C)]
pub struct JavaJsonCallbacks {
    pub engine_state: *mut c_void,
    /// Parse a batch of JSON strings into a new Java batch.
    /// `json_strings_batch` is a `JavaEngineData` handle whose schema is a
    /// single string column (engines should call back through the engine-data
    /// vtable to materialize the strings).
    pub parse_json: extern "C" fn(
        engine_state: *mut c_void,
        json_strings_batch: *mut c_void,
        output_schema_json: KernelStringSlice,
        out: *mut JavaBatchResult,
    ),
    /// Read JSON files. `predicate_handle` is opaque (a `Handle<SharedPredicate>`
    /// or null).
    pub read_json_files: extern "C" fn(
        engine_state: *mut c_void,
        files: *const FileMeta,
        files_len: usize,
        physical_schema_json: KernelStringSlice,
        predicate: *mut c_void,
        out: *mut crate::callback_engine::JavaParquetReadResult,
    ),
    /// Write a single JSON file.
    pub write_json_file: extern "C" fn(
        engine_state: *mut c_void,
        path: KernelStringSlice,
        data_iter_state: *mut c_void,
        overwrite: bool,
        out_error: *mut u32,
    ),
    /// Pump the next batch from a file-read iterator.
    pub iter_next: extern "C" fn(
        engine_state: *mut c_void,
        iter_state: *mut c_void,
        out: *mut crate::callback_engine::JavaEngineDataResult,
    ),
    pub iter_free: extern "C" fn(engine_state: *mut c_void, iter_state: *mut c_void),
    /// Engine-data callbacks for batches this handler produces. Reused by
    /// every JSON-derived `JavaEngineData`.
    pub engine_data_callbacks: JavaEngineDataCallbacks,
    /// Free the engine state when the handler drops.
    pub free_engine_state: extern "C" fn(engine_state: *mut c_void),
}

unsafe impl Send for JavaJsonCallbacks {}
unsafe impl Sync for JavaJsonCallbacks {}

/// Result of `parse_json` (or any per-call batch producer).
#[repr(C)]
pub struct JavaBatchResult {
    pub batch_handle: *mut c_void,
    pub error: u32,
}

// ============================================================================
// Storage handler vtable
// ============================================================================

/// Storage handler vtable. Returns iterators of [`FileMeta`] (for
/// `list_from`) or raw byte buffers (for `read_files`); no `EngineData`
/// involvement, so the engine-data callbacks are not needed.
#[repr(C)]
pub struct JavaStorageCallbacks {
    pub engine_state: *mut c_void,
    pub list_from: extern "C" fn(
        engine_state: *mut c_void,
        path: KernelStringSlice,
        out: *mut JavaFileIterResult,
    ),
    pub read_files: extern "C" fn(
        engine_state: *mut c_void,
        files: *const FileSliceFFI,
        files_len: usize,
        out: *mut JavaBytesIterResult,
    ),
    pub copy_atomic: extern "C" fn(
        engine_state: *mut c_void,
        src: KernelStringSlice,
        dst: KernelStringSlice,
        out_error: *mut u32,
    ),
    pub put: extern "C" fn(
        engine_state: *mut c_void,
        path: KernelStringSlice,
        data: *const u8,
        data_len: usize,
        overwrite: bool,
        out_error: *mut u32,
    ),
    pub head: extern "C" fn(
        engine_state: *mut c_void,
        path: KernelStringSlice,
        out: *mut JavaFileMetaResult,
    ),
    /// Pump a file iterator returned by `list_from`.
    pub list_iter_next: extern "C" fn(
        engine_state: *mut c_void,
        iter_state: *mut c_void,
        out: *mut JavaFileMetaResult,
    ),
    pub list_iter_free: extern "C" fn(engine_state: *mut c_void, iter_state: *mut c_void),
    /// Pump a bytes iterator returned by `read_files`.
    pub bytes_iter_next: extern "C" fn(
        engine_state: *mut c_void,
        iter_state: *mut c_void,
        out: *mut JavaBytesResult,
    ),
    pub bytes_iter_free: extern "C" fn(engine_state: *mut c_void, iter_state: *mut c_void),
    pub free_engine_state: extern "C" fn(engine_state: *mut c_void),
}

unsafe impl Send for JavaStorageCallbacks {}
unsafe impl Sync for JavaStorageCallbacks {}

#[repr(C)]
pub struct JavaFileIterResult {
    pub iter_state: *mut c_void,
    pub error: u32,
}

#[repr(C)]
pub struct JavaBytesIterResult {
    pub iter_state: *mut c_void,
    pub error: u32,
}

#[repr(C)]
pub struct JavaFileMetaResult {
    pub path: KernelStringSlice,
    pub last_modified: i64,
    pub size: u64,
    pub done: bool,
    pub error: u32,
}

#[repr(C)]
pub struct JavaBytesResult {
    pub data: *const u8,
    pub data_len: usize,
    pub done: bool,
    pub error: u32,
}

#[repr(C)]
pub struct FileSliceFFI {
    pub path: KernelStringSlice,
    pub start_offset: u64,
    pub end_offset: u64,
}

// ============================================================================
// Evaluation handler vtable
// ============================================================================

/// Evaluation handler vtable. Predicate / expression evaluators are looked up
/// via `new_*_evaluator`; the resulting evaluator handle is invoked via
/// `evaluate_expression` / `evaluate_predicate` per batch.
///
/// Expressions and predicates cross the boundary as `Handle<SharedExpression>`
/// / `Handle<SharedPredicate>` -- the engine consumes them and is free to
/// translate into its native expression representation up front (e.g. by
/// walking via the existing `EngineExpressionVisitor` pattern).
#[repr(C)]
pub struct JavaEvaluationCallbacks {
    pub engine_state: *mut c_void,
    pub new_expression_evaluator: extern "C" fn(
        engine_state: *mut c_void,
        input_schema_json: KernelStringSlice,
        expression: *mut c_void, // Handle<SharedExpression>
        output_type_json: KernelStringSlice,
        out: *mut JavaEvaluatorResult,
    ),
    pub new_predicate_evaluator: extern "C" fn(
        engine_state: *mut c_void,
        input_schema_json: KernelStringSlice,
        predicate: *mut c_void, // Handle<SharedPredicate>
        out: *mut JavaEvaluatorResult,
    ),
    /// Run an expression evaluator against a batch. Result is a new batch
    /// handle whose schema matches the evaluator's `output_type`.
    pub evaluate_expression: extern "C" fn(
        engine_state: *mut c_void,
        evaluator: *mut c_void,
        batch_handle: *mut c_void,
        out: *mut JavaBatchResult,
    ),
    /// Run a predicate evaluator against a batch. Result is a new batch
    /// handle with a single boolean column named "output".
    pub evaluate_predicate: extern "C" fn(
        engine_state: *mut c_void,
        evaluator: *mut c_void,
        batch_handle: *mut c_void,
        out: *mut JavaBatchResult,
    ),
    pub free_evaluator: extern "C" fn(engine_state: *mut c_void, evaluator: *mut c_void),
    /// Build a single-row null batch with the given output schema.
    pub null_row: extern "C" fn(
        engine_state: *mut c_void,
        output_schema_json: KernelStringSlice,
        out: *mut JavaBatchResult,
    ),
    /// Build a multi-row batch from pre-encoded scalar values.
    /// `rows_buffer` is a flat encoding of the values (engine + Rust agree
    /// on a binary protocol; not yet defined in this skeleton).
    pub create_many: extern "C" fn(
        engine_state: *mut c_void,
        schema_json: KernelStringSlice,
        rows_buffer: *const u8,
        rows_buffer_len: usize,
        out: *mut JavaBatchResult,
    ),
    /// Engine-data callbacks for batches eval produces.
    pub engine_data_callbacks: JavaEngineDataCallbacks,
    pub free_engine_state: extern "C" fn(engine_state: *mut c_void),
}

unsafe impl Send for JavaEvaluationCallbacks {}
unsafe impl Sync for JavaEvaluationCallbacks {}

#[repr(C)]
pub struct JavaEvaluatorResult {
    pub evaluator_handle: *mut c_void,
    pub error: u32,
}

// ============================================================================
// JavaEngineData -- the EngineData impl backed by a Java handle.
// ============================================================================

/// Shared state every `JavaEngineData` carries to upcall back into Java.
/// Cloned per batch via `Arc` so the engine state outlives any batch the
/// kernel still holds (mirrors the parquet-handler lifetime model).
pub(crate) struct EngineDataState {
    pub callbacks: JavaEngineDataCallbacks,
    pub engine_state: *mut c_void,
    /// Optional handler-state Arc to keep alive (so the engine state isn't
    /// freed while batches are still in flight). For JSON the handler owns
    /// engine state; for eval the evaluator owns it. The `JavaEngineData`
    /// just holds an Arc to the handler via this opaque field.
    pub _handler_keeper: Option<Arc<dyn std::any::Any + Send + Sync>>,
}

// SAFETY: same justification as the vtable structs above.
unsafe impl Send for EngineDataState {}
unsafe impl Sync for EngineDataState {}

/// Pass-through `EngineData` whose payload lives on the Java side, with
/// `visit_rows` implemented by upcalling into Java for column buffers.
///
/// **Status: skeleton.** `visit_rows` returns `Error::generic("not yet
/// implemented")` until the column-materialization adapters are filled in.
/// The intended flow is:
///
/// ```text
/// 1. Build VisitRowsRequest with column_paths.
/// 2. Call materialize_columns -> VisitRowsResult{columns_ptr}.
/// 3. Walk the ColumnBuffers array, build per-type GetData adapters.
/// 4. Call visitor.visit(row_count, &adapters).
/// 5. Call free_columns(columns_ptr, columns_len).
/// ```
pub struct JavaEngineData {
    pub batch_handle: *mut c_void,
    pub state: Arc<EngineDataState>,
}

unsafe impl Send for JavaEngineData {}
unsafe impl Sync for JavaEngineData {}

impl Drop for JavaEngineData {
    fn drop(&mut self) {
        if !self.batch_handle.is_null() {
            (self.state.callbacks.free_batch)(self.state.engine_state, self.batch_handle);
        }
    }
}

impl EngineData for JavaEngineData {
    fn visit_rows(
        &self,
        column_names: &[ColumnName],
        visitor: &mut dyn RowVisitor,
    ) -> DeltaResult<()> {
        // 1. Encode column paths as KernelStringSlices (dotted form).
        let path_strings: Vec<String> = column_names.iter().map(|cn| cn.to_string()).collect();
        let path_slices: Vec<KernelStringSlice> = path_strings
            .iter()
            // SAFETY: each `s` is borrowed from `path_strings`, which lives
            // until the end of this function (which fully encompasses the
            // synchronous upcall + visitor invocation).
            .map(|s| unsafe { KernelStringSlice::new_unsafe(s.as_str()) })
            .collect();

        let request = VisitRowsRequest {
            batch_handle: self.batch_handle,
            paths_len: path_slices.len(),
            paths: path_slices.as_ptr(),
        };
        let mut result = VisitRowsResult {
            columns: std::ptr::null(),
            columns_len: 0,
            error: 0,
        };

        // 2. Upcall.
        (self.state.callbacks.materialize_columns)(
            self.state.engine_state,
            &request as *const _,
            &mut result as *mut _,
        );

        if result.error != 0 {
            return Err(Error::generic(format!(
                "materialize_columns signalled error code {}",
                result.error
            )));
        }
        if result.columns_len != path_slices.len() {
            // Free what we got, surface a clear error.
            (self.state.callbacks.free_columns)(
                self.state.engine_state,
                result.columns,
                result.columns_len,
            );
            return Err(Error::generic(format!(
                "engine returned {} columns; kernel requested {}",
                result.columns_len,
                path_slices.len()
            )));
        }

        // 3. Walk columns, build per-type adapters.
        let columns_slice = if result.columns_len == 0 {
            &[][..]
        } else {
            // SAFETY: engine guarantees `columns` valid for `columns_len`
            // ColumnBuffers entries until `free_columns` is called.
            unsafe { std::slice::from_raw_parts(result.columns, result.columns_len) }
        };
        let row_count = columns_slice.first().map(|c| c.row_count).unwrap_or(0);

        // Build all adapters, then references to them. We have to scope
        // the borrow + visit + free carefully so the buffer lifetimes are
        // honored.
        let visit_result: DeltaResult<()> = (|| {
            let adapters = build_get_data_adapters(columns_slice)?;
            let getter_refs: Vec<&dyn GetData<'_>> =
                adapters.iter().map(|a| a.as_getter()).collect();
            visitor.visit(row_count, &getter_refs)
        })();

        // 4. Free buffers regardless of visit outcome.
        (self.state.callbacks.free_columns)(
            self.state.engine_state,
            result.columns,
            result.columns_len,
        );
        visit_result
    }

    fn len(&self) -> usize {
        (self.state.callbacks.batch_len)(self.state.engine_state, self.batch_handle)
    }

    fn append_columns(
        &self,
        _schema: SchemaRef,
        _columns: Vec<ArrayData>,
    ) -> DeltaResult<Box<dyn EngineData>> {
        Err(Error::generic(
            "JavaEngineData::append_columns: skeleton -- column wire format encoder not yet \
             implemented.",
        ))
    }

    fn apply_selection_vector(
        self: Box<Self>,
        selection_vector: Vec<bool>,
    ) -> DeltaResult<Box<dyn EngineData>> {
        // Trivial all-true selection passes through (matches the read-path
        // shortcut for tables without DVs). Non-trivial selections delegate
        // to the engine's apply_selection_vector callback.
        let len = self.len();
        let all_selected = selection_vector.iter().all(|&b| b);
        if all_selected && selection_vector.len() == len {
            return Ok(self);
        }
        // TODO: pack selection_vector into a byte buffer and call the
        // engine's apply_selection_vector. Until that's wired we surface a
        // clear error so any DV-bearing table fails loudly instead of
        // silently misrepresenting row counts.
        Err(Error::generic(
            "JavaEngineData::apply_selection_vector: non-trivial selection requires the engine's \
             apply_selection_vector callback (not yet wired through). The engine that produced \
             this batch must filter on its side.",
        ))
    }

    fn has_field(&self, name: &ColumnName) -> bool {
        let path = name.to_string();
        // SAFETY: `path` lives for the duration of this call.
        let slice = unsafe { KernelStringSlice::new_unsafe(&path) };
        (self.state.callbacks.has_field)(self.state.engine_state, self.batch_handle, slice)
    }
}

// ============================================================================
// JavaJsonHandler -- skeleton impl of JsonHandler over the vtable
// ============================================================================

/// Shared state for the JSON handler. The engine state is freed exactly once
/// when the last Arc reference (held by the handler + any batches it
/// produced) drops.
struct JsonHandlerState {
    callbacks: JavaJsonCallbacks,
    /// Used by `JavaEngineData::Drop` to keep the engine state alive even
    /// after the handler itself has been dropped.
    engine_data_state: Arc<EngineDataState>,
}

impl Drop for JsonHandlerState {
    fn drop(&mut self) {
        (self.callbacks.free_engine_state)(self.callbacks.engine_state);
    }
}

/// `JsonHandler` impl that upcalls into Java for parsing / file reading.
/// **Skeleton** -- implementations are stubs.
pub struct JavaJsonHandler {
    state: Arc<JsonHandlerState>,
}

impl JavaJsonHandler {
    pub fn new(callbacks: JavaJsonCallbacks) -> Self {
        let engine_state = callbacks.engine_state;
        let engine_data_callbacks = JavaEngineDataCallbacks {
            ..callbacks.engine_data_callbacks
        };
        let state = Arc::new(JsonHandlerState {
            engine_data_state: Arc::new(EngineDataState {
                callbacks: engine_data_callbacks,
                engine_state,
                _handler_keeper: None,
            }),
            callbacks,
        });
        Self { state }
    }
}

impl JsonHandler for JavaJsonHandler {
    fn parse_json(
        &self,
        _json_strings: Box<dyn EngineData>,
        _output_schema: SchemaRef,
    ) -> DeltaResult<Box<dyn EngineData>> {
        // Not used by log replay; only kicks in when the kernel has JSON
        // strings already in memory (e.g. some commit-info paths). Defer
        // until a use case lands.
        Err(Error::generic(
            "JavaJsonHandler::parse_json: not yet wired; log replay uses read_json_files instead.",
        ))
    }

    fn read_json_files(
        &self,
        files: &[KernelFileMeta],
        _physical_schema: SchemaRef,
        _predicate: Option<PredicateRef>,
    ) -> DeltaResult<FileDataReadResultIterator> {
        // Marshal &[KernelFileMeta] -> [FfiFileMeta]; same shape as the
        // parquet handler.
        let ffi_files: Vec<FileMeta> = files
            .iter()
            .map(|fm| {
                let url = fm.location.as_str();
                let size: usize = fm.size.try_into().map_err(|_| {
                    Error::generic("FileMeta::size does not fit in usize on this platform")
                })?;
                Ok(FileMeta {
                    // SAFETY: `url` borrows `fm.location`, valid for this call.
                    path: unsafe { KernelStringSlice::new_unsafe(url) },
                    last_modified: fm.last_modified,
                    size,
                })
            })
            .collect::<DeltaResult<Vec<_>>>()?;

        // Schema is currently ignored by the engine side -- the JSON
        // callback is expected to parse the full action shape and the
        // kernel asks for specific column paths via visit_rows. Pass a
        // null path slice as the JSON-form schema for now (engines can
        // ignore it).
        let schema_json_slice = unsafe { KernelStringSlice::new_unsafe("") };

        let mut result = crate::callback_engine::JavaParquetReadResult {
            iter_state: std::ptr::null_mut(),
            error: 0,
        };
        // Predicate: always null in the POC.
        (self.state.callbacks.read_json_files)(
            self.state.callbacks.engine_state,
            ffi_files.as_ptr(),
            ffi_files.len(),
            schema_json_slice,
            std::ptr::null_mut(),
            &mut result as *mut _,
        );
        if result.error != 0 {
            return Err(Error::generic(format!(
                "JavaJsonHandler.read_json_files signalled error code {}",
                result.error
            )));
        }
        if result.iter_state.is_null() {
            return Err(Error::generic(
                "JavaJsonHandler.read_json_files returned null iterator on success",
            ));
        }

        Ok(Box::new(JavaJsonIter {
            iter_state: result.iter_state,
            handler_state: self.state.clone(),
            done: false,
        }))
    }

    fn write_json_file(
        &self,
        _path: &Url,
        _data: Box<dyn Iterator<Item = DeltaResult<FilteredEngineData>> + Send + '_>,
        _overwrite: bool,
    ) -> DeltaResult<()> {
        Err(Error::generic(
            "JavaJsonHandler::write_json_file: skeleton -- write-side serialization not yet \
             implemented.",
        ))
    }
}

/// Iterator over JSON-derived batches. Pumps the engine's `iter_next` and
/// wraps each returned batch handle in a `JavaEngineData`.
struct JavaJsonIter {
    iter_state: *mut c_void,
    handler_state: Arc<JsonHandlerState>,
    done: bool,
}

unsafe impl Send for JavaJsonIter {}

impl Drop for JavaJsonIter {
    fn drop(&mut self) {
        if !self.iter_state.is_null() {
            (self.handler_state.callbacks.iter_free)(
                self.handler_state.callbacks.engine_state,
                self.iter_state,
            );
        }
    }
}

impl Iterator for JavaJsonIter {
    type Item = DeltaResult<Box<dyn EngineData>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let mut res = crate::callback_engine::JavaEngineDataResult {
            batch: std::ptr::null_mut(),
            len: 0,
            done: false,
            error: 0,
        };
        (self.handler_state.callbacks.iter_next)(
            self.handler_state.callbacks.engine_state,
            self.iter_state,
            &mut res as *mut _,
        );
        if res.error != 0 {
            self.done = true;
            return Some(Err(Error::generic(format!(
                "JavaJsonIter::next signalled error code {}",
                res.error
            ))));
        }
        if res.done {
            self.done = true;
            return None;
        }
        let batch: Box<dyn EngineData> = Box::new(JavaEngineData {
            batch_handle: res.batch,
            state: self.handler_state.engine_data_state.clone(),
        });
        Some(Ok(batch))
    }
}

// ============================================================================
// JavaStorageHandler -- skeleton impl of StorageHandler over the vtable
// ============================================================================

struct StorageHandlerState {
    callbacks: JavaStorageCallbacks,
}

impl Drop for StorageHandlerState {
    fn drop(&mut self) {
        (self.callbacks.free_engine_state)(self.callbacks.engine_state);
    }
}

/// `StorageHandler` impl that upcalls into Java. **Skeleton.**
pub struct JavaStorageHandler {
    state: Arc<StorageHandlerState>,
}

impl JavaStorageHandler {
    pub fn new(callbacks: JavaStorageCallbacks) -> Self {
        Self {
            state: Arc::new(StorageHandlerState { callbacks }),
        }
    }
}

impl StorageHandler for JavaStorageHandler {
    fn list_from(
        &self,
        _path: &Url,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<KernelFileMeta>>>> {
        Err(Error::generic(
            "JavaStorageHandler::list_from: skeleton -- file-iterator wiring not yet \
             implemented.",
        ))
    }

    fn read_files(
        &self,
        _files: Vec<FileSlice>,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<Bytes>>>> {
        Err(Error::generic(
            "JavaStorageHandler::read_files: skeleton -- bytes-iterator wiring not yet \
             implemented.",
        ))
    }

    fn copy_atomic(&self, _src: &Url, _dst: &Url) -> DeltaResult<()> {
        Err(Error::generic(
            "JavaStorageHandler::copy_atomic: skeleton -- not yet implemented.",
        ))
    }

    fn put(&self, _path: &Url, _data: Bytes, _overwrite: bool) -> DeltaResult<()> {
        Err(Error::generic(
            "JavaStorageHandler::put: skeleton -- not yet implemented.",
        ))
    }

    fn head(&self, _path: &Url) -> DeltaResult<KernelFileMeta> {
        Err(Error::generic(
            "JavaStorageHandler::head: skeleton -- not yet implemented.",
        ))
    }
}

// ============================================================================
// JavaEvaluationHandler -- skeleton impl of EvaluationHandler over the vtable
// ============================================================================

struct EvaluationHandlerState {
    callbacks: JavaEvaluationCallbacks,
    #[allow(dead_code)]
    engine_data_state: Arc<EngineDataState>,
}

impl Drop for EvaluationHandlerState {
    fn drop(&mut self) {
        (self.callbacks.free_engine_state)(self.callbacks.engine_state);
    }
}

/// `EvaluationHandler` impl that upcalls into Java. **Skeleton.**
pub struct JavaEvaluationHandler {
    state: Arc<EvaluationHandlerState>,
}

impl JavaEvaluationHandler {
    pub fn new(callbacks: JavaEvaluationCallbacks) -> Self {
        let engine_state = callbacks.engine_state;
        let engine_data_callbacks = JavaEngineDataCallbacks {
            ..callbacks.engine_data_callbacks
        };
        let state = Arc::new(EvaluationHandlerState {
            engine_data_state: Arc::new(EngineDataState {
                callbacks: engine_data_callbacks,
                engine_state,
                _handler_keeper: None,
            }),
            callbacks,
        });
        Self { state }
    }
}

/// Skeleton evaluator. Filling in `evaluate` requires the column-materialization
/// protocol to be live so we can read the input batch.
struct JavaEvaluator {
    handler: Arc<EvaluationHandlerState>,
    evaluator_handle: *mut c_void,
}

unsafe impl Send for JavaEvaluator {}
unsafe impl Sync for JavaEvaluator {}

impl Drop for JavaEvaluator {
    fn drop(&mut self) {
        (self.handler.callbacks.free_evaluator)(
            self.handler.callbacks.engine_state,
            self.evaluator_handle,
        );
    }
}

impl ExpressionEvaluator for JavaEvaluator {
    fn evaluate(&self, _batch: &dyn EngineData) -> DeltaResult<Box<dyn EngineData>> {
        Err(Error::generic(
            "JavaEvaluator::evaluate: skeleton -- expression dispatch not yet wired.",
        ))
    }
}

impl PredicateEvaluator for JavaEvaluator {
    fn evaluate(&self, _batch: &dyn EngineData) -> DeltaResult<Box<dyn EngineData>> {
        Err(Error::generic(
            "JavaEvaluator::evaluate (predicate): skeleton -- predicate dispatch not yet wired.",
        ))
    }
}

impl EvaluationHandler for JavaEvaluationHandler {
    fn new_expression_evaluator(
        &self,
        _input_schema: SchemaRef,
        _expression: ExpressionRef,
        _output_type: DataType,
    ) -> DeltaResult<Arc<dyn ExpressionEvaluator>> {
        // Returning a stub evaluator that always errors when invoked. A real
        // impl would call `new_expression_evaluator` on the vtable and store
        // the returned handle.
        Ok(Arc::new(NotImplementedEvaluator))
    }

    fn new_predicate_evaluator(
        &self,
        _input_schema: SchemaRef,
        _predicate: PredicateRef,
    ) -> DeltaResult<Arc<dyn PredicateEvaluator>> {
        Ok(Arc::new(NotImplementedEvaluator))
    }

    fn null_row(&self, _output_schema: SchemaRef) -> DeltaResult<Box<dyn EngineData>> {
        Err(Error::generic(
            "JavaEvaluationHandler::null_row: skeleton -- not yet implemented.",
        ))
    }

    fn create_many(
        &self,
        _schema: SchemaRef,
        _rows: &[&[delta_kernel::expressions::Scalar]],
    ) -> DeltaResult<Box<dyn EngineData>> {
        Err(Error::generic(
            "JavaEvaluationHandler::create_many: skeleton -- scalar-row encoder not yet \
             implemented.",
        ))
    }
}

struct NotImplementedEvaluator;
impl ExpressionEvaluator for NotImplementedEvaluator {
    fn evaluate(&self, _batch: &dyn EngineData) -> DeltaResult<Box<dyn EngineData>> {
        Err(Error::generic(
            "JavaEvaluationHandler: skeleton -- expression evaluators not yet implemented.",
        ))
    }
}
impl PredicateEvaluator for NotImplementedEvaluator {
    fn evaluate(&self, _batch: &dyn EngineData) -> DeltaResult<Box<dyn EngineData>> {
        Err(Error::generic(
            "JavaEvaluationHandler: skeleton -- predicate evaluators not yet implemented.",
        ))
    }
}

// ============================================================================
// GetData adapters -- read engine-materialized column buffers
// ============================================================================

/// Helper: enum of per-type adapters. Exists so we can hold the adapter
/// (with its borrowed buffers) in a `Vec` without `Box<dyn>` lifetime
/// gymnastics; downcasts to the right impl are done via `as_getter`.
enum ColumnAdapter<'a> {
    Bool(BoolAdapter<'a>),
    Int(IntAdapter<'a>),
    Long(LongAdapter<'a>),
    Str(StringAdapter<'a>),
    StringList(StringListAdapter<'a>),
    StringMap(StringMapAdapter<'a>),
    Unsupported(u8),
}

impl<'a> ColumnAdapter<'a> {
    fn as_getter(&'a self) -> &'a dyn GetData<'a> {
        match self {
            ColumnAdapter::Bool(a) => a,
            ColumnAdapter::Int(a) => a,
            ColumnAdapter::Long(a) => a,
            ColumnAdapter::Str(a) => a,
            ColumnAdapter::StringList(a) => a,
            ColumnAdapter::StringMap(a) => a,
            // Unsupported types fall back to `()` which returns null for
            // every getter -- the kernel will then error if it actually
            // tried to read this column.
            ColumnAdapter::Unsupported(_) => &(),
        }
    }
}

fn build_get_data_adapters<'a>(
    columns: &'a [ColumnBuffers],
) -> DeltaResult<Vec<ColumnAdapter<'a>>> {
    columns.iter().map(build_one_adapter).collect()
}

fn build_one_adapter<'a>(col: &'a ColumnBuffers) -> DeltaResult<ColumnAdapter<'a>> {
    match col.type_tag {
        t if t == ColumnTypeTag::Bool as u8 => Ok(ColumnAdapter::Bool(BoolAdapter::new(col)?)),
        t if t == ColumnTypeTag::Int as u8 => Ok(ColumnAdapter::Int(IntAdapter::new(col)?)),
        t if t == ColumnTypeTag::Long as u8 => Ok(ColumnAdapter::Long(LongAdapter::new(col)?)),
        t if t == ColumnTypeTag::String as u8 => Ok(ColumnAdapter::Str(StringAdapter::new(col)?)),
        t if t == ColumnTypeTag::List as u8 => {
            // Only list<string> is wired today.
            let elem = unsafe { resolve_single_child(col)? };
            if elem.type_tag != ColumnTypeTag::String as u8 {
                return Ok(ColumnAdapter::Unsupported(col.type_tag));
            }
            Ok(ColumnAdapter::StringList(StringListAdapter::new(
                col, elem,
            )?))
        }
        t if t == ColumnTypeTag::Map as u8 => {
            // Only map<string,string> is wired today.
            let (keys, values) = unsafe { resolve_two_children(col)? };
            if keys.type_tag != ColumnTypeTag::String as u8
                || values.type_tag != ColumnTypeTag::String as u8
            {
                return Ok(ColumnAdapter::Unsupported(col.type_tag));
            }
            Ok(ColumnAdapter::StringMap(StringMapAdapter::new(
                col, keys, values,
            )?))
        }
        // Other types return null on every access -- log replay only
        // touches the types above for the schemas it cares about.
        _ => Ok(ColumnAdapter::Unsupported(col.type_tag)),
    }
}

/// Resolve a list/array column's single child column.
unsafe fn resolve_single_child(col: &ColumnBuffers) -> DeltaResult<&ColumnBuffers> {
    if col.children_len != 1 || col.children.is_null() {
        return Err(Error::generic(format!(
            "list column expected 1 child; got children_len={}",
            col.children_len
        )));
    }
    Ok(unsafe { &*col.children })
}

/// Resolve a map column's (keys, values) child columns.
unsafe fn resolve_two_children(
    col: &ColumnBuffers,
) -> DeltaResult<(&ColumnBuffers, &ColumnBuffers)> {
    if col.children_len != 2 || col.children.is_null() {
        return Err(Error::generic(format!(
            "map column expected 2 children; got children_len={}",
            col.children_len
        )));
    }
    let children = unsafe { std::slice::from_raw_parts(col.children, 2) };
    Ok((&children[0], &children[1]))
}

/// Read a bit out of a packed null bitmap (`true` = NOT null). When no
/// bitmap is present (`null_bitmap` is null), every row is non-null.
fn is_valid(col: &ColumnBuffers, row: usize) -> bool {
    if col.null_bitmap.is_null() {
        return true;
    }
    // null_bitmap_len bytes; bit `row % 8` in byte `row / 8` -- 1 means
    // the row is VALID (not null), matching Arrow's convention.
    let byte_idx = row / 8;
    if byte_idx >= col.null_bitmap_len {
        return true;
    }
    let byte = unsafe { *col.null_bitmap.add(byte_idx) };
    (byte >> (row % 8)) & 1 == 1
}

/// Read primary buffer as a typed slice.
unsafe fn primary_as<T>(col: &ColumnBuffers) -> &[T] {
    if col.primary.is_null() || col.row_count == 0 {
        return &[];
    }
    unsafe { std::slice::from_raw_parts(col.primary as *const T, col.row_count) }
}

/// Read primary buffer as a slice of i32 offsets (length row_count + 1).
unsafe fn offsets(col: &ColumnBuffers) -> &[i32] {
    if col.primary.is_null() {
        return &[];
    }
    unsafe { std::slice::from_raw_parts(col.primary as *const i32, col.row_count + 1) }
}

// -------- Bool / Int / Long primitive adapters --------

struct BoolAdapter<'a> {
    col: &'a ColumnBuffers,
    values: &'a [u8],
}
impl<'a> BoolAdapter<'a> {
    fn new(col: &'a ColumnBuffers) -> DeltaResult<Self> {
        Ok(Self {
            col,
            values: unsafe { primary_as::<u8>(col) },
        })
    }
}
impl<'a> GetData<'a> for BoolAdapter<'a> {
    fn get_bool(&'a self, row: usize, _field: &str) -> DeltaResult<Option<bool>> {
        if !is_valid(self.col, row) {
            return Ok(None);
        }
        Ok(Some(self.values[row] != 0))
    }
}

struct IntAdapter<'a> {
    col: &'a ColumnBuffers,
    values: &'a [i32],
}
impl<'a> IntAdapter<'a> {
    fn new(col: &'a ColumnBuffers) -> DeltaResult<Self> {
        Ok(Self {
            col,
            values: unsafe { primary_as::<i32>(col) },
        })
    }
}
impl<'a> GetData<'a> for IntAdapter<'a> {
    fn get_int(&'a self, row: usize, _field: &str) -> DeltaResult<Option<i32>> {
        if !is_valid(self.col, row) {
            return Ok(None);
        }
        Ok(Some(self.values[row]))
    }
}

struct LongAdapter<'a> {
    col: &'a ColumnBuffers,
    values: &'a [i64],
}
impl<'a> LongAdapter<'a> {
    fn new(col: &'a ColumnBuffers) -> DeltaResult<Self> {
        Ok(Self {
            col,
            values: unsafe { primary_as::<i64>(col) },
        })
    }
}
impl<'a> GetData<'a> for LongAdapter<'a> {
    fn get_long(&'a self, row: usize, _field: &str) -> DeltaResult<Option<i64>> {
        if !is_valid(self.col, row) {
            return Ok(None);
        }
        Ok(Some(self.values[row]))
    }
}

// -------- String adapter --------

struct StringAdapter<'a> {
    col: &'a ColumnBuffers,
    offsets: &'a [i32],
    bytes: &'a [u8],
}
impl<'a> StringAdapter<'a> {
    fn new(col: &'a ColumnBuffers) -> DeltaResult<Self> {
        let offsets = unsafe { offsets(col) };
        let bytes = if col.aux.is_null() {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(col.aux, col.aux_len) }
        };
        Ok(Self {
            col,
            offsets,
            bytes,
        })
    }
    fn value_at(&'a self, row: usize) -> DeltaResult<&'a str> {
        let start = self.offsets[row] as usize;
        let end = self.offsets[row + 1] as usize;
        std::str::from_utf8(&self.bytes[start..end])
            .map_err(|e| Error::generic(format!("invalid utf8 at row {row}: {e}")))
    }
}
impl<'a> GetData<'a> for StringAdapter<'a> {
    fn get_str(&'a self, row: usize, _field: &str) -> DeltaResult<Option<&'a str>> {
        if !is_valid(self.col, row) {
            return Ok(None);
        }
        self.value_at(row).map(Some)
    }
}

// -------- StringArrayAccessor over a string ColumnBuffers --------

/// Wraps a child string column and exposes it via `StringArrayAccessor` for
/// `ListItem` / `MapItem`.
struct StringArrayView<'a> {
    col: &'a ColumnBuffers,
    offsets: &'a [i32],
    bytes: &'a [u8],
}
impl<'a> StringArrayView<'a> {
    fn new(col: &'a ColumnBuffers) -> DeltaResult<Self> {
        if col.type_tag != ColumnTypeTag::String as u8 {
            return Err(Error::generic("StringArrayView expects a string column"));
        }
        // For a child string column the offset count is row_count + 1.
        let offsets = if col.primary.is_null() {
            &[][..]
        } else {
            unsafe { std::slice::from_raw_parts(col.primary as *const i32, col.row_count + 1) }
        };
        let bytes = if col.aux.is_null() {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(col.aux, col.aux_len) }
        };
        Ok(Self {
            col,
            offsets,
            bytes,
        })
    }
}
impl<'a> StringArrayAccessor for StringArrayView<'a> {
    fn len(&self) -> usize {
        self.col.row_count
    }
    fn value(&self, index: usize) -> &str {
        let start = self.offsets[index] as usize;
        let end = self.offsets[index + 1] as usize;
        // SAFETY: engine asserted these are valid utf8 when materializing.
        unsafe { std::str::from_utf8_unchecked(&self.bytes[start..end]) }
    }
    fn is_valid(&self, index: usize) -> bool {
        is_valid(self.col, index)
    }
}

// -------- list<string> adapter --------

struct StringListAdapter<'a> {
    col: &'a ColumnBuffers,
    list_offsets: &'a [i32],
    elements: StringArrayView<'a>,
}
impl<'a> StringListAdapter<'a> {
    fn new(col: &'a ColumnBuffers, child: &'a ColumnBuffers) -> DeltaResult<Self> {
        let list_offsets = unsafe { offsets(col) };
        Ok(Self {
            col,
            list_offsets,
            elements: StringArrayView::new(child)?,
        })
    }
}
impl<'a> GetData<'a> for StringListAdapter<'a> {
    fn get_list(&'a self, row: usize, _field: &str) -> DeltaResult<Option<ListItem<'a>>> {
        if !is_valid(self.col, row) {
            return Ok(None);
        }
        let start = self.list_offsets[row] as usize;
        let end = self.list_offsets[row + 1] as usize;
        Ok(Some(ListItem::new(&self.elements, Range { start, end })))
    }
}

// -------- map<string,string> adapter --------

struct StringMapAdapter<'a> {
    col: &'a ColumnBuffers,
    map_offsets: &'a [i32],
    keys: StringArrayView<'a>,
    values: StringArrayView<'a>,
}
impl<'a> StringMapAdapter<'a> {
    fn new(
        col: &'a ColumnBuffers,
        keys: &'a ColumnBuffers,
        values: &'a ColumnBuffers,
    ) -> DeltaResult<Self> {
        let map_offsets = unsafe { offsets(col) };
        Ok(Self {
            col,
            map_offsets,
            keys: StringArrayView::new(keys)?,
            values: StringArrayView::new(values)?,
        })
    }
}
impl<'a> GetData<'a> for StringMapAdapter<'a> {
    fn get_map(&'a self, row: usize, _field: &str) -> DeltaResult<Option<MapItem<'a>>> {
        if !is_valid(self.col, row) {
            return Ok(None);
        }
        let start = self.map_offsets[row] as usize;
        let end = self.map_offsets[row + 1] as usize;
        Ok(Some(MapItem::new(
            &self.keys,
            &self.values,
            Range { start, end },
        )))
    }
}

// Bring the unused symbols into scope so cbindgen sees them when generating
// the C header. (Some of these types are only referenced through fn pointer
// signatures; without an explicit use cbindgen sometimes elides them.)
#[allow(dead_code)]
fn _cbindgen_anchors(
    _: ColumnBuffers,
    _: VisitRowsRequest,
    _: VisitRowsResult,
    _: SelectionResult,
    _: JavaBatchResult,
    _: JavaFileIterResult,
    _: JavaBytesIterResult,
    _: JavaFileMetaResult,
    _: JavaBytesResult,
    _: FileSliceFFI,
    _: JavaEvaluatorResult,
) {
}

// Touch the parquet types so the linker keeps them; harmless no-op.
#[allow(dead_code)]
fn _parquet_link_anchor(_: &JavaParquetCallbacks, _: &JavaParquetHandler) {}

// Re-exports for predicate/expression handle types used in the eval vtable.
#[allow(unused_imports)]
pub use crate::expressions::{SharedExpression, SharedOpaqueExpressionOp, SharedOpaquePredicateOp};

// ============================================================================
// Builder for the full callback engine -- composes optional handler vtables
// ============================================================================

/// Bundle of every Java-supplied handler vtable. Any field set to `None` /
/// null on the FFI side falls back to the inner engine's handler.
#[repr(C)]
pub struct JavaHandlerBundle {
    /// Pointer to a [`JavaParquetCallbacks`], or null.
    pub parquet: *const JavaParquetCallbacks,
    /// Pointer to a [`JavaJsonCallbacks`], or null.
    pub json: *const JavaJsonCallbacks,
    /// Pointer to a [`JavaStorageCallbacks`], or null.
    pub storage: *const JavaStorageCallbacks,
    /// Pointer to a [`JavaEvaluationCallbacks`], or null.
    pub evaluation: *const JavaEvaluationCallbacks,
}

unsafe impl Send for JavaHandlerBundle {}
unsafe impl Sync for JavaHandlerBundle {}

/// Composed [`Engine`] backed by an inner engine plus zero or more Java
/// handler vtables. Each handler accessor returns the Java-backed handler
/// when supplied, otherwise falls through to the inner engine.
///
/// In the skeleton, only the parquet path is fully wired; calls into JSON /
/// Storage / Evaluation Java handlers will return errors because their trait
/// impls are stubs. The compose-and-route logic itself is exercised
/// end-to-end whenever any handler comes from Java.
struct ComposedEngine {
    inner: Arc<dyn delta_kernel::Engine>,
    parquet: Option<Arc<dyn ParquetHandler>>,
    json: Option<Arc<dyn JsonHandler>>,
    storage: Option<Arc<dyn StorageHandler>>,
    eval: Option<Arc<dyn EvaluationHandler>>,
}

impl delta_kernel::Engine for ComposedEngine {
    fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler> {
        self.eval
            .clone()
            .unwrap_or_else(|| self.inner.evaluation_handler())
    }
    fn storage_handler(&self) -> Arc<dyn StorageHandler> {
        self.storage
            .clone()
            .unwrap_or_else(|| self.inner.storage_handler())
    }
    fn json_handler(&self) -> Arc<dyn JsonHandler> {
        self.json
            .clone()
            .unwrap_or_else(|| self.inner.json_handler())
    }
    fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
        self.parquet
            .clone()
            .unwrap_or_else(|| self.inner.parquet_handler())
    }
}

impl ComposedEngine {
    fn new(
        inner: Arc<dyn delta_kernel::Engine>,
        parquet: Option<JavaParquetCallbacks>,
        json: Option<JavaJsonCallbacks>,
        storage: Option<JavaStorageCallbacks>,
        eval: Option<JavaEvaluationCallbacks>,
    ) -> Self {
        let parquet_handler = parquet.map(|cbs| {
            // Footer/write fall back to the inner engine, mirroring
            // `CallbackEngine::new` in callback_engine.rs.
            let inner_pq = inner.parquet_handler();
            Arc::new(JavaParquetHandler::new(cbs, inner_pq)) as Arc<dyn ParquetHandler>
        });
        Self {
            inner,
            parquet: parquet_handler,
            json: json.map(|cbs| Arc::new(JavaJsonHandler::new(cbs)) as Arc<dyn JsonHandler>),
            storage: storage
                .map(|cbs| Arc::new(JavaStorageHandler::new(cbs)) as Arc<dyn StorageHandler>),
            eval: eval
                .map(|cbs| Arc::new(JavaEvaluationHandler::new(cbs)) as Arc<dyn EvaluationHandler>),
        }
    }
}

/// Build a callback engine where every handler may individually be supplied
/// from Java. Handlers whose pointer is null fall back to the inner engine.
///
/// In the skeleton this fully routes the parquet vtable through
/// [`JavaParquetHandler`]; JSON / Storage / Evaluation vtables are routed
/// through their respective `Java*Handler` impls but those impls are stubs
/// (every method returns a clear "not yet implemented" error). Pass `null`
/// for any handler the connector hasn't supplied to fall back to inner.
///
/// # Safety
/// - `inner_engine` must be a valid handle.
/// - Each non-null handler pointer must point to a valid struct readable by Rust for the duration
///   of this call. The vtable is **copied by value** into the returned engine; the pointer does not
///   need to outlive the call.
/// - Each non-null vtable's function pointers must remain valid for the lifetime of the returned
///   engine.
/// - Each non-null vtable's `engine_state` ownership transfers into the returned engine.
#[cfg(feature = "default-engine-base")]
#[no_mangle]
pub unsafe extern "C" fn make_callback_engine_full(
    inner_engine: Handle<SharedExternEngine>,
    bundle: JavaHandlerBundle,
    allocate_error: AllocateErrorFn,
) -> ExternResult<Handle<SharedExternEngine>> {
    let inner_extern = unsafe { inner_engine.clone_as_arc() };
    let inner = inner_extern.engine();

    // SAFETY: each non-null pointer is required (per the function contract)
    // to point at a valid, fully-initialized vtable struct. We read by
    // value -- the structs are #[repr(C)] bundles of fn pointers + an
    // opaque engine-state pointer; their `unsafe impl Send + Sync` covers
    // the Send requirement for `Arc<dyn Engine>`.
    let parquet = if bundle.parquet.is_null() {
        None
    } else {
        Some(unsafe { std::ptr::read(bundle.parquet) })
    };
    let json = if bundle.json.is_null() {
        None
    } else {
        Some(unsafe { std::ptr::read(bundle.json) })
    };
    let storage = if bundle.storage.is_null() {
        None
    } else {
        Some(unsafe { std::ptr::read(bundle.storage) })
    };
    let eval = if bundle.evaluation.is_null() {
        None
    } else {
        Some(unsafe { std::ptr::read(bundle.evaluation) })
    };

    let composed = ComposedEngine::new(inner, parquet, json, storage, eval);
    let engine: Arc<dyn delta_kernel::Engine> = Arc::new(composed);
    let result: DeltaResult<Handle<SharedExternEngine>> =
        Ok(crate::engine_to_handle(engine, allocate_error));
    unsafe { result.into_extern_result(&allocate_error) }
}

// ============================================================================
// Tests -- exercise the bundle factory + null-fallback routing.
// ============================================================================

#[cfg(all(test, feature = "default-engine-base"))]
mod tests {
    //! End-to-end skeleton verification. Builds a [`ComposedEngine`] with
    //! the parquet vtable routed through the same Rust-shim used by the
    //! parent `callback_engine::tests` (so we know parquet still works) and
    //! the other handlers null (so they fall through to the inner default).
    //! Drives a parquet read end-to-end via `parquet_handler()` and asserts
    //! the same counters the existing tests check.
    //!
    //! This also catches the easy mistake of swapping Java handlers vs inner
    //! handlers in the routing logic.
    use std::collections::HashMap;
    use std::ffi::c_void;
    use std::sync::{Arc, Mutex};

    use delta_kernel::engine::default::storage::store_from_url_opts;
    use delta_kernel::engine::default::DefaultEngineBuilder;
    use delta_kernel::expressions::{ArrayData, ColumnName};
    use delta_kernel::schema::{DataType, StructField, StructType};
    use delta_kernel::{Engine, EngineData};
    use url::Url;

    use super::*;
    use crate::callback_engine::{
        JavaEngineDataResult, JavaParquetCallbacks, JavaParquetReadResult,
    };
    use crate::engine_funcs::FileMeta as FfiFileMeta;

    // -------- Reuse the same shim shape as callback_engine's tests --------

    struct ShimEngineState {
        batch_sizes: Vec<usize>,
        invocations: Arc<Mutex<ShimCounters>>,
    }

    #[derive(Default)]
    struct ShimCounters {
        read_calls: usize,
        next_calls: usize,
        iter_frees: usize,
        data_frees: usize,
        engine_frees: usize,
    }

    struct ShimIterState {
        remaining: Mutex<Vec<usize>>,
    }

    struct StubBatch {
        len: usize,
    }

    impl EngineData for StubBatch {
        fn visit_rows(
            &self,
            _column_names: &[ColumnName],
            _visitor: &mut dyn delta_kernel::engine_data::RowVisitor,
        ) -> DeltaResult<()> {
            Err(Error::generic("StubBatch::visit_rows"))
        }
        fn len(&self) -> usize {
            self.len
        }
        fn append_columns(
            &self,
            _schema: SchemaRef,
            _columns: Vec<ArrayData>,
        ) -> DeltaResult<Box<dyn EngineData>> {
            Err(Error::generic("StubBatch::append_columns"))
        }
        fn apply_selection_vector(
            self: Box<Self>,
            _sv: Vec<bool>,
        ) -> DeltaResult<Box<dyn EngineData>> {
            Err(Error::generic("StubBatch::apply_selection_vector"))
        }
        fn has_field(&self, _name: &ColumnName) -> bool {
            false
        }
    }

    extern "C" fn shim_read(
        engine_state: *mut c_void,
        _files: *const FfiFileMeta,
        _files_len: usize,
        physical_schema: Handle<crate::SharedSchema>,
        out: *mut JavaParquetReadResult,
    ) {
        let _schema = unsafe { physical_schema.into_inner() };
        let state = unsafe { &*(engine_state as *const ShimEngineState) };
        state.invocations.lock().unwrap().read_calls += 1;
        let iter = Box::new(ShimIterState {
            remaining: Mutex::new(state.batch_sizes.clone()),
        });
        unsafe {
            *out = JavaParquetReadResult {
                iter_state: Box::into_raw(iter) as *mut c_void,
                error: 0,
            };
        }
    }

    extern "C" fn shim_next(
        engine_state: *mut c_void,
        iter_state: *mut c_void,
        out: *mut JavaEngineDataResult,
    ) {
        let state = unsafe { &*(engine_state as *const ShimEngineState) };
        state.invocations.lock().unwrap().next_calls += 1;
        let iter = unsafe { &*(iter_state as *const ShimIterState) };
        let mut remaining = iter.remaining.lock().unwrap();
        let result = if remaining.is_empty() {
            JavaEngineDataResult {
                batch: std::ptr::null_mut(),
                len: 0,
                done: true,
                error: 0,
            }
        } else {
            let len = remaining.remove(0);
            let batch = Box::new(StubBatch { len });
            JavaEngineDataResult {
                batch: Box::into_raw(batch) as *mut c_void,
                len,
                done: false,
                error: 0,
            }
        };
        unsafe { *out = result };
    }

    extern "C" fn shim_iter_free(engine_state: *mut c_void, iter_state: *mut c_void) {
        let state = unsafe { &*(engine_state as *const ShimEngineState) };
        state.invocations.lock().unwrap().iter_frees += 1;
        if !iter_state.is_null() {
            unsafe { drop(Box::from_raw(iter_state as *mut ShimIterState)) };
        }
    }

    extern "C" fn shim_free_data(engine_state: *mut c_void, batch: *mut c_void) {
        let state = unsafe { &*(engine_state as *const ShimEngineState) };
        state.invocations.lock().unwrap().data_frees += 1;
        if !batch.is_null() {
            unsafe { drop(Box::from_raw(batch as *mut StubBatch)) };
        }
    }

    extern "C" fn shim_free_engine_state(engine_state: *mut c_void) {
        if !engine_state.is_null() {
            {
                let state = unsafe { &*(engine_state as *const ShimEngineState) };
                state.invocations.lock().unwrap().engine_frees += 1;
            }
            unsafe { drop(Box::from_raw(engine_state as *mut ShimEngineState)) };
        }
    }

    fn make_inner() -> Arc<dyn Engine> {
        let url = Url::parse("memory:///doesntmatter/").unwrap();
        let store = store_from_url_opts(&url, HashMap::<String, String>::new()).unwrap();
        Arc::new(DefaultEngineBuilder::new(store).build())
    }

    fn shim_callbacks(state: Box<ShimEngineState>) -> JavaParquetCallbacks {
        JavaParquetCallbacks {
            engine_state: Box::into_raw(state) as *mut c_void,
            read_parquet_files: shim_read,
            iter_next: shim_next,
            iter_free: shim_iter_free,
            free_data: shim_free_data,
            free_engine_state: shim_free_engine_state,
        }
    }

    fn synthetic_files() -> Vec<KernelFileMeta> {
        vec![KernelFileMeta {
            location: Url::parse("memory:///x/y.parquet").unwrap(),
            last_modified: 0,
            size: 0,
        }]
    }

    fn synthetic_schema() -> SchemaRef {
        Arc::new(StructType::try_new([StructField::nullable("id", DataType::INTEGER)]).unwrap())
    }

    /// End-to-end test: build a ComposedEngine via the bundle factory with
    /// only parquet supplied, drive `parquet_handler()`, and assert every
    /// counter behaves the same as the parquet-only `make_callback_engine`.
    #[test]
    fn composed_engine_routes_parquet_through_java_handler() {
        let counters = Arc::new(Mutex::new(ShimCounters::default()));
        let shim = Box::new(ShimEngineState {
            batch_sizes: vec![5, 7, 3],
            invocations: counters.clone(),
        });
        let cbs = shim_callbacks(shim);
        let composed = ComposedEngine::new(make_inner(), Some(cbs), None, None, None);

        let mut iter = composed
            .parquet_handler()
            .read_parquet_files(&synthetic_files(), synthetic_schema(), None)
            .expect("read should succeed");
        let mut total = 0usize;
        let mut batches = 0usize;
        for b in iter.by_ref() {
            let b = b.expect("ok");
            total += b.len();
            batches += 1;
        }
        drop(iter);
        drop(composed);

        let c = counters.lock().unwrap();
        assert_eq!(c.read_calls, 1);
        assert_eq!(c.next_calls, 4); // 3 batches + 1 done
        assert_eq!(c.iter_frees, 1);
        assert_eq!(c.data_frees, 3);
        assert_eq!(c.engine_frees, 1);
        assert_eq!(batches, 3);
        assert_eq!(total, 5 + 7 + 3);
    }

    /// Null-fallback verification: with no Java handlers, every accessor
    /// returns the inner engine's handler directly.
    #[test]
    fn composed_engine_falls_back_to_inner_when_all_null() {
        let inner = make_inner();
        let composed = ComposedEngine::new(inner.clone(), None, None, None, None);
        // Same Arc identity for json/storage/eval (these are cheap to
        // re-acquire from the inner default each call so identity isn't
        // guaranteed; just check that we get a valid handler back).
        // Parquet returns inner's handler too because we passed None.
        assert!(Arc::ptr_eq(
            &composed.parquet_handler(),
            &inner.parquet_handler()
        ));
    }

    /// Verify that calling into a Java JsonHandler (whose impl is a stub)
    /// surfaces a clear error -- proves the routing reaches the stubbed
    /// handler instead of silently using inner.
    #[test]
    fn composed_engine_json_routes_through_java_handler() {
        // The JSON handler now actually upcalls into the vtable. This test
        // supplies a `read_json_files` callback that signals an engine
        // error (error=42) and verifies the kernel surfaces it. Catches
        // routing regressions where the JSON path silently falls through
        // to the inner default.
        extern "C" fn read_returns_error(
            _: *mut c_void,
            _: *const FfiFileMeta,
            _: usize,
            _: KernelStringSlice,
            _: *mut c_void,
            out: *mut JavaParquetReadResult,
        ) {
            unsafe {
                *out = JavaParquetReadResult {
                    iter_state: std::ptr::null_mut(),
                    error: 42,
                };
            }
        }
        extern "C" fn unreachable_parse(
            _: *mut c_void,
            _: *mut c_void,
            _: KernelStringSlice,
            _: *mut JavaBatchResult,
        ) {
            unreachable!()
        }
        extern "C" fn unreachable_write(
            _: *mut c_void,
            _: KernelStringSlice,
            _: *mut c_void,
            _: bool,
            _: *mut u32,
        ) {
            unreachable!()
        }
        extern "C" fn unreachable_iter_next(
            _: *mut c_void,
            _: *mut c_void,
            _: *mut JavaEngineDataResult,
        ) {
            unreachable!()
        }
        extern "C" fn unreachable_iter_free(_: *mut c_void, _: *mut c_void) {
            unreachable!()
        }
        extern "C" fn unreachable_materialize(
            _: *mut c_void,
            _: *const VisitRowsRequest,
            _: *mut VisitRowsResult,
        ) {
            unreachable!()
        }
        extern "C" fn unreachable_free_columns(_: *mut c_void, _: *const ColumnBuffers, _: usize) {
            unreachable!()
        }
        extern "C" fn unreachable_apply_sv(
            _: *mut c_void,
            _: *mut c_void,
            _: *const u8,
            _: usize,
            _: *mut SelectionResult,
        ) {
            unreachable!()
        }
        extern "C" fn unreachable_append_columns(
            _: *mut c_void,
            _: *mut c_void,
            _: KernelStringSlice,
            _: *const ColumnBuffers,
            _: usize,
            _: *mut SelectionResult,
        ) {
            unreachable!()
        }
        extern "C" fn unreachable_free_batch(_: *mut c_void, _: *mut c_void) {}
        extern "C" fn unreachable_batch_len(_: *mut c_void, _: *mut c_void) -> usize {
            0
        }
        extern "C" fn unreachable_has_field(
            _: *mut c_void,
            _: *mut c_void,
            _: KernelStringSlice,
        ) -> bool {
            false
        }
        extern "C" fn noop_free_engine(_: *mut c_void) {}

        let json_callbacks = JavaJsonCallbacks {
            engine_state: std::ptr::null_mut(),
            parse_json: unreachable_parse,
            read_json_files: read_returns_error,
            write_json_file: unreachable_write,
            iter_next: unreachable_iter_next,
            iter_free: unreachable_iter_free,
            engine_data_callbacks: JavaEngineDataCallbacks {
                materialize_columns: unreachable_materialize,
                free_columns: unreachable_free_columns,
                apply_selection_vector: unreachable_apply_sv,
                append_columns: unreachable_append_columns,
                free_batch: unreachable_free_batch,
                batch_len: unreachable_batch_len,
                has_field: unreachable_has_field,
            },
            free_engine_state: noop_free_engine,
        };
        let composed = ComposedEngine::new(make_inner(), None, Some(json_callbacks), None, None);
        let json_handler = composed.json_handler();
        let result = json_handler.read_json_files(&[], synthetic_schema(), None);
        let err = match result {
            Err(e) => e,
            Ok(_) => panic!("Java JSON callback returned error 42; should surface"),
        };
        assert!(
            format!("{err}").contains("error code 42"),
            "expected error code 42, got: {err}"
        );
    }
}
