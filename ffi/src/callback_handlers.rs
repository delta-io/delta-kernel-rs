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
use std::sync::Arc;

use bytes::Bytes;
use delta_kernel::engine_data::RowVisitor;
use delta_kernel::expressions::{ArrayData, ColumnName};
use delta_kernel::schema::{DataType, SchemaRef};
use delta_kernel::{
    DeltaResult, EngineData, Error, EvaluationHandler, ExpressionEvaluator, ExpressionRef,
    FileDataReadResultIterator, FileMeta as KernelFileMeta, FileSlice, FilteredEngineData,
    JsonHandler, PredicateEvaluator, PredicateRef, StorageHandler,
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
        _column_names: &[ColumnName],
        _visitor: &mut dyn RowVisitor,
    ) -> DeltaResult<()> {
        // SKELETON: production impl materializes columns via the engine-data
        // callbacks, builds GetData adapters, and invokes the visitor. See
        // module docs for the full protocol.
        Err(Error::generic(
            "JavaEngineData::visit_rows: skeleton -- column-materialization adapters not yet \
             implemented. Filling in this method is the next milestone for option-2 wire-up.",
        ))
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
        _selection_vector: Vec<bool>,
    ) -> DeltaResult<Box<dyn EngineData>> {
        Err(Error::generic(
            "JavaEngineData::apply_selection_vector: skeleton -- selection-vector marshalling \
             not yet implemented.",
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
        Err(Error::generic(
            "JavaJsonHandler::parse_json: skeleton -- input-batch handle marshalling and \
             column-materialization protocol not yet wired through.",
        ))
    }

    fn read_json_files(
        &self,
        _files: &[KernelFileMeta],
        _physical_schema: SchemaRef,
        _predicate: Option<PredicateRef>,
    ) -> DeltaResult<FileDataReadResultIterator> {
        Err(Error::generic(
            "JavaJsonHandler::read_json_files: skeleton -- file/schema marshalling and \
             iterator wiring not yet implemented.",
        ))
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

/// Build a callback engine where every handler may individually be supplied
/// from Java. Handlers whose pointer is null fall back to the inner engine.
///
/// **Skeleton.** Real engine wiring routes through the existing
/// `make_callback_engine` for parquet only; JSON / Storage / Eval pointers
/// are accepted and stored but the impl panics if the kernel actually calls
/// into a stubbed handler. Filling in the implementations is follow-up
/// work.
///
/// # Safety
/// - `inner_engine` must be a valid handle.
/// - Each non-null handler pointer must remain valid for the lifetime of the returned engine.
#[cfg(feature = "default-engine-base")]
#[no_mangle]
pub unsafe extern "C" fn make_callback_engine_full(
    _inner_engine: Handle<SharedExternEngine>,
    _bundle: JavaHandlerBundle,
    _allocate_error: AllocateErrorFn,
) -> ExternResult<Handle<SharedExternEngine>> {
    // Skeleton: real impl will read each non-null bundle pointer, build
    // the corresponding Java*Handler, compose into a new CallbackEngine
    // with the inner engine for fallback. Until JSON / Storage / Eval
    // handlers are filled in, callers should keep using
    // `make_callback_engine`.
    let result: DeltaResult<Handle<SharedExternEngine>> = Err(Error::generic(
        "make_callback_engine_full: skeleton -- only `make_callback_engine` (parquet-only) is \
         wired today; JSON / Storage / Evaluation vtables are accepted but not yet routed.",
    ));
    unsafe { result.into_extern_result(&_allocate_error) }
}
