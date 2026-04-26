//! Callback-driven [`Engine`] FFI shim.
//!
//! This module exposes a Rust [`Engine`] whose [`ParquetHandler`] is implemented
//! by an engine-supplied vtable of `extern "C" fn` pointers. The remaining
//! handlers (storage, json, evaluation) come from a wrapped inner engine
//! (typically the default Rust engine), so the connector only needs to
//! re-implement the parquet read path on its own side.
//!
//! ============================================================================
//!  Wire shape (see [`JavaParquetCallbacks`])
//! ============================================================================
//!
//! - `read_parquet_files`: kernel hands the engine a contiguous `[FileMeta]` plus a
//!   `Handle<SharedSchema>` (callback consumes both) and gets back an opaque iterator handle.
//! - `iter_next`: kernel pumps batches off the iterator until the engine signals end-of-stream.
//! - `iter_free`: kernel releases the iterator state when iteration finishes (or aborts).
//! - `free_data`: per-batch release callback. The kernel never inspects the batch payload -- it
//!   just hands the opaque handle back to the engine via the existing FFI surface (e.g.
//!   [`read_result_next`]).
//! - `free_engine_state`: invoked once when the last [`CallbackEngine`] / handler reference drops.
//!
//! POC scope: only the read path; predicates and write are deferred. See
//! `~/.claude/notes/java-engine-trait-upcalls.md` for the full design.
//!
//! [`read_result_next`]: crate::engine_funcs::read_result_next

use std::ffi::c_void;
use std::sync::Arc;

use delta_kernel::expressions::ColumnName;
use delta_kernel::schema::SchemaRef;
use delta_kernel::{
    DeltaResult, Engine, EngineData, Error, EvaluationHandler, FileDataReadResultIterator,
    FileMeta as KernelFileMeta, JsonHandler, ParquetFooter, ParquetHandler, PredicateRef,
    StorageHandler,
};
use tracing::debug;

// Reference the FFI `FileMeta` by its concrete name so cbindgen emits a single,
// consistent `FileMeta` typedef in the generated header. (An alias here would
// surface as an undeclared type in the function-pointer signature -- cbindgen
// tracks declarations by Rust path, not local alias.)
use crate::engine_funcs::FileMeta;
use crate::handle::Handle;
use crate::{
    AllocateErrorFn, ExternResult, IntoExternResult, KernelStringSlice, SharedExternEngine,
    SharedSchema,
};

// ============================================================================
// FFI types -- vtable that the connector fills in
// ============================================================================

/// Result of a single [`JavaParquetCallbacks::read_parquet_files`] call.
#[repr(C)]
pub struct JavaParquetReadResult {
    /// Engine-managed iterator state. Opaque to Rust; passed back verbatim to
    /// `iter_next` / `iter_free`. Must be non-null on success and is
    /// **ignored** when `error != 0`.
    pub iter_state: *mut c_void,
    /// `0` on success; non-zero indicates the engine signalled an error and
    /// the iterator state will not be pumped or freed by Rust. The actual
    /// error string is conveyed via the inner engine's error allocator on the
    /// connector side -- this field is only a binary success/fail signal.
    pub error: u32,
}

/// Result of a single [`JavaParquetCallbacks::iter_next`] call.
#[repr(C)]
pub struct JavaEngineDataResult {
    /// Engine-managed batch handle. Null iff `done == true`.
    pub batch: *mut c_void,
    /// Number of rows in the batch. Zero iff `done == true`.
    pub len: usize,
    /// Set by the engine to `true` when no more batches remain. When `true`,
    /// the kernel will not call `iter_next` again on this iterator and will
    /// shortly call `iter_free`.
    pub done: bool,
    /// `0` on success.
    pub error: u32,
}

/// Function-pointer table the connector supplies to drive the parquet handler
/// from outside Rust.
///
/// All pointers are required and must remain valid for the lifetime of the
/// resulting [`CallbackEngine`]. The `engine_state` pointer is opaque to the
/// kernel and is supplied as the first argument to every callback.
#[repr(C)]
pub struct JavaParquetCallbacks {
    /// Engine-managed opaque pointer threaded through every callback.
    pub engine_state: *mut c_void,
    /// Read parquet files. The kernel hands the callback a contiguous array
    /// of [`FileMeta`] (length given by `files_len`) and a shared schema
    /// handle the engine **consumes** (must drop via the standard
    /// `free_schema` FFI before returning, or [`Handle::into_inner`] when
    /// implemented in Rust).
    ///
    /// Predicates are intentionally absent in this POC; engines must apply
    /// their own filtering.
    ///
    /// The result is written into the kernel-allocated `out` buffer. This
    /// out-pointer style (rather than returning the struct by value) keeps
    /// the FFM upcall surface portable across ABIs -- struct-by-value
    /// returns force the engine into either register-pair or sret marshalling
    /// depending on size, and FFM's handling of small-struct returns from
    /// upcalls is platform-specific.
    pub read_parquet_files: extern "C" fn(
        engine_state: *mut c_void,
        files: *const FileMeta,
        files_len: usize,
        physical_schema: Handle<SharedSchema>,
        out: *mut JavaParquetReadResult,
    ),
    /// Pull the next batch from an iterator state previously returned by
    /// `read_parquet_files`. Setting `out.done = true` signals end-of-stream;
    /// the kernel will then call `iter_free` and not invoke `iter_next` again.
    pub iter_next: extern "C" fn(
        engine_state: *mut c_void,
        iter_state: *mut c_void,
        out: *mut JavaEngineDataResult,
    ),
    /// Free an iterator state. Called exactly once per successful
    /// `read_parquet_files` invocation, after iteration completes (or is
    /// aborted by the kernel dropping the iterator).
    pub iter_free: extern "C" fn(engine_state: *mut c_void, iter_state: *mut c_void),
    /// Free a batch handle returned by `iter_next` once the kernel is done
    /// forwarding it. Engines are free to refcount on their side.
    pub free_data: extern "C" fn(engine_state: *mut c_void, batch: *mut c_void),
    /// Free the engine state itself. Invoked exactly once, when the last
    /// [`CallbackEngine`] / [`JavaParquetHandler`] reference is dropped.
    pub free_engine_state: extern "C" fn(engine_state: *mut c_void),
}

// SAFETY: the vtable is a struct of plain function pointers plus an opaque
// engine-managed pointer. The kernel never dereferences `engine_state`; it
// only forwards it back to the engine. Thread safety of the underlying state
// is the engine's concern -- mirrors the contract documented on
// `ExternEngineVtable`.
unsafe impl Send for JavaParquetCallbacks {}
unsafe impl Sync for JavaParquetCallbacks {}

// ============================================================================
// Pass-through engine data
// ============================================================================

/// Pass-through [`EngineData`] holding an engine-owned batch handle.
///
/// The kernel never inspects the payload; it forwards the handle straight
/// back to the engine through the existing FFI iterator surface
/// (e.g. [`read_result_next`]). On drop, the wrapper calls back into the
/// engine to release the batch.
///
/// Methods that would actually require Rust to understand the payload
/// (`visit_rows`, `append_columns`, `apply_selection_vector`) return
/// "unsupported" errors: the read-path POC keeps batches opaque end-to-end,
/// and any code path that would inspect them would be a sign that the wrong
/// engine was wired in.
///
/// **Lifetime safety**: holds an `Arc<JavaParquetHandler>` to keep the
/// engine state alive even if the iterator that produced this batch -- and
/// even the originating engine handle -- have already been freed by the
/// time the batch drops. Without this Arc the `free` callback would
/// dereference freed engine state, which is the kind of UAF that makes
/// FFM crashes maximally hard to debug.
///
/// [`read_result_next`]: crate::engine_funcs::read_result_next
pub struct OpaqueJavaEngineData {
    batch: *mut c_void,
    len: usize,
    /// Keep the engine state alive for the duration of this batch. The Arc
    /// clone is one refcount bump per batch -- negligible cost compared to
    /// the alternative of UAF when batches outlive their iterator.
    state: Arc<HandlerState>,
}

// SAFETY: same justification as `JavaParquetCallbacks`. Engine is responsible
// for any thread safety on its side.
unsafe impl Send for OpaqueJavaEngineData {}
unsafe impl Sync for OpaqueJavaEngineData {}

impl Drop for OpaqueJavaEngineData {
    fn drop(&mut self) {
        if !self.batch.is_null() {
            (self.state.callbacks.free_data)(self.state.callbacks.engine_state, self.batch);
        }
    }
}

impl EngineData for OpaqueJavaEngineData {
    fn visit_rows(
        &self,
        _column_names: &[ColumnName],
        _visitor: &mut dyn delta_kernel::engine_data::RowVisitor,
    ) -> DeltaResult<()> {
        Err(Error::generic(
            "OpaqueJavaEngineData is opaque to the Rust kernel; the connector must \
             extract its rows via the engine handler that produced it.",
        ))
    }

    fn len(&self) -> usize {
        self.len
    }

    fn append_columns(
        &self,
        _schema: SchemaRef,
        _columns: Vec<delta_kernel::expressions::ArrayData>,
    ) -> DeltaResult<Box<dyn EngineData>> {
        Err(Error::generic(
            "OpaqueJavaEngineData::append_columns is not supported.",
        ))
    }

    fn apply_selection_vector(
        self: Box<Self>,
        _selection_vector: Vec<bool>,
    ) -> DeltaResult<Box<dyn EngineData>> {
        Err(Error::generic(
            "OpaqueJavaEngineData::apply_selection_vector is not supported by the \
             read-path POC; route DV-bearing scans through the JVM backend instead.",
        ))
    }

    fn has_field(&self, _name: &ColumnName) -> bool {
        // Conservative: returning false ensures the kernel's scan path will
        // never try to traverse the opaque payload looking for nested fields.
        false
    }
}

// ============================================================================
// JavaParquetHandler -- impl ParquetHandler over the vtable
// ============================================================================

/// State shared between [`JavaParquetHandler`], every [`JavaParquetIter`] it
/// produces, and every [`OpaqueJavaEngineData`] those iterators emit. The
/// engine state is freed exactly once -- when the last of these holders
/// drops -- via this struct's `Drop` impl.
///
/// Splitting the state out of `JavaParquetHandler` is what gives iterators
/// and batches a typed lifetime tie-in: each holds an `Arc<HandlerState>`
/// rather than a raw pointer to engine memory. This makes the "engine state
/// outlives any batch it produced" invariant explicit instead of conventional.
struct HandlerState {
    callbacks: JavaParquetCallbacks,
    /// Underlying engine's parquet handler. Used to satisfy
    /// `read_parquet_footer` (called by the read path during V2 checkpoint
    /// loading) and `write_parquet_file`. Without this delegate, the
    /// callback engine would break snapshot loads on V2-checkpointed tables.
    inner: Arc<dyn ParquetHandler>,
}

// SAFETY: the callbacks struct is already declared Send + Sync above; the
// inner ParquetHandler is required to be Send + Sync by the trait. So
// HandlerState is composable.
unsafe impl Send for HandlerState {}
unsafe impl Sync for HandlerState {}

impl Drop for HandlerState {
    fn drop(&mut self) {
        debug!("HandlerState dropping; releasing engine state");
        (self.callbacks.free_engine_state)(self.callbacks.engine_state);
    }
}

/// [`ParquetHandler`] that forwards `read_parquet_files` to a connector-supplied
/// vtable. Holds an `Arc<HandlerState>`; cloning the handler bumps the state
/// refcount, and so does cloning into iterators / batches.
///
/// Footer reads and writes are delegated to the inner engine's handler so
/// V2 checkpoint reads continue to work (the kernel calls
/// `read_parquet_footer` during snapshot loading) and so future write-path
/// integrations don't have to touch this scaffolding.
pub struct JavaParquetHandler {
    state: Arc<HandlerState>,
}

impl JavaParquetHandler {
    /// `inner` is the inner engine's parquet handler -- used for the
    /// non-bulk-read operations (`read_parquet_footer`, `write_parquet_file`)
    /// the connector does not yet implement on its side.
    ///
    /// The callback's `free_engine_state` MUST be safe to call concurrently
    /// with no other engine-state-touching callbacks in flight; the kernel
    /// guarantees that no iterator or batch outlives the last
    /// `Arc<HandlerState>` reference (because each holds one).
    pub fn new(callbacks: JavaParquetCallbacks, inner: Arc<dyn ParquetHandler>) -> Self {
        Self {
            state: Arc::new(HandlerState { callbacks, inner }),
        }
    }
}

impl ParquetHandler for JavaParquetHandler {
    fn read_parquet_files(
        &self,
        files: &[KernelFileMeta],
        physical_schema: SchemaRef,
        _predicate: Option<PredicateRef>,
    ) -> DeltaResult<FileDataReadResultIterator> {
        // Marshal &[KernelFileMeta] -> [FileMeta]. The KernelStringSlice
        // borrows are valid for the duration of this call (which is when the
        // engine dereferences them); after returning, the engine has produced
        // an iterator state that retains paths in its own memory if needed.
        //
        // CONTRACT: the engine MUST copy any path bytes it needs to retain
        // past the synchronous return of this callback. Path slices are
        // backed by `files: &[KernelFileMeta]` whose lifetime ends when this
        // function returns to the kernel.
        let ffi_files: Vec<FileMeta> = files
            .iter()
            .map(|fm| {
                let url = fm.location.as_str();
                let size: usize = fm.size.try_into().map_err(|_| {
                    Error::generic("FileMeta::size does not fit in usize on this platform")
                })?;
                Ok(FileMeta {
                    // SAFETY: `url` is a borrow of `fm.location`, which lives
                    // for the lifetime of `files: &[KernelFileMeta]`. The borrow
                    // is valid through the synchronous call into the engine.
                    path: unsafe { KernelStringSlice::new_unsafe(url) },
                    last_modified: fm.last_modified,
                    size,
                })
            })
            .collect::<DeltaResult<Vec<_>>>()?;

        // Hand the schema across as a shared handle. Callback contract: the
        // engine MUST consume it exactly once (via `Handle::into_inner`, or
        // by calling `free_schema` on the FFI side), regardless of whether
        // the call succeeds or returns an error code. Failing to consume
        // leaks the underlying `Arc<Schema>`.
        let schema_handle: Handle<SharedSchema> = physical_schema.into();

        let mut result = JavaParquetReadResult {
            iter_state: std::ptr::null_mut(),
            error: 0,
        };
        (self.state.callbacks.read_parquet_files)(
            self.state.callbacks.engine_state,
            ffi_files.as_ptr(),
            ffi_files.len(),
            schema_handle,
            &mut result as *mut _,
        );
        if result.error != 0 {
            return Err(Error::generic(format!(
                "JavaParquetHandler.read_parquet_files signalled error code {}",
                result.error
            )));
        }
        if result.iter_state.is_null() {
            return Err(Error::generic(
                "JavaParquetHandler.read_parquet_files returned a null iterator state on success",
            ));
        }
        Ok(Box::new(JavaParquetIter {
            iter_state: result.iter_state,
            // Clone the Arc so the iterator -- and any batches it emits --
            // keeps the engine state alive even if the engine handle and
            // every other `JavaParquetHandler` reference drop first.
            state: self.state.clone(),
            done: false,
        }))
    }

    fn write_parquet_file(
        &self,
        location: url::Url,
        data: Box<dyn Iterator<Item = DeltaResult<Box<dyn EngineData>>> + Send>,
    ) -> DeltaResult<()> {
        // Delegate to inner; transaction commits and checkpoint writes go
        // through this. The connector POC only re-implements bulk reads.
        self.state.inner.write_parquet_file(location, data)
    }

    fn read_parquet_footer(&self, file: &KernelFileMeta) -> DeltaResult<ParquetFooter> {
        // Delegate to inner. The kernel calls this during V2 checkpoint
        // schema discovery; if we returned an error here, snapshot loading
        // would fail on every V2-checkpointed table.
        self.state.inner.read_parquet_footer(file)
    }
}

// ============================================================================
// Iterator wrapper -- pumps the engine-side iterator
// ============================================================================

struct JavaParquetIter {
    iter_state: *mut c_void,
    /// Keep the handler state alive for the lifetime of this iterator and
    /// every batch it emits. When the last `Arc<HandlerState>` drops, the
    /// engine state is freed exactly once.
    state: Arc<HandlerState>,
    done: bool,
}

// SAFETY: identical justification to `JavaParquetCallbacks`.
unsafe impl Send for JavaParquetIter {}

impl Drop for JavaParquetIter {
    fn drop(&mut self) {
        if !self.iter_state.is_null() {
            (self.state.callbacks.iter_free)(self.state.callbacks.engine_state, self.iter_state);
        }
    }
}

impl Iterator for JavaParquetIter {
    type Item = DeltaResult<Box<dyn EngineData>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let mut res = JavaEngineDataResult {
            batch: std::ptr::null_mut(),
            len: 0,
            done: false,
            error: 0,
        };
        (self.state.callbacks.iter_next)(
            self.state.callbacks.engine_state,
            self.iter_state,
            &mut res as *mut _,
        );
        if res.error != 0 {
            self.done = true;
            return Some(Err(Error::generic(format!(
                "JavaParquetIter::next signalled error code {}",
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
                "JavaParquetIter::next returned a null batch with done=false",
            )));
        }
        let batch: Box<dyn EngineData> = Box::new(OpaqueJavaEngineData {
            batch: res.batch,
            len: res.len,
            // Clone the Arc so the batch keeps engine state alive even if
            // the iterator drops before the kernel finishes forwarding the
            // batch downstream. One refcount bump per batch -- negligible.
            state: self.state.clone(),
        });
        Some(Ok(batch))
    }
}

// ============================================================================
// CallbackEngine -- composes inner default engine + Java parquet handler
// ============================================================================

/// An [`Engine`] whose parquet operations are forwarded to engine-supplied
/// callbacks; all other handlers come from the inner [`Engine`] passed at
/// construction time.
pub struct CallbackEngine {
    inner: Arc<dyn Engine>,
    parquet: Arc<JavaParquetHandler>,
}

impl Engine for CallbackEngine {
    fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler> {
        self.inner.evaluation_handler()
    }
    fn storage_handler(&self) -> Arc<dyn StorageHandler> {
        self.inner.storage_handler()
    }
    fn json_handler(&self) -> Arc<dyn JsonHandler> {
        self.inner.json_handler()
    }
    fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
        self.parquet.clone()
    }
}

impl CallbackEngine {
    /// Build a [`CallbackEngine`] from a plain `Arc<dyn Engine>`. Useful for
    /// pure-Rust unit tests that don't go through the FFI factory.
    ///
    /// The inner engine's parquet handler is captured up front (via
    /// [`Engine::parquet_handler`]) and threaded into the [`JavaParquetHandler`]
    /// for footer-read / write delegation. This keeps the read path working
    /// for V2-checkpointed tables.
    pub fn new(inner: Arc<dyn Engine>, callbacks: JavaParquetCallbacks) -> Self {
        let inner_parquet = inner.parquet_handler();
        Self {
            inner,
            parquet: Arc::new(JavaParquetHandler::new(callbacks, inner_parquet)),
        }
    }
}

// ============================================================================
// FFI factory
// ============================================================================

/// Build a [`CallbackEngine`] backed by the supplied inner engine for
/// non-parquet handlers and the supplied vtable for parquet reads. Returns a
/// fresh shared engine handle the caller is responsible for freeing via
/// [`free_engine`].
///
/// The `inner_engine` handle is not consumed; the caller is still responsible
/// for freeing it. (The factory bumps its refcount internally.)
///
/// [`free_engine`]: crate::free_engine
///
/// # Safety
/// - `inner_engine` must be a valid handle returned by [`get_default_engine`] or [`builder_build`].
/// - The function pointers inside `callbacks` must remain valid for the lifetime of the returned
///   engine.
/// - `engine_state` ownership transfers into the new engine; the caller must not free it
///   independently.
///
/// [`get_default_engine`]: crate::get_default_engine
/// [`builder_build`]: crate::builder_build
#[cfg(feature = "default-engine-base")]
#[no_mangle]
pub unsafe extern "C" fn make_callback_engine(
    inner_engine: Handle<SharedExternEngine>,
    callbacks: JavaParquetCallbacks,
    allocate_error: AllocateErrorFn,
) -> ExternResult<Handle<SharedExternEngine>> {
    let inner_extern = unsafe { inner_engine.clone_as_arc() };
    let inner = inner_extern.engine();
    let engine: Arc<dyn Engine> = Arc::new(CallbackEngine::new(inner, callbacks));
    let result: DeltaResult<Handle<SharedExternEngine>> =
        Ok(crate::engine_to_handle(engine, allocate_error));
    unsafe { result.into_extern_result(&allocate_error) }
}

// ============================================================================
// Test driver -- exercises the parquet handler from outside the kernel scan
// ============================================================================

/// Result of [`test_drive_callback_parquet_handler`]: the number of batches
/// the engine emitted and the sum of their reported row counts.
#[repr(C)]
pub struct CallbackParquetDriveResult {
    pub batch_count: usize,
    pub total_rows: usize,
}

/// Drive `engine.parquet_handler().read_parquet_files(...)` from Rust with a
/// fixed synthetic schema and one synthetic [`FileMeta`], then pump the
/// resulting iterator to exhaustion and write a `(batch_count, total_rows)`
/// summary into `out`.
///
/// Intended for Java integration tests of the [`CallbackEngine`]: the Java
/// side wires up a vtable, calls [`make_callback_engine`], and invokes this
/// function instead of a full snapshot scan to verify the upcall plumbing
/// without having to round-trip schemas, file metadata, or an actual Delta
/// log through the boundary.
///
/// # Safety
/// - `engine` must be a valid handle returned by [`make_callback_engine`].
/// - `out` must point to writable memory of at least `sizeof(CallbackParquetDriveResult)` bytes.
#[cfg(feature = "default-engine-base")]
#[no_mangle]
pub unsafe extern "C" fn test_drive_callback_parquet_handler(
    engine: Handle<SharedExternEngine>,
    out: *mut CallbackParquetDriveResult,
) -> ExternResult<bool> {
    let extern_engine = unsafe { engine.clone_as_arc() };
    let result = drive_parquet_handler_impl(extern_engine.engine());
    unsafe {
        match &result {
            Ok((bc, tr)) => {
                *out = CallbackParquetDriveResult {
                    batch_count: *bc,
                    total_rows: *tr,
                };
            }
            Err(_) => {
                *out = CallbackParquetDriveResult {
                    batch_count: 0,
                    total_rows: 0,
                };
            }
        }
    }
    unsafe {
        result
            .map(|_| true)
            .into_extern_result(&extern_engine.as_ref())
    }
}

#[cfg(feature = "default-engine-base")]
fn drive_parquet_handler_impl(engine: Arc<dyn Engine>) -> DeltaResult<(usize, usize)> {
    use delta_kernel::schema::{DataType, StructField, StructType};

    let synthetic_file = KernelFileMeta {
        location: url::Url::parse("memory:///doesntmatter/test.parquet")
            .map_err(|e| Error::generic(format!("bad url: {e}")))?,
        last_modified: 0,
        size: 0,
    };
    let synthetic_schema: SchemaRef = Arc::new(
        StructType::try_new([StructField::nullable("id", DataType::INTEGER)])
            .map_err(|e| Error::generic(format!("bad schema: {e}")))?,
    );
    let mut iter = engine.parquet_handler().read_parquet_files(
        std::slice::from_ref(&synthetic_file),
        synthetic_schema,
        None,
    )?;
    let mut batch_count = 0usize;
    let mut total_rows = 0usize;
    for batch in iter.by_ref() {
        let batch = batch?;
        batch_count += 1;
        total_rows += batch.len();
    }
    Ok((batch_count, total_rows))
}

// ============================================================================
// Tests -- pure-Rust shim that simulates the engine side of the vtable
// ============================================================================

#[cfg(all(test, feature = "default-engine-base"))]
mod tests {
    //! Pure-Rust unit test for the callback engine. The "engine" side of the
    //! vtable is implemented by Rust functions that hand back a synthetic
    //! `EngineData` -- this proves the wire shape end-to-end without
    //! requiring a real parquet file or a JVM.

    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use delta_kernel::engine::default::storage::store_from_url_opts;
    use delta_kernel::engine::default::DefaultEngineBuilder;
    use delta_kernel::expressions::{ArrayData, ColumnName};
    use delta_kernel::schema::{DataType, StructField, StructType};
    use delta_kernel::Engine;
    use url::Url;

    use super::*;

    /// Engine-side state held opaquely by Rust on behalf of the "Java"
    /// connector. The synthetic batch sizes the shim should yield for each
    /// `read_parquet_files` call.
    struct ShimEngineState {
        batch_sizes: Vec<usize>,
        /// Counters observable by the test, to assert the kernel actually
        /// invoked each callback.
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

    /// Engine-side iterator state: emit the queued batch sizes one at a time.
    struct ShimIterState {
        remaining: Mutex<Vec<usize>>,
    }

    /// Synthetic engine data the kernel never inspects. Standalone (not
    /// `OpaqueJavaEngineData`) so the test can drop it directly without
    /// going through the engine free callback.
    struct StubBatch {
        len: usize,
    }

    impl EngineData for StubBatch {
        fn visit_rows(
            &self,
            _column_names: &[ColumnName],
            _visitor: &mut dyn delta_kernel::engine_data::RowVisitor,
        ) -> DeltaResult<()> {
            Err(Error::generic("StubBatch::visit_rows: opaque test data"))
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
            _selection_vector: Vec<bool>,
        ) -> DeltaResult<Box<dyn EngineData>> {
            Err(Error::generic("StubBatch::apply_selection_vector"))
        }
        fn has_field(&self, _name: &ColumnName) -> bool {
            false
        }
    }

    extern "C" fn shim_read_parquet_files(
        engine_state: *mut c_void,
        _files: *const FileMeta,
        _files_len: usize,
        physical_schema: Handle<SharedSchema>,
        out: *mut JavaParquetReadResult,
    ) {
        // Consume the schema handle (-1 refcount) -- mirrors what the Java
        // FFM stub will do when it's done forwarding the schema to its own
        // handler.
        let _schema = unsafe { physical_schema.into_inner() };
        let state = unsafe { &*(engine_state as *const ShimEngineState) };
        state.invocations.lock().unwrap().read_calls += 1;

        let iter_state = Box::new(ShimIterState {
            remaining: Mutex::new(state.batch_sizes.clone()),
        });
        unsafe {
            *out = JavaParquetReadResult {
                iter_state: Box::into_raw(iter_state) as *mut c_void,
                error: 0,
            };
        }
    }

    extern "C" fn shim_iter_next(
        engine_state: *mut c_void,
        iter_state: *mut c_void,
        out: *mut JavaEngineDataResult,
    ) {
        let state = unsafe { &*(engine_state as *const ShimEngineState) };
        state.invocations.lock().unwrap().next_calls += 1;

        let iter = unsafe { &*(iter_state as *const ShimIterState) };
        let mut remaining = iter.remaining.lock().unwrap();
        let result = match remaining.is_empty() {
            true => JavaEngineDataResult {
                batch: std::ptr::null_mut(),
                len: 0,
                done: true,
                error: 0,
            },
            false => {
                let len = remaining.remove(0);
                let batch = Box::new(StubBatch { len });
                JavaEngineDataResult {
                    batch: Box::into_raw(batch) as *mut c_void,
                    len,
                    done: false,
                    error: 0,
                }
            }
        };
        unsafe {
            *out = result;
        }
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
            // Bump the counter before dropping so the test can observe via
            // the cloned Arc<Mutex<_>>.
            {
                let state = unsafe { &*(engine_state as *const ShimEngineState) };
                state.invocations.lock().unwrap().engine_frees += 1;
            }
            unsafe { drop(Box::from_raw(engine_state as *mut ShimEngineState)) };
        }
    }

    fn make_inner_engine() -> Arc<dyn Engine> {
        // memory:// store keeps the test hermetic; we never actually use the
        // inner engine's storage / json / parquet handlers.
        let url = Url::parse("memory:///doesntmatter/").unwrap();
        let store = store_from_url_opts(&url, HashMap::<String, String>::new()).unwrap();
        Arc::new(DefaultEngineBuilder::new(store).build())
    }

    fn make_callbacks(state: Box<ShimEngineState>) -> JavaParquetCallbacks {
        JavaParquetCallbacks {
            engine_state: Box::into_raw(state) as *mut c_void,
            read_parquet_files: shim_read_parquet_files,
            iter_next: shim_iter_next,
            iter_free: shim_iter_free,
            free_data: shim_free_data,
            free_engine_state: shim_free_engine_state,
        }
    }

    fn synthetic_schema() -> SchemaRef {
        Arc::new(StructType::try_new([StructField::nullable("id", DataType::INTEGER)]).unwrap())
    }

    fn synthetic_files() -> Vec<KernelFileMeta> {
        vec![KernelFileMeta {
            location: Url::parse("memory:///doesntmatter/foo.parquet").unwrap(),
            last_modified: 0,
            size: 0,
        }]
    }

    /// Drive parquet reads through the [`Engine`] trait surface and verify
    /// the kernel pumps the engine-side iterator to exhaustion, producing
    /// exactly the synthetic batches the shim queued.
    #[test]
    fn callback_engine_yields_engine_supplied_batches() {
        let counters: Arc<Mutex<ShimCounters>> = Arc::new(Mutex::new(ShimCounters::default()));
        let shim_state = Box::new(ShimEngineState {
            batch_sizes: vec![5, 7, 3],
            invocations: counters.clone(),
        });
        let callbacks = make_callbacks(shim_state);
        let inner = make_inner_engine();
        let engine = CallbackEngine::new(inner, callbacks);

        let files = synthetic_files();
        let mut iter = engine
            .parquet_handler()
            .read_parquet_files(&files, synthetic_schema(), None)
            .expect("read_parquet_files should succeed");

        let mut total_rows = 0usize;
        let mut batch_count = 0usize;
        while let Some(batch) = iter.next() {
            let batch = batch.expect("batch should be Ok");
            total_rows += batch.len();
            batch_count += 1;
        }
        drop(iter); // triggers iter_free
        drop(engine); // triggers handler drop -> free_engine_state

        let counters = counters.lock().unwrap();
        assert_eq!(
            counters.read_calls, 1,
            "read_parquet_files should be called once"
        );
        // 3 successful nexts + 1 final next that signals done = 4
        assert_eq!(
            counters.next_calls, 4,
            "iter_next should be pumped to exhaustion"
        );
        assert_eq!(
            counters.iter_frees, 1,
            "iter_free should run on iterator drop"
        );
        assert_eq!(
            counters.data_frees, 3,
            "every batch should be freed on drop"
        );
        assert_eq!(
            counters.engine_frees, 1,
            "engine state should be freed exactly once"
        );

        assert_eq!(batch_count, 3);
        assert_eq!(total_rows, 5 + 7 + 3);
    }

    /// The kernel must respect an engine-signalled error and propagate it
    /// without further pumping the iterator.
    #[test]
    fn callback_engine_propagates_iter_error() {
        extern "C" fn read_returns_error(
            _engine_state: *mut c_void,
            _files: *const FileMeta,
            _files_len: usize,
            physical_schema: Handle<SharedSchema>,
            out: *mut JavaParquetReadResult,
        ) {
            let _schema = unsafe { physical_schema.into_inner() };
            unsafe {
                *out = JavaParquetReadResult {
                    iter_state: std::ptr::null_mut(),
                    error: 42,
                };
            }
        }
        extern "C" fn unreachable_next(
            _: *mut c_void,
            _: *mut c_void,
            _: *mut JavaEngineDataResult,
        ) {
            unreachable!("iter_next must not be called when read_parquet_files errored")
        }
        extern "C" fn noop_free_iter(_: *mut c_void, _: *mut c_void) {}
        extern "C" fn noop_free_data(_: *mut c_void, _: *mut c_void) {}
        extern "C" fn noop_free_engine_state(_: *mut c_void) {}

        let callbacks = JavaParquetCallbacks {
            engine_state: std::ptr::null_mut(),
            read_parquet_files: read_returns_error,
            iter_next: unreachable_next,
            iter_free: noop_free_iter,
            free_data: noop_free_data,
            free_engine_state: noop_free_engine_state,
        };
        let engine = CallbackEngine::new(make_inner_engine(), callbacks);
        let result = engine.parquet_handler().read_parquet_files(
            &synthetic_files(),
            synthetic_schema(),
            None,
        );
        let err = match result {
            Err(e) => e,
            Ok(_) => panic!("expected error to surface"),
        };
        assert!(
            format!("{err}").contains("error code 42"),
            "error must surface engine code; got: {err}"
        );
    }

    /// Regression test for the engine-state-lifetime invariant: dropping a
    /// batch must remain safe even after the iterator AND the engine that
    /// produced them have been dropped. Without an `Arc<HandlerState>` in
    /// each batch this test would UAF on `free_data` / `free_engine_state`.
    #[test]
    fn batch_can_outlive_iterator_and_engine() {
        let counters: Arc<Mutex<ShimCounters>> = Arc::new(Mutex::new(ShimCounters::default()));
        let shim_state = Box::new(ShimEngineState {
            batch_sizes: vec![5, 7],
            invocations: counters.clone(),
        });
        let callbacks = make_callbacks(shim_state);
        let inner = make_inner_engine();
        let engine = CallbackEngine::new(inner, callbacks);

        // Drain into a Vec so batches outlive the iterator and engine.
        let files = synthetic_files();
        let mut iter = engine
            .parquet_handler()
            .read_parquet_files(&files, synthetic_schema(), None)
            .expect("read_parquet_files should succeed");
        let batches: Vec<Box<dyn EngineData>> =
            std::iter::from_fn(|| iter.next().map(|r| r.expect("ok"))).collect();

        // Drop in pathological order: iterator first, then engine, then
        // batches. Without the Arc lifetime fix, the batch drops would
        // dereference freed engine state.
        drop(iter);
        // After iter drop: iter_free called once.
        assert_eq!(counters.lock().unwrap().iter_frees, 1);
        // engine_frees still 0 -- batches still hold Arc<HandlerState>.
        assert_eq!(counters.lock().unwrap().engine_frees, 0);

        drop(engine);
        // engine drop alone does NOT free engine state -- batches still hold it.
        assert_eq!(counters.lock().unwrap().engine_frees, 0);

        // Now drop the batches. Each free_data fires; only the LAST drop
        // triggers free_engine_state.
        let batch_count_before = counters.lock().unwrap().data_frees;
        drop(batches);
        let counters = counters.lock().unwrap();
        assert_eq!(
            counters.data_frees - batch_count_before,
            2,
            "both batches should free on drop",
        );
        assert_eq!(
            counters.engine_frees, 1,
            "engine state must free exactly once, after the last batch drops",
        );
    }
}
