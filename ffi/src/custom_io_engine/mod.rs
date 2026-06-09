//! All-or-nothing custom IO [`Engine`] FFI surface.
//!
//! [`CustomIOEngine`] wires a connector's storage, JSON, and parquet handlers
//! through `extern "C"` vtables while always supplying the kernel's default
//! Arrow [`EvaluationHandler`]. Unlike a partial-compose engine, all three IO
//! handlers are required: a connector that adopts this surface owns every
//! byte of IO, and the kernel owns expression evaluation.
//!
//! - Storage upcalls produce file/byte iterators (see [`storage`]).
//! - JSON reads bridge engine-owned batches to Arrow via the column-materialization protocol (see
//!   [`json`] and the `arrow_bridge` module).
//! - Parquet reads cross the boundary as Arrow C Data Interface batches; writes and footer reads
//!   are fully custom (see [`parquet`]).

use std::sync::Arc;

use delta_kernel::engine::arrow_expression::ArrowEvaluationHandler;
use delta_kernel::{
    DeltaResult, Engine, Error, EvaluationHandler, FileMeta as KernelFileMeta, JsonHandler,
    ParquetHandler, StorageHandler,
};

use crate::engine_funcs::FileMeta;
use crate::handle::Handle;
use crate::{
    engine_to_handle, AllocateErrorFn, ExternResult, IntoExternResult, KernelStringSlice,
    SharedExternEngine,
};

pub mod json;
pub mod parquet;
pub mod storage;

pub use json::{
    ColumnBuffers, ColumnTypeTag, CustomEngineData, CustomEngineDataCallbacks,
    CustomEngineDataResult, CustomJsonCallbacks, CustomJsonHandler, VisitRowsRequest,
    VisitRowsResult,
};
pub use parquet::{CustomArrowDataResult, CustomParquetCallbacks, CustomParquetHandler};
pub use storage::{
    CustomBytesIterResult, CustomBytesResult, CustomFileIterResult, CustomFileMetaResult,
    CustomStorageCallbacks, CustomStorageHandler, FileSliceFFI,
};

/// Result of a `read_*_files` callback: an engine-managed iterator state.
///
/// Shared by the JSON and parquet read paths. `iter_state` must be non-null on
/// success and is ignored when `error != 0`.
#[repr(C)]
pub struct CustomReadResult {
    /// Engine-managed iterator state, passed back verbatim to `iter_next` /
    /// `iter_free`.
    pub iter_state: *mut std::ffi::c_void,
    /// `0` on success; non-zero signals an engine error.
    pub error: u32,
}

/// Marshal a slice of kernel [`FileMeta`] into the FFI [`FileMeta`] layout.
///
/// The returned entries borrow path bytes from `files`; the result is only
/// valid while `files` is alive, which always covers the synchronous upcall
/// that consumes it.
///
/// [`FileMeta`]: crate::engine_funcs::FileMeta
pub(crate) fn to_ffi_file_metas(files: &[KernelFileMeta]) -> DeltaResult<Vec<FileMeta>> {
    files
        .iter()
        .map(|fm| {
            let size: usize = fm.size.try_into().map_err(|_| {
                Error::generic("FileMeta::size does not fit in usize on this platform")
            })?;
            Ok(FileMeta {
                // SAFETY: `fm.location` lives for the lifetime of `files`, which
                // covers the synchronous upcall that reads this slice.
                path: unsafe { KernelStringSlice::new_unsafe(fm.location.as_str()) },
                last_modified: fm.last_modified,
                size,
            })
        })
        .collect()
}

/// Bundle of the three required IO handler vtables. Each pointer must be
/// non-null and point to a fully-initialized vtable; the structs are read by
/// value into the engine, so the pointers need not outlive
/// [`make_custom_io_engine`].
#[repr(C)]
pub struct CustomIOCallbacks {
    /// Pointer to a [`CustomStorageCallbacks`] (required).
    pub storage: *const CustomStorageCallbacks,
    /// Pointer to a [`CustomJsonCallbacks`] (required).
    pub json: *const CustomJsonCallbacks,
    /// Pointer to a [`CustomParquetCallbacks`] (required).
    pub parquet: *const CustomParquetCallbacks,
}

// SAFETY: pointers into engine-owned vtables that the kernel only reads once.
unsafe impl Send for CustomIOCallbacks {}
unsafe impl Sync for CustomIOCallbacks {}

/// An [`Engine`] whose storage, JSON, and parquet handlers are connector
/// vtables, and whose evaluation handler is always the kernel's default Arrow
/// implementation.
pub struct CustomIOEngine {
    storage: Arc<CustomStorageHandler>,
    json: Arc<CustomJsonHandler>,
    parquet: Arc<CustomParquetHandler>,
    evaluation: Arc<ArrowEvaluationHandler>,
}

impl CustomIOEngine {
    /// Build a [`CustomIOEngine`] from the three handler vtables. The Arrow
    /// evaluation handler is supplied internally.
    pub fn new(
        storage: CustomStorageCallbacks,
        json: CustomJsonCallbacks,
        parquet: CustomParquetCallbacks,
    ) -> Self {
        Self {
            storage: Arc::new(CustomStorageHandler::new(storage)),
            json: Arc::new(CustomJsonHandler::new(json)),
            parquet: Arc::new(CustomParquetHandler::new(parquet)),
            evaluation: Arc::new(ArrowEvaluationHandler),
        }
    }
}

impl Engine for CustomIOEngine {
    fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler> {
        self.evaluation.clone()
    }
    fn storage_handler(&self) -> Arc<dyn StorageHandler> {
        self.storage.clone()
    }
    fn json_handler(&self) -> Arc<dyn JsonHandler> {
        self.json.clone()
    }
    fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
        self.parquet.clone()
    }
}

/// Build a [`CustomIOEngine`] from a bundle of three required IO vtables.
/// Returns a fresh shared engine handle the caller must free via
/// [`free_engine`].
///
/// [`free_engine`]: crate::free_engine
///
/// # Safety
/// - `callbacks.storage`, `callbacks.json`, and `callbacks.parquet` must each be non-null and point
///   to a valid, fully-initialized vtable readable by Rust for the duration of this call. Each
///   vtable is copied by value into the returned engine.
/// - Every function pointer in each vtable must remain valid for the lifetime of the returned
///   engine, and each vtable's `engine_state` ownership transfers into the returned engine.
#[no_mangle]
pub unsafe extern "C" fn make_custom_io_engine(
    callbacks: CustomIOCallbacks,
    allocate_error: AllocateErrorFn,
) -> ExternResult<Handle<SharedExternEngine>> {
    let result = unsafe { make_custom_io_engine_impl(callbacks, allocate_error) };
    unsafe { result.into_extern_result(&allocate_error) }
}

unsafe fn make_custom_io_engine_impl(
    callbacks: CustomIOCallbacks,
    allocate_error: AllocateErrorFn,
) -> DeltaResult<Handle<SharedExternEngine>> {
    if callbacks.storage.is_null() || callbacks.json.is_null() || callbacks.parquet.is_null() {
        return Err(Error::generic(
            "make_custom_io_engine: storage, json, and parquet callbacks are all required \
             (non-null)",
        ));
    }
    // SAFETY: each pointer was checked non-null and (per the function contract)
    // points to a fully-initialized vtable. Reading by value transfers each
    // vtable's `engine_state` ownership into the engine.
    let storage = unsafe { std::ptr::read(callbacks.storage) };
    let json = unsafe { std::ptr::read(callbacks.json) };
    let parquet = unsafe { std::ptr::read(callbacks.parquet) };
    let engine: Arc<dyn Engine> = Arc::new(CustomIOEngine::new(storage, json, parquet));
    Ok(engine_to_handle(engine, allocate_error))
}

/// Test driver for C/C++ integrators: builds a [`CustomIOEngine`] from the
/// supplied callbacks and immediately frees it. Returns `0` on success.
///
/// # Safety
/// Same requirements as [`make_custom_io_engine`], including a valid `allocate_error`.
#[no_mangle]
pub unsafe extern "C" fn test_drive_custom_io_engine(
    callbacks: CustomIOCallbacks,
    allocate_error: AllocateErrorFn,
) -> u32 {
    match unsafe { make_custom_io_engine_impl(callbacks, allocate_error) } {
        Ok(handle) => {
            unsafe { crate::free_engine(handle) };
            0
        }
        Err(_) => 1,
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::c_void;

    use delta_kernel::{JsonHandler, ParquetHandler, StorageHandler};

    use super::*;
    use crate::ffi_test_utils::allocate_err;
    use crate::ExternResult;

    // === Noop vtables: valid function pointers that are never invoked (the
    // factory only reads the structs and, on success, frees engine state). ===

    extern "C" fn noop_free(_: *mut c_void) {}

    fn noop_storage() -> CustomStorageCallbacks {
        extern "C" fn list_from(
            _: *mut c_void,
            _: KernelStringSlice,
            _: *mut CustomFileIterResult,
        ) {
        }
        extern "C" fn read_files(
            _: *mut c_void,
            _: *const FileSliceFFI,
            _: usize,
            _: *mut CustomBytesIterResult,
        ) {
        }
        extern "C" fn copy_atomic(
            _: *mut c_void,
            _: KernelStringSlice,
            _: KernelStringSlice,
            _: *mut u32,
        ) {
        }
        extern "C" fn put(
            _: *mut c_void,
            _: KernelStringSlice,
            _: *const u8,
            _: usize,
            _: bool,
            _: *mut u32,
        ) {
        }
        extern "C" fn head(_: *mut c_void, _: KernelStringSlice, _: *mut CustomFileMetaResult) {}
        extern "C" fn list_next(_: *mut c_void, _: *mut c_void, _: *mut CustomFileMetaResult) {}
        extern "C" fn bytes_next(_: *mut c_void, _: *mut c_void, _: *mut CustomBytesResult) {}
        extern "C" fn iter_free(_: *mut c_void, _: *mut c_void) {}
        CustomStorageCallbacks {
            engine_state: std::ptr::null_mut(),
            list_from,
            read_files,
            copy_atomic,
            put,
            head,
            list_iter_next: list_next,
            list_iter_free: iter_free,
            bytes_iter_next: bytes_next,
            bytes_iter_free: iter_free,
            free_engine_state: noop_free,
        }
    }

    fn noop_json() -> CustomJsonCallbacks {
        extern "C" fn read_json(
            _: *mut c_void,
            _: *const FileMeta,
            _: usize,
            schema: Handle<crate::SharedSchema>,
            predicate: *mut crate::expressions::SharedPredicate,
            _: *mut CustomReadResult,
        ) {
            // Consume the handles so they are not leaked if ever called.
            let _ = unsafe { schema.into_inner() };
            if !predicate.is_null() {
                unsafe {
                    std::ptr::read(predicate as *mut Handle<crate::expressions::SharedPredicate>)
                        .drop_handle()
                };
            }
        }
        extern "C" fn iter_next(_: *mut c_void, _: *mut c_void, _: *mut CustomEngineDataResult) {}
        extern "C" fn iter_free(_: *mut c_void, _: *mut c_void) {}
        extern "C" fn write_json(
            _: *mut c_void,
            _: KernelStringSlice,
            _: *mut crate::engine_data::ArrowFFIData,
            _: usize,
            _: bool,
            _: *mut u32,
        ) {
        }
        extern "C" fn materialize(
            _: *mut c_void,
            _: *const VisitRowsRequest,
            _: *mut VisitRowsResult,
        ) {
        }
        extern "C" fn free_columns(_: *mut c_void, _: *const ColumnBuffers, _: usize) {}
        extern "C" fn free_batch(_: *mut c_void, _: *mut c_void) {}
        CustomJsonCallbacks {
            engine_state: std::ptr::null_mut(),
            read_json_files: read_json,
            iter_next,
            iter_free,
            write_json_file: write_json,
            engine_data_callbacks: CustomEngineDataCallbacks {
                materialize_columns: materialize,
                free_columns,
                free_batch,
            },
            free_engine_state: noop_free,
        }
    }

    fn noop_parquet() -> CustomParquetCallbacks {
        extern "C" fn read_parquet(
            _: *mut c_void,
            _: *const FileMeta,
            _: usize,
            schema: Handle<crate::SharedSchema>,
            predicate: *mut crate::expressions::SharedPredicate,
            _: *mut CustomReadResult,
        ) {
            let _ = unsafe { schema.into_inner() };
            if !predicate.is_null() {
                unsafe {
                    std::ptr::read(predicate as *mut Handle<crate::expressions::SharedPredicate>)
                        .drop_handle()
                };
            }
        }
        extern "C" fn iter_next(_: *mut c_void, _: *mut c_void, _: *mut CustomArrowDataResult) {}
        extern "C" fn iter_free(_: *mut c_void, _: *mut c_void) {}
        extern "C" fn write_parquet(
            _: *mut c_void,
            _: KernelStringSlice,
            _: *mut crate::engine_data::ArrowFFIData,
            _: usize,
            _: *mut u32,
        ) {
        }
        extern "C" fn footer(
            _: *mut c_void,
            _: *const FileMeta,
            _: *mut Handle<crate::SharedSchema>,
        ) -> u32 {
            1
        }
        CustomParquetCallbacks {
            engine_state: std::ptr::null_mut(),
            read_parquet_files: read_parquet,
            iter_next,
            iter_free,
            write_parquet_file: write_parquet,
            read_parquet_footer: footer,
            free_engine_state: noop_free,
        }
    }

    #[test]
    fn make_custom_io_engine_rejects_null_callback() {
        let storage = noop_storage();
        let json = noop_json();
        // Null parquet pointer must be rejected.
        let callbacks = CustomIOCallbacks {
            storage: &storage,
            json: &json,
            parquet: std::ptr::null(),
        };
        let result = unsafe { make_custom_io_engine(callbacks, allocate_err) };
        match result {
            ExternResult::Err(e) => {
                let err = unsafe { crate::ffi_test_utils::recover_error(e) };
                assert!(
                    err.message.contains("all required"),
                    "unexpected message: {}",
                    err.message
                );
            }
            ExternResult::Ok(_) => panic!("null parquet callbacks should be rejected"),
        }
    }

    #[test]
    fn make_custom_io_engine_builds_with_all_callbacks() {
        let storage = noop_storage();
        let json = noop_json();
        let parquet = noop_parquet();
        let callbacks = CustomIOCallbacks {
            storage: &storage,
            json: &json,
            parquet: &parquet,
        };
        let engine = match unsafe { make_custom_io_engine(callbacks, allocate_err) } {
            ExternResult::Ok(handle) => handle,
            ExternResult::Err(_) => panic!("engine construction should succeed"),
        };
        // The factory read each vtable by value; dropping the handle runs each
        // `free_engine_state` exactly once.
        unsafe { crate::free_engine(engine) };
    }

    #[test]
    fn custom_io_engine_wires_all_three_io_handlers() {
        let engine = CustomIOEngine::new(noop_storage(), noop_json(), noop_parquet());
        // Each accessor returns a distinct handler; evaluation is always Arrow.
        let _storage: Arc<dyn StorageHandler> = engine.storage_handler();
        let _json: Arc<dyn JsonHandler> = engine.json_handler();
        let _parquet: Arc<dyn ParquetHandler> = engine.parquet_handler();
        let _eval: Arc<dyn EvaluationHandler> = engine.evaluation_handler();
    }

    #[test]
    fn custom_io_engine_uses_default_arrow_evaluation_handler() {
        let engine = CustomIOEngine::new(noop_storage(), noop_json(), noop_parquet());
        // The evaluation handler is always the kernel's Arrow handler; a simple
        // null-row build over a trivial schema should succeed.
        use delta_kernel::schema::{DataType, StructField, StructType};
        let schema =
            Arc::new(StructType::try_new([StructField::nullable("a", DataType::INTEGER)]).unwrap());
        let row = engine
            .evaluation_handler()
            .null_row(schema)
            .expect("default Arrow eval handler should build a null row");
        assert_eq!(row.len(), 1);
    }
}
