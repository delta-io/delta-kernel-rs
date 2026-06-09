//! Custom [`ParquetHandler`] driven by an engine-supplied vtable.
//!
//! Unlike the JSON handler, parquet batches cross the boundary as Arrow data
//! via the [Arrow C Data Interface]: `iter_next` hands the kernel an exported
//! struct array + schema ([`ArrowFFIData`]), which the kernel imports into an
//! `ArrowEngineData` directly -- no column-materialization round trip. Writes
//! and footer reads are likewise fully custom callbacks (no inner engine
//! delegation).
//!
//! [Arrow C Data Interface]: https://arrow.apache.org/docs/format/CDataInterface.html

use std::ffi::c_void;
use std::mem::MaybeUninit;
use std::sync::Arc;

use delta_kernel::arrow::array::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use delta_kernel::arrow::array::{RecordBatch, StructArray};
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::schema::SchemaRef;
use delta_kernel::{
    DeltaResult, DeltaResultIterator, EngineData, Error, FileDataReadResultIterator,
    FileMeta as KernelFileMeta, ParquetFooter, ParquetHandler, PredicateRef,
};

use crate::custom_io_engine::{to_ffi_file_metas, CustomReadResult};
use crate::engine_data::ArrowFFIData;
use crate::engine_funcs::FileMeta;
use crate::expressions::SharedPredicate;
use crate::handle::Handle;
use crate::{KernelStringSlice, SharedSchema};

/// Result of [`CustomParquetCallbacks::iter_next`]: one batch as exported
/// Arrow C Data Interface structs.
#[repr(C)]
pub struct CustomArrowDataResult {
    /// Exported Arrow array (a struct array whose fields are the batch's
    /// columns). Valid iff `done == false && error == 0`.
    pub array: FFI_ArrowArray,
    /// Schema describing `array`.
    pub schema: FFI_ArrowSchema,
    /// Set to `true` when no more batches remain.
    pub done: bool,
    /// `0` on success.
    pub error: u32,
}

impl CustomArrowDataResult {
    fn empty() -> Self {
        Self {
            array: FFI_ArrowArray::empty(),
            schema: FFI_ArrowSchema::empty(),
            done: false,
            error: 0,
        }
    }
}

/// Function-pointer table the connector supplies to drive the parquet handler.
#[repr(C)]
pub struct CustomParquetCallbacks {
    /// Engine-managed opaque pointer threaded through every callback.
    pub engine_state: *mut c_void,
    /// Begin reading the given parquet files into the `physical_schema`. The
    /// engine **consumes** the schema handle exactly once. When `predicate` is
    /// non-null it points to a [`SharedPredicate`] handle the engine also
    /// consumes exactly once; null means no predicate hint. On success the
    /// engine writes a non-null iterator state.
    pub read_parquet_files: extern "C" fn(
        engine_state: *mut c_void,
        files: *const FileMeta,
        files_len: usize,
        physical_schema: Handle<SharedSchema>,
        predicate: *mut crate::expressions::SharedPredicate,
        out: *mut CustomReadResult,
    ),
    /// Pump the next batch from an iterator, exporting it via the Arrow C Data
    /// Interface. The kernel imports (and thereby releases) the exported
    /// array/schema; the engine must not touch them after returning.
    pub iter_next: extern "C" fn(
        engine_state: *mut c_void,
        iter_state: *mut c_void,
        out: *mut CustomArrowDataResult,
    ),
    /// Release an iterator state.
    pub iter_free: extern "C" fn(engine_state: *mut c_void, iter_state: *mut c_void),
    /// Write the supplied Arrow batches to a parquet file at `path`,
    /// overwriting any existing file. The `batches` array is valid only for
    /// the duration of this call; the engine consumes each batch in place
    /// (per the Arrow C Data Interface, releasing each array it imports).
    pub write_parquet_file: extern "C" fn(
        engine_state: *mut c_void,
        path: KernelStringSlice,
        batches: *mut ArrowFFIData,
        batches_len: usize,
        out_error: *mut u32,
    ),
    /// Read a parquet file footer, writing the file's schema into `out_schema`
    /// on success. Returns `0` on success or a non-zero error code. `out_schema`
    /// receives a [`SharedSchema`] handle the kernel takes ownership of.
    pub read_parquet_footer: extern "C" fn(
        engine_state: *mut c_void,
        file: *const FileMeta,
        out_schema: *mut Handle<SharedSchema>,
    ) -> u32,
    /// Free the engine state when the last handler reference drops.
    pub free_engine_state: extern "C" fn(engine_state: *mut c_void),
}

// SAFETY: plain function pointers plus an opaque engine pointer the kernel
// never dereferences.
unsafe impl Send for CustomParquetCallbacks {}
unsafe impl Sync for CustomParquetCallbacks {}

/// Shared state for the parquet handler. The engine state is freed exactly
/// once, when the last `Arc` reference (handler + any live iterator) drops.
struct ParquetHandlerState {
    callbacks: CustomParquetCallbacks,
}

impl Drop for ParquetHandlerState {
    fn drop(&mut self) {
        (self.callbacks.free_engine_state)(self.callbacks.engine_state);
    }
}

/// [`ParquetHandler`] that forwards every operation to a connector-supplied
/// vtable, importing read batches from the Arrow C Data Interface.
pub struct CustomParquetHandler {
    state: Arc<ParquetHandlerState>,
}

impl CustomParquetHandler {
    pub(crate) fn new(callbacks: CustomParquetCallbacks) -> Self {
        Self {
            state: Arc::new(ParquetHandlerState { callbacks }),
        }
    }
}

impl ParquetHandler for CustomParquetHandler {
    fn read_parquet_files(
        &self,
        files: &[KernelFileMeta],
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> DeltaResult<FileDataReadResultIterator> {
        let ffi_files = to_ffi_file_metas(files)?;
        let schema_handle: Handle<SharedSchema> = physical_schema.into();
        let mut predicate_handle = predicate.map(Handle::<SharedPredicate>::from);
        // `Handle<SharedPredicate>` is `repr(transparent)` over `NonNull<SharedPredicate>`.
        let predicate_ptr = predicate_handle
            .as_mut()
            .map_or(std::ptr::null_mut(), |h| std::ptr::from_mut(h).cast());
        let mut result = CustomReadResult {
            iter_state: std::ptr::null_mut(),
            error: 0,
        };
        (self.state.callbacks.read_parquet_files)(
            self.state.callbacks.engine_state,
            ffi_files.as_ptr(),
            ffi_files.len(),
            schema_handle,
            predicate_ptr,
            &mut result,
        );
        if result.error != 0 {
            return Err(Error::generic(format!(
                "CustomParquetHandler::read_parquet_files signalled error code {}",
                result.error
            )));
        }
        if result.iter_state.is_null() {
            return Err(Error::generic(
                "CustomParquetHandler::read_parquet_files returned a null iterator state on success",
            ));
        }
        Ok(Box::new(CustomParquetIter {
            iter_state: result.iter_state,
            handler: self.state.clone(),
            done: false,
        }))
    }

    fn write_parquet_file(
        &self,
        location: url::Url,
        data: DeltaResultIterator<Box<dyn EngineData>>,
    ) -> DeltaResult<()> {
        let mut batches: Vec<ArrowFFIData> = Vec::new();
        for engine_data in data {
            batches.push(ArrowFFIData::try_from_engine_data(engine_data?)?);
        }
        // SAFETY: `location.as_str()` outlives this synchronous upcall.
        let slice = unsafe { KernelStringSlice::new_unsafe(location.as_str()) };
        let mut error: u32 = 0;
        (self.state.callbacks.write_parquet_file)(
            self.state.callbacks.engine_state,
            slice,
            batches.as_mut_ptr(),
            batches.len(),
            &mut error,
        );
        // `batches` drops here; each `ArrowFFIData`'s release runs unless the
        // engine already consumed it (matching the `free_arrow_ffi_data`
        // contract) -- no double free, no leak.
        if error != 0 {
            return Err(Error::generic(format!(
                "CustomParquetHandler::write_parquet_file signalled error code {error} for {location}"
            )));
        }
        Ok(())
    }

    fn read_parquet_footer(&self, file: &KernelFileMeta) -> DeltaResult<ParquetFooter> {
        let ffi_files = to_ffi_file_metas(std::slice::from_ref(file))?;
        let mut schema_slot = MaybeUninit::<Handle<SharedSchema>>::uninit();
        let error = (self.state.callbacks.read_parquet_footer)(
            self.state.callbacks.engine_state,
            ffi_files.as_ptr(),
            schema_slot.as_mut_ptr(),
        );
        if error != 0 {
            return Err(Error::generic(format!(
                "CustomParquetHandler::read_parquet_footer signalled error code {error} for {}",
                file.location
            )));
        }
        // SAFETY: the engine wrote a valid `SharedSchema` handle on success;
        // we take ownership of it here.
        let schema_handle = unsafe { schema_slot.assume_init() };
        let schema: SchemaRef = unsafe { schema_handle.into_inner() };
        Ok(ParquetFooter { schema })
    }
}

/// Iterator over parquet batches, importing each exported Arrow array into an
/// `ArrowEngineData`.
struct CustomParquetIter {
    iter_state: *mut c_void,
    handler: Arc<ParquetHandlerState>,
    done: bool,
}

// SAFETY: same justification as `CustomParquetCallbacks`.
unsafe impl Send for CustomParquetIter {}

impl Drop for CustomParquetIter {
    fn drop(&mut self) {
        if !self.iter_state.is_null() {
            (self.handler.callbacks.iter_free)(
                self.handler.callbacks.engine_state,
                self.iter_state,
            );
        }
    }
}

impl Iterator for CustomParquetIter {
    type Item = DeltaResult<Box<dyn EngineData>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let mut res = CustomArrowDataResult::empty();
        (self.handler.callbacks.iter_next)(
            self.handler.callbacks.engine_state,
            self.iter_state,
            &mut res,
        );
        if res.error != 0 {
            self.done = true;
            return Some(Err(Error::generic(format!(
                "CustomParquetIter::next signalled error code {}",
                res.error
            ))));
        }
        if res.done {
            self.done = true;
            return None;
        }
        // Move the exported structs out and import them into Arrow. Leave
        // empties behind so `res` dropping is a no-op.
        let array = std::mem::replace(&mut res.array, FFI_ArrowArray::empty());
        let schema = std::mem::replace(&mut res.schema, FFI_ArrowSchema::empty());
        Some(import_arrow_batch(array, schema))
    }
}

/// Import an exported Arrow struct array into an `ArrowEngineData`.
fn import_arrow_batch(
    array: FFI_ArrowArray,
    schema: FFI_ArrowSchema,
) -> DeltaResult<Box<dyn EngineData>> {
    // SAFETY: the engine exported `array`/`schema` via the Arrow C Data
    // Interface; `from_ffi` takes ownership and releases them.
    let array_data = unsafe { from_ffi(array, &schema) }?;
    let record_batch: RecordBatch = StructArray::from(array_data).into();
    let arrow_engine_data: ArrowEngineData = record_batch.into();
    Ok(Box::new(arrow_engine_data))
}

#[cfg(test)]
mod tests {
    //! Pure-Rust shim that simulates the engine side of the parquet vtable:
    //! `iter_next` synthesizes Arrow batches and exports them via the C Data
    //! Interface, exactly as a real connector would. This exercises the
    //! brand-new Arrow import path end-to-end without a real parquet file.
    use std::ffi::c_void;
    use std::sync::{Arc, Mutex};

    use delta_kernel::arrow::array::ffi::to_ffi;
    use delta_kernel::arrow::array::{Array, ArrayRef, Int32Array, StructArray};
    use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};
    use delta_kernel::engine::arrow_data::ArrowEngineData;
    use delta_kernel::schema::{DataType, StructField, StructType};

    use super::*;

    /// Engine-side state: the row counts the shim should emit, one batch each.
    struct ParquetShim {
        remaining: Mutex<Vec<usize>>,
        engine_frees: Arc<Mutex<usize>>,
    }

    extern "C" fn shim_read(
        _engine_state: *mut c_void,
        _files: *const FileMeta,
        _files_len: usize,
        physical_schema: Handle<SharedSchema>,
        predicate: *mut SharedPredicate,
        out: *mut CustomReadResult,
    ) {
        // Consume the schema handle, mirroring a real engine.
        let _schema = unsafe { physical_schema.into_inner() };
        if !predicate.is_null() {
            unsafe { std::ptr::read(predicate as *mut Handle<SharedPredicate>).drop_handle() };
        }
        // The iterator state owns its own queue, cloned from the engine state.
        let state = unsafe { &*(_engine_state as *const ParquetShim) };
        let remaining: Vec<usize> = state.remaining.lock().unwrap().clone();
        let iter = Box::new(Mutex::new(remaining));
        unsafe {
            *out = CustomReadResult {
                iter_state: Box::into_raw(iter) as *mut c_void,
                error: 0,
            };
        }
    }

    extern "C" fn shim_iter_next(
        _engine_state: *mut c_void,
        iter_state: *mut c_void,
        out: *mut CustomArrowDataResult,
    ) {
        let queue = unsafe { &*(iter_state as *const Mutex<Vec<usize>>) };
        let mut remaining = queue.lock().unwrap();
        if remaining.is_empty() {
            unsafe {
                (*out).done = true;
            }
            return;
        }
        let n = remaining.remove(0);
        let col = Arc::new(Int32Array::from((0..n as i32).collect::<Vec<_>>())) as ArrayRef;
        let field = Arc::new(ArrowField::new("id", ArrowDataType::Int32, true));
        let sa = StructArray::from(vec![(field, col)]);
        let (array, schema) = to_ffi(&sa.into_data()).expect("to_ffi");
        unsafe {
            *out = CustomArrowDataResult {
                array,
                schema,
                done: false,
                error: 0,
            };
        }
    }

    extern "C" fn shim_iter_free(_engine_state: *mut c_void, iter_state: *mut c_void) {
        if !iter_state.is_null() {
            unsafe { drop(Box::from_raw(iter_state as *mut Mutex<Vec<usize>>)) };
        }
    }

    extern "C" fn shim_write(
        _engine_state: *mut c_void,
        _path: KernelStringSlice,
        _batches: *mut ArrowFFIData,
        _batches_len: usize,
        out_error: *mut u32,
    ) {
        unsafe { *out_error = 0 };
    }

    extern "C" fn shim_footer(
        _engine_state: *mut c_void,
        _file: *const FileMeta,
        _out_schema: *mut Handle<SharedSchema>,
    ) -> u32 {
        // Unused by these tests; signal an error so the (absent) out_schema is
        // never read.
        1
    }

    extern "C" fn shim_free_engine(engine_state: *mut c_void) {
        if !engine_state.is_null() {
            let shim = unsafe { Box::from_raw(engine_state as *mut ParquetShim) };
            *shim.engine_frees.lock().unwrap() += 1;
        }
    }

    fn make_callbacks(
        batches: Vec<usize>,
        engine_frees: Arc<Mutex<usize>>,
    ) -> CustomParquetCallbacks {
        let shim = Box::new(ParquetShim {
            remaining: Mutex::new(batches),
            engine_frees,
        });
        CustomParquetCallbacks {
            engine_state: Box::into_raw(shim) as *mut c_void,
            read_parquet_files: shim_read,
            iter_next: shim_iter_next,
            iter_free: shim_iter_free,
            write_parquet_file: shim_write,
            read_parquet_footer: shim_footer,
            free_engine_state: shim_free_engine,
        }
    }

    fn synthetic_schema() -> SchemaRef {
        Arc::new(StructType::try_new([StructField::nullable("id", DataType::INTEGER)]).unwrap())
    }

    fn synthetic_files() -> Vec<KernelFileMeta> {
        vec![KernelFileMeta {
            location: url::Url::parse("memory:///t/data.parquet").unwrap(),
            last_modified: 0,
            size: 0,
        }]
    }

    #[test]
    fn parquet_handler_imports_engine_arrow_batches() {
        let engine_frees = Arc::new(Mutex::new(0));
        let handler = CustomParquetHandler::new(make_callbacks(vec![3, 2], engine_frees.clone()));
        let iter = handler
            .read_parquet_files(&synthetic_files(), synthetic_schema(), None)
            .expect("read_parquet_files should succeed");

        let mut batch_count = 0usize;
        let mut total_rows = 0usize;
        for batch in iter {
            let batch = batch.expect("batch should import cleanly");
            // Confirm the imported batch is real Arrow data, not opaque.
            assert!(batch.any_ref().downcast_ref::<ArrowEngineData>().is_some());
            total_rows += batch.len();
            batch_count += 1;
        }
        assert_eq!(batch_count, 2, "two batches should be emitted");
        assert_eq!(total_rows, 5, "row counts (3 + 2) should sum");

        drop(handler);
        assert_eq!(
            *engine_frees.lock().unwrap(),
            1,
            "engine state must be freed exactly once"
        );
    }

    #[test]
    fn parquet_handler_propagates_read_error() {
        extern "C" fn read_errors(
            _engine_state: *mut c_void,
            _files: *const FileMeta,
            _files_len: usize,
            physical_schema: Handle<SharedSchema>,
            predicate: *mut SharedPredicate,
            out: *mut CustomReadResult,
        ) {
            let _schema = unsafe { physical_schema.into_inner() };
            if !predicate.is_null() {
                unsafe { std::ptr::read(predicate as *mut Handle<SharedPredicate>).drop_handle() };
            }
            unsafe {
                *out = CustomReadResult {
                    iter_state: std::ptr::null_mut(),
                    error: 7,
                };
            }
        }
        let engine_frees = Arc::new(Mutex::new(0));
        let mut callbacks = make_callbacks(vec![], engine_frees);
        callbacks.read_parquet_files = read_errors;
        let handler = CustomParquetHandler::new(callbacks);
        let err = match handler.read_parquet_files(&synthetic_files(), synthetic_schema(), None) {
            Ok(_) => panic!("error code should surface"),
            Err(e) => e,
        };
        assert!(format!("{err}").contains("error code 7"), "got: {err}");
    }
}
