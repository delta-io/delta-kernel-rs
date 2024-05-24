//! Scan and EngineData related ffi code

use std::collections::HashMap;
use std::ffi::c_void;
use std::sync::{Arc, Mutex};

use delta_kernel::scan::state::{visit_scan_files, DvInfo, GlobalScanState};
use delta_kernel::scan::{Scan, ScanBuilder, ScanData};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::{DeltaResult, EngineData as KernelEngineData, Error};
use delta_kernel_ffi_macros::handle_descriptor;
use tracing::debug;
use url::Url;

use crate::{
    unwrap_kernel_expression, AllocateStringFn, EnginePredicate, ExternEngine, ExternResult,
    IntoExternResult, KernelBoolSlice, KernelExpressionVisitorState, KernelStringSlice,
    NullableCvoid, SharedExternEngine, SharedSnapshot, TryFromStringSlice,
};

use super::handle::Handle;

// TODO: Do we want this handle at all? Perhaps we should just _always_ pass raw *mut c_void pointers
// that are the engine data? Even if we want the type, should it be a shared handle instead?
/// an opaque struct that encapsulates data read by an engine. this handle can be passed back into
/// some kernel calls to operate on the data, or can be converted into the raw data as read by the
/// [`delta_kernel::Engine`] by calling [`get_raw_engine_data`]
#[handle_descriptor(target=dyn KernelEngineData, mutable=true, sized=false)]
pub struct EngineData;

/// Allow an engine to "unwrap" an [`EngineData`] into the raw pointer for the case it wants
/// to use its own engine data format
///
/// # Safety
///
/// `data_handle` must be a valid pointer to a kernel allocated `EngineData`. The Engine must
/// ensure the handle outlives the returned pointer.
// TODO(frj): What is the engine actually doing with this method?? If we need access to raw extern
// pointers, we will need to define an `ExternEngineData` trait that exposes such capability, along
// with an ExternEngineDataVtable that implements it. See `ExternEngine` and `ExternEngineVtable`
// for examples of how that works.
#[no_mangle]
pub unsafe extern "C" fn get_raw_engine_data(mut data: Handle<EngineData>) -> *mut c_void {
    let ptr = get_raw_engine_data_impl(&mut data) as *mut dyn KernelEngineData;
    ptr as _
}

unsafe fn get_raw_engine_data_impl(data: &mut Handle<EngineData>) -> &mut dyn KernelEngineData {
    let _data = unsafe { data.as_mut() };
    todo!() // See TODO comment for EngineData
}

/// Struct to allow binding to the arrow [C Data
/// Interface](https://arrow.apache.org/docs/format/CDataInterface.html). This includes the data and
/// the schema.
#[cfg(feature = "default-engine")]
// FIXME: cbindgen doesn't recognize the FFI_ArrowFoo fields in this struct
//#[repr(C)]
pub struct ArrowFFIData {
    pub array: arrow_data::ffi::FFI_ArrowArray,
    pub schema: arrow_schema::ffi::FFI_ArrowSchema,
}

/// Get an [`ArrowFFIData`] to allow binding to the arrow [C Data
/// Interface](https://arrow.apache.org/docs/format/CDataInterface.html). This includes the data and
/// the schema.
///
/// # Safety
/// data_handle must be a valid EngineData as read by the
/// [`delta_kernel::engine::default::DefaultEngine`] obtained from `get_default_engine`.
#[cfg(feature = "default-engine")]
#[no_mangle]
pub unsafe extern "C" fn get_raw_arrow_data(
    data: Handle<EngineData>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<*mut ArrowFFIData> {
    // TODO(frj): This consumes the handle. Is that what we really want?
    let data = unsafe { data.into_inner() };
    get_raw_arrow_data_impl(data).into_extern_result(&engine.as_ref())
}

// TODO: This method leaks the returned pointer memory. How will the engine free it?
#[cfg(feature = "default-engine")]
fn get_raw_arrow_data_impl(data: Box<dyn KernelEngineData>) -> DeltaResult<*mut ArrowFFIData> {
    let record_batch: arrow_array::RecordBatch = data
        .into_any()
        .downcast::<delta_kernel::engine::arrow_data::ArrowEngineData>()
        .map_err(|_| delta_kernel::Error::EngineDataType("ArrowEngineData".to_string()))?
        .into();
    let sa: arrow_array::StructArray = record_batch.into();
    let array_data: arrow_data::ArrayData = sa.into();
    // these call `clone`. is there a way to not copy anything and what exactly are they cloning?
    let array = arrow_data::ffi::FFI_ArrowArray::new(&array_data);
    let schema = arrow_schema::ffi::FFI_ArrowSchema::try_from(array_data.data_type())?;
    let ret_data = Box::new(ArrowFFIData { array, schema });
    Ok(Box::leak(ret_data))
}

// TODO: Why do we even need to expose a scan, when the only thing an engine can do with it is
// handit back to the kernel by calling `kernel_scan_data_init`? There isn't even an FFI method to
// drop it!
#[handle_descriptor(target=Scan, mutable=false, sized=true)]
pub struct SharedScan;

/// Drops a scan.
/// # Safety
/// Caller is responsible for passing a [valid][Handle#Validity] scan handle.
#[no_mangle]
pub unsafe extern "C" fn drop_scan(scan: Handle<SharedScan>) {
    scan.drop_handle();
}

/// Get a [`Scan`] over the table specified by the passed snapshot.
/// # Safety
///
/// Caller is responsible for passing a valid snapshot pointer, and engine pointer
#[no_mangle]
pub unsafe extern "C" fn scan(
    snapshot: Handle<SharedSnapshot>,
    engine: Handle<SharedExternEngine>,
    predicate: Option<&mut EnginePredicate>,
) -> ExternResult<Handle<SharedScan>> {
    let snapshot = unsafe { snapshot.clone_as_arc() };
    scan_impl(snapshot, predicate).into_extern_result(&engine.as_ref())
}

fn scan_impl(
    snapshot: Arc<Snapshot>,
    predicate: Option<&mut EnginePredicate>,
) -> DeltaResult<Handle<SharedScan>> {
    let mut scan_builder = ScanBuilder::new(snapshot);
    if let Some(predicate) = predicate {
        let mut visitor_state = KernelExpressionVisitorState::new();
        let exprid = (predicate.visitor)(predicate.predicate, &mut visitor_state);
        if let Some(predicate) = unwrap_kernel_expression(&mut visitor_state, exprid) {
            debug!("Got predicate: {}", predicate);
            scan_builder = scan_builder.with_predicate(predicate);
        }
    }
    Ok(Arc::new(scan_builder.build()?).into())
}

#[handle_descriptor(target=GlobalScanState, mutable=false, sized=true)]
pub struct SharedGlobalScanState;

/// Get the global state for a scan. See the docs for [`delta_kernel::scan::state::GlobalScanState`]
/// for more information.
///
/// # Safety
/// Engine is responsible for providing a valid scan pointer
#[no_mangle]
pub unsafe extern "C" fn get_global_scan_state(
    scan: Handle<SharedScan>,
) -> Handle<SharedGlobalScanState> {
    let scan = unsafe { scan.as_ref() };
    Arc::new(scan.global_scan_state()).into()
}

/// # Safety
///
/// Caller is responsible for passing a valid global scan pointer.
#[no_mangle]
pub unsafe extern "C" fn drop_global_scan_state(state: Handle<SharedGlobalScanState>) {
    state.drop_handle();
}

// Intentionally opaque to the engine.
//
// TODO: This approach liberates the engine from having to worry about mutual exclusion, but that
// means kernel made the decision of how to achieve thread safety. This may not be desirable if the
// engine is single-threaded, or has its own mutual exclusion mechanisms. Deadlock is even a
// conceivable risk, if this interacts poorly with engine's mutual exclusion mechanism.
pub struct KernelScanDataIterator {
    // Mutex -> Allow the iterator to be accessed safely by multiple threads.
    // Box -> Wrap its unsized content this struct is fixed-size with thin pointers.
    // Item = Box<dyn KernelEngineData>, see above, Vec<bool> -> can become a KernelBoolSlice
    data: Mutex<Box<dyn Iterator<Item = DeltaResult<ScanData>> + Send>>,

    // Also keep a reference to the external engine for its error allocator. The default Parquet and
    // Json handlers don't hold any reference to the tokio reactor they rely on, so the iterator
    // terminates early if the last engine goes out of scope.
    engine: Arc<dyn ExternEngine>,
}

#[handle_descriptor(target=KernelScanDataIterator, mutable=false, sized=true)]
pub struct SharedScanDataIterator;

impl Drop for KernelScanDataIterator {
    fn drop(&mut self) {
        debug!("dropping KernelScanDataIterator");
    }
}

/// Get an iterator over the data needed to perform a scan. This will return a
/// [`KernelScanDataIterator`] which can be passed to [`kernel_scan_data_next`] to get the actual
/// data in the iterator.
///
/// # Safety
///
/// Engine is responsible for passing a valid [`SharedExternEngine`] and [`SharedScan`]
#[no_mangle]
pub unsafe extern "C" fn kernel_scan_data_init(
    engine: Handle<SharedExternEngine>,
    scan: Handle<SharedScan>,
) -> ExternResult<Handle<SharedScanDataIterator>> {
    let engine = unsafe { engine.clone_as_arc() };
    let scan = unsafe { scan.as_ref() };
    kernel_scan_data_init_impl(&engine, scan).into_extern_result(&engine.as_ref())
}

fn kernel_scan_data_init_impl(
    engine: &Arc<dyn ExternEngine>,
    scan: &Scan,
) -> DeltaResult<Handle<SharedScanDataIterator>> {
    let scan_data = scan.scan_data(engine.engine().as_ref())?;
    let data = KernelScanDataIterator {
        data: Mutex::new(Box::new(scan_data)),
        engine: engine.clone(),
    };
    Ok(Arc::new(data).into())
}

/// # Safety
///
/// The iterator must be valid (returned by [kernel_scan_data_init]) and not yet freed by
/// [kernel_scan_data_free]. The visitor function pointer must be non-null.
#[no_mangle]
pub unsafe extern "C" fn kernel_scan_data_next(
    data: Handle<SharedScanDataIterator>,
    engine_context: NullableCvoid,
    engine_visitor: extern "C" fn(
        engine_context: NullableCvoid,
        engine_data: Handle<EngineData>,
        selection_vector: KernelBoolSlice,
    ),
) -> ExternResult<bool> {
    let data = unsafe { data.as_ref() };
    kernel_scan_data_next_impl(data, engine_context, engine_visitor)
        .into_extern_result(&data.engine.as_ref())
}
fn kernel_scan_data_next_impl(
    data: &KernelScanDataIterator,
    engine_context: NullableCvoid,
    engine_visitor: extern "C" fn(
        engine_context: NullableCvoid,
        engine_data: Handle<EngineData>,
        selection_vector: KernelBoolSlice,
    ),
) -> DeltaResult<bool> {
    let mut data = data
        .data
        .lock()
        .map_err(|_| Error::generic("poisoned mutex"))?;
    if let Some((data, sel_vec)) = data.next().transpose()? {
        let bool_slice = KernelBoolSlice::from(sel_vec);
        (engine_visitor)(engine_context, data.into(), bool_slice);
        Ok(true)
    } else {
        Ok(false)
    }
}

/// # Safety
///
/// Caller is responsible for (at most once) passing a valid pointer returned by a call to
/// [`kernel_scan_data_init`].
// we should probably be consistent with drop vs. free on engine side (probably the latter is more
// intuitive to non-rust code)
#[no_mangle]
pub unsafe extern "C" fn kernel_scan_data_free(data: Handle<SharedScanDataIterator>) {
    data.drop_handle();
}

type CScanCallback = extern "C" fn(
    engine_context: NullableCvoid,
    path: KernelStringSlice,
    size: i64,
    dv_info: &DvInfo,
    partition_map: &CStringMap,
);

pub struct CStringMap {
    values: HashMap<String, String>,
}

#[no_mangle]
/// allow probing into a CStringMap. If the specified key is in the map, kernel will call
/// allocate_fn with the value associated with the key and return the value returned from that
/// function. If the key is not in the map, this will return NULL
///
/// # Safety
///
/// The engine is responsible for providing a valid [`CStringMap`] pointer and [`KernelStringSlice`]
pub unsafe extern "C" fn get_from_map(
    map: &CStringMap,
    key: KernelStringSlice,
    allocate_fn: AllocateStringFn,
) -> NullableCvoid {
    // TODO: Return ExternResult to caller instead of panicking?
    let string_key = unsafe { String::try_from_slice(key) };
    map.values
        .get(&string_key.unwrap())
        .and_then(|v| allocate_fn(v.into()))
}

/// Get a selection vector out of a [`DvInfo`] struct
///
/// # Safety
/// Engine is responsible for providing valid pointers for each argument
#[no_mangle]
pub unsafe extern "C" fn selection_vector_from_dv(
    dv_info: &DvInfo,
    engine: Handle<SharedExternEngine>,
    state: Handle<SharedGlobalScanState>,
) -> ExternResult<KernelBoolSlice> {
    let state = unsafe { state.as_ref() };
    let engine = unsafe { engine.as_ref() };
    selection_vector_from_dv_impl(dv_info, engine, state).into_extern_result(&engine)
}

fn selection_vector_from_dv_impl(
    dv_info: &DvInfo,
    extern_engine: &dyn ExternEngine,
    state: &GlobalScanState,
) -> DeltaResult<KernelBoolSlice> {
    let root_url = Url::parse(&state.table_root)?;
    match dv_info.get_selection_vector(extern_engine.engine().as_ref(), &root_url)? {
        Some(v) => Ok(v.into()),
        None => Ok(KernelBoolSlice::empty()),
    }
}

// Wrapper function that gets called by the kernel, transforms the arguments to make the ffi-able,
// and then calls the ffi specified callback
fn rust_callback(
    context: &mut ContextWrapper,
    path: &str,
    size: i64,
    dv_info: DvInfo,
    partition_values: HashMap<String, String>,
) {
    let partition_map = CStringMap {
        values: partition_values,
    };
    (context.callback)(
        context.engine_context,
        path.into(),
        size,
        &dv_info,
        &partition_map,
    );
}

// Wrap up stuff from C so we can pass it through to our callback
struct ContextWrapper {
    engine_context: NullableCvoid,
    callback: CScanCallback,
}

/// Shim for ffi to call visit_scan_data. This will generally be called when iterating through scan
/// data which provides the data handle and selection vector as each element in the iterator.
///
/// # Safety
/// engine is responsbile for passing a valid [`EngineData`] and selection vector.
#[no_mangle]
pub unsafe extern "C" fn visit_scan_data(
    data: Handle<EngineData>,
    selection_vec: KernelBoolSlice,
    engine_context: NullableCvoid,
    callback: CScanCallback,
) {
    let selection_vec = unsafe { selection_vec.as_ref() };
    let data = unsafe { data.as_ref() };
    let context_wrapper = ContextWrapper {
        engine_context,
        callback,
    };
    // TODO: return ExternResult to caller instead of panicking?
    visit_scan_files(data, selection_vec, context_wrapper, rust_callback).unwrap();
}
