//! Scan related ffi code

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use delta_kernel::scan::state::{DvInfo, GlobalScanState};
use delta_kernel::scan::{Scan, ScanMetadata};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::{DeltaResult, Error, Expression, ExpressionRef};
use delta_kernel_ffi_macros::handle_descriptor;
use tracing::debug;
use url::Url;

use crate::expressions::engine::{
    unwrap_kernel_expression, EnginePredicate, KernelExpressionVisitorState,
};
use crate::expressions::SharedExpression;
use crate::{
    kernel_string_slice, AllocateStringFn, ExternEngine, ExternResult, IntoExternResult,
    KernelBoolSlice, KernelRowIndexArray, KernelStringSlice, NullableCvoid, SharedExternEngine,
    SharedSchema, SharedSnapshot, TryFromStringSlice,
};

use super::handle::Handle;

// TODO: Why do we even need to expose a scan, when the only thing an engine can do with it is
// handit back to the kernel by calling `scan_metadata_iter_init`? There isn't even an FFI method to
// drop it!
#[handle_descriptor(target=Scan, mutable=false, sized=true)]
pub struct SharedScan;

#[handle_descriptor(target=ScanMetadata, mutable=false, sized=true)]
pub struct SharedScanMetadata;

/// Drop a `SharedScanMetadata`.
///
/// # Safety
///
/// Caller is responsible for passing a valid scan data handle.
#[no_mangle]
pub unsafe extern "C" fn free_scan_metadata(scan_metadata: Handle<SharedScanMetadata>) {
    scan_metadata.drop_handle();
}

/// Get a selection vector out of a [`SharedScanMetadata`] struct
///
/// # Safety
/// Engine is responsible for providing valid pointers for each argument
#[no_mangle]
pub unsafe extern "C" fn selection_vector_from_scan_metadata(
    scan_metadata: Handle<SharedScanMetadata>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<KernelBoolSlice> {
    let scan_metadata = unsafe { scan_metadata.as_ref() };
    selection_vector_from_scan_metadata_impl(scan_metadata).into_extern_result(&engine.as_ref())
}

fn selection_vector_from_scan_metadata_impl(
    scan_metadata: &ScanMetadata,
) -> DeltaResult<KernelBoolSlice> {
    Ok(scan_metadata.scan_files.selection_vector.clone().into())
}

/// Drops a scan.
///
/// # Safety
/// Caller is responsible for passing a valid scan handle.
#[no_mangle]
pub unsafe extern "C" fn free_scan(scan: Handle<SharedScan>) {
    scan.drop_handle();
}

/// Get a [`Scan`] over the table specified by the passed snapshot. It is the responsibility of the
/// _engine_ to free this scan when complete by calling [`free_scan`].
///
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
    let mut scan_builder = snapshot.scan_builder();
    if let Some(predicate) = predicate {
        let mut visitor_state = KernelExpressionVisitorState::default();
        let pred_id = (predicate.visitor)(predicate.predicate, &mut visitor_state);
        let predicate = unwrap_kernel_expression(&mut visitor_state, pred_id);
        debug!("Got predicate: {:#?}", predicate);
        scan_builder = scan_builder.with_predicate(predicate.map(Arc::new));
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

/// Get the kernel view of the physical read schema that an engine should read from parquet file in
/// a scan
///
/// # Safety
/// Engine is responsible for providing a valid GlobalScanState pointer
#[no_mangle]
pub unsafe extern "C" fn get_global_read_schema(
    state: Handle<SharedGlobalScanState>,
) -> Handle<SharedSchema> {
    let state = unsafe { state.as_ref() };
    state.physical_schema.clone().into()
}

/// Get the kernel view of the physical read schema that an engine should read from parquet file in
/// a scan
///
/// # Safety
/// Engine is responsible for providing a valid GlobalScanState pointer
#[no_mangle]
pub unsafe extern "C" fn get_global_logical_schema(
    state: Handle<SharedGlobalScanState>,
) -> Handle<SharedSchema> {
    let state = unsafe { state.as_ref() };
    state.logical_schema.clone().into()
}

/// # Safety
///
/// Caller is responsible for passing a valid global scan state pointer.
#[no_mangle]
pub unsafe extern "C" fn free_global_scan_state(state: Handle<SharedGlobalScanState>) {
    state.drop_handle();
}

// Intentionally opaque to the engine.
//
// TODO: This approach liberates the engine from having to worry about mutual exclusion, but that
// means kernel made the decision of how to achieve thread safety. This may not be desirable if the
// engine is single-threaded, or has its own mutual exclusion mechanisms. Deadlock is even a
// conceivable risk, if this interacts poorly with engine's mutual exclusion mechanism.
pub struct ScanMetadataIterator {
    // Mutex -> Allow the iterator to be accessed safely by multiple threads.
    // Box -> Wrap its unsized content this struct is fixed-size with thin pointers.
    // Item = DeltaResult<ScanMetadata>
    data: Mutex<Box<dyn Iterator<Item = DeltaResult<ScanMetadata>> + Send>>,

    // Also keep a reference to the external engine for its error allocator. The default Parquet and
    // Json handlers don't hold any reference to the tokio reactor they rely on, so the iterator
    // terminates early if the last engine goes out of scope.
    engine: Arc<dyn ExternEngine>,
}

#[handle_descriptor(target=ScanMetadataIterator, mutable=false, sized=true)]
pub struct SharedScanMetadataIterator;

impl Drop for ScanMetadataIterator {
    fn drop(&mut self) {
        debug!("dropping ScanMetadataIterator");
    }
}

/// Get an iterator over the data needed to perform a scan. This will return a
/// [`ScanMetadataIterator`] which can be passed to [`scan_metadata_next`] to get the
/// actual data in the iterator.
///
/// # Safety
///
/// Engine is responsible for passing a valid [`SharedExternEngine`] and [`SharedScan`]
#[no_mangle]
pub unsafe extern "C" fn scan_metadata_iter_init(
    engine: Handle<SharedExternEngine>,
    scan: Handle<SharedScan>,
) -> ExternResult<Handle<SharedScanMetadataIterator>> {
    let engine = unsafe { engine.clone_as_arc() };
    let scan = unsafe { scan.as_ref() };
    scan_metadata_iter_init_impl(&engine, scan).into_extern_result(&engine.as_ref())
}

fn scan_metadata_iter_init_impl(
    engine: &Arc<dyn ExternEngine>,
    scan: &Scan,
) -> DeltaResult<Handle<SharedScanMetadataIterator>> {
    let scan_metadata = scan.scan_metadata(engine.engine().as_ref())?;
    let data = ScanMetadataIterator {
        data: Mutex::new(Box::new(scan_metadata)),
        engine: engine.clone(),
    };
    Ok(Arc::new(data).into())
}

/// Call the provided `engine_visitor` on the next scan metadata item. The visitor will be provided with
/// a [`SharedScanMetadata`], which contains the actual scan files and the associated selection vector. It is the
/// responsibility of the _engine_ to free the associated resources after use by calling
/// [`free_engine_data`] and [`free_bool_slice`] respectively.
///
/// # Safety
///
/// The iterator must be valid (returned by [scan_metadata_iter_init]) and not yet freed by
/// [`free_scan_metadata_iter`]. The visitor function pointer must be non-null.
///
/// [`free_bool_slice`]: crate::free_bool_slice
/// [`free_engine_data`]: crate::free_engine_data
#[no_mangle]
pub unsafe extern "C" fn scan_metadata_next(
    data: Handle<SharedScanMetadataIterator>,
    engine_context: NullableCvoid,
    engine_visitor: extern "C" fn(
        engine_context: NullableCvoid,
        scan_metadata: Handle<SharedScanMetadata>,
    ),
) -> ExternResult<bool> {
    let data = unsafe { data.as_ref() };
    scan_metadata_next_impl(data, engine_context, engine_visitor)
        .into_extern_result(&data.engine.as_ref())
}
fn scan_metadata_next_impl(
    data: &ScanMetadataIterator,
    engine_context: NullableCvoid,
    engine_visitor: extern "C" fn(
        engine_context: NullableCvoid,
        scan_metadata: Handle<SharedScanMetadata>,
    ),
) -> DeltaResult<bool> {
    let mut data = data
        .data
        .lock()
        .map_err(|_| Error::generic("poisoned mutex"))?;
    if let Some(scan_metadata) = data.next().transpose()? {
        (engine_visitor)(engine_context, Arc::new(scan_metadata).into());
        Ok(true)
    } else {
        Ok(false)
    }
}

/// # Safety
///
/// Caller is responsible for (at most once) passing a valid pointer returned by a call to
/// [`scan_metadata_iter_init`].
// we should probably be consistent with drop vs. free on engine side (probably the latter is more
// intuitive to non-rust code)
#[no_mangle]
pub unsafe extern "C" fn free_scan_metadata_iter(data: Handle<SharedScanMetadataIterator>) {
    data.drop_handle();
}

/// Give engines an easy way to consume stats
#[repr(C)]
pub struct Stats {
    /// For any file where the deletion vector is not present (see [`DvInfo::has_vector`]), the
    /// `num_records` statistic must be present and accurate, and must equal the number of records
    /// in the data file. In the presence of Deletion Vectors the statistics may be somewhat
    /// outdated, i.e. not reflecting deleted rows yet.
    pub num_records: u64,
}

/// This callback will be invoked for each valid file that needs to be read for a scan.
///
/// The arguments to the callback are:
/// * `context`: a `void*` context this can be anything that engine needs to pass through to each call
/// * `path`: a `KernelStringSlice` which is the path to the file
/// * `size`: an `i64` which is the size of the file
/// * `dv_info`: a [`DvInfo`] struct, which allows getting the selection vector for this file
/// * `transform`: An optional expression that, if not `NULL`, _must_ be applied to physical data to
///   convert it to the correct logical format. If this is `NULL`, no transform is needed.
/// * `partition_values`: [DEPRECATED] a `HashMap<String, String>` which are partition values
type CScanCallback = extern "C" fn(
    engine_context: NullableCvoid,
    path: KernelStringSlice,
    size: i64,
    stats: Option<&Stats>,
    dv_info: &DvInfo,
    transform: Option<&Expression>,
    partition_map: &CStringMap,
);

#[derive(Default)]
pub struct CStringMap {
    values: HashMap<String, String>,
}

impl From<HashMap<String, String>> for CStringMap {
    fn from(val: HashMap<String, String>) -> Self {
        Self { values: val }
    }
}

#[no_mangle]
/// allow probing into a CStringMap. If the specified key is in the map, kernel will call
/// allocate_fn with the value associated with the key and return the value returned from that
/// function. If the key is not in the map, this will return NULL
///
/// # Safety
///
/// The engine is responsible for providing a valid [`CStringMap`] pointer and [`KernelStringSlice`]
pub unsafe extern "C" fn get_from_string_map(
    map: &CStringMap,
    key: KernelStringSlice,
    allocate_fn: AllocateStringFn,
) -> NullableCvoid {
    // TODO: Return ExternResult to caller instead of panicking?
    let string_key = unsafe { TryFromStringSlice::try_from_slice(&key) };
    map.values
        .get(string_key.unwrap())
        .and_then(|v| allocate_fn(kernel_string_slice!(v)))
}

/// Transformation expressions that need to be applied to each row `i` in ScanMetadata. You can use
/// [`get_transform_for_row`] to get the transform for a particular row. If that returns an
/// associated expression, it _must_ be applied to the data read from the file specified by the
/// row. The resultant schema for this expression is guaranteed to be `Scan.schema()`. If
/// `get_transform_for_row` returns `NULL` no expression need be applied and the data read from disk
/// is already in the correct logical state.
///
/// NB: If you are using `visit_scan_metadata` you don't need to worry about dealing with probing
/// `CTransforms`. The callback will be invoked with the correct transform for you.
pub struct CTransforms {
    transforms: Vec<Option<ExpressionRef>>,
}

#[no_mangle]
/// Allow getting the transform for a particular row. If the requested row is outside the range of
/// the passed `CTransforms` returns `NULL`, otherwise returns the element at the index of the
/// specified row. See also [`CTransforms`] above.
///
/// # Safety
///
/// The engine is responsible for providing a valid [`CTransforms`] pointer, and for checking if the
/// return value is `NULL` or not.
pub unsafe extern "C" fn get_transform_for_row(
    row: usize,
    transforms: &CTransforms,
) -> Option<Handle<SharedExpression>> {
    transforms
        .transforms
        .get(row)
        .cloned()
        .flatten()
        .map(Into::into)
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

/// Get a vector of row indexes out of a [`DvInfo`] struct
///
/// # Safety
/// Engine is responsible for providing valid pointers for each argument
#[no_mangle]
pub unsafe extern "C" fn row_indexes_from_dv(
    dv_info: &DvInfo,
    engine: Handle<SharedExternEngine>,
    state: Handle<SharedGlobalScanState>,
) -> ExternResult<KernelRowIndexArray> {
    let state = unsafe { state.as_ref() };
    let engine = unsafe { engine.as_ref() };
    row_indexes_from_dv_impl(dv_info, engine, state).into_extern_result(&engine)
}

fn row_indexes_from_dv_impl(
    dv_info: &DvInfo,
    extern_engine: &dyn ExternEngine,
    state: &GlobalScanState,
) -> DeltaResult<KernelRowIndexArray> {
    let root_url = Url::parse(&state.table_root)?;
    match dv_info.get_row_indexes(extern_engine.engine().as_ref(), &root_url)? {
        Some(v) => Ok(v.into()),
        None => Ok(KernelRowIndexArray::empty()),
    }
}

// Wrapper function that gets called by the kernel, transforms the arguments to make the ffi-able,
// and then calls the ffi specified callback
fn rust_callback(
    context: &mut ContextWrapper,
    path: &str,
    size: i64,
    kernel_stats: Option<delta_kernel::scan::state::Stats>,
    dv_info: DvInfo,
    transform: Option<ExpressionRef>,
    partition_values: HashMap<String, String>,
) {
    let transform = transform.map(|e| e.as_ref().clone());
    let partition_map = CStringMap {
        values: partition_values,
    };
    let stats = kernel_stats.map(|ks| Stats {
        num_records: ks.num_records,
    });
    (context.callback)(
        context.engine_context,
        kernel_string_slice!(path),
        size,
        stats.as_ref(),
        &dv_info,
        transform.as_ref(),
        &partition_map,
    );
}

// Wrap up stuff from C so we can pass it through to our callback
struct ContextWrapper {
    engine_context: NullableCvoid,
    callback: CScanCallback,
}

/// Shim for ffi to call visit_scan_metadata. This will generally be called when iterating through scan
/// data which provides the [`SharedScanMetadata`] as each element in the iterator.
///
/// # Safety
/// engine is responsible for passing a valid [`SharedScanMetadata`].
#[no_mangle]
pub unsafe extern "C" fn visit_scan_metadata(
    scan_metadata: Handle<SharedScanMetadata>,
    engine_context: NullableCvoid,
    callback: CScanCallback,
) {
    let scan_metadata = unsafe { scan_metadata.as_ref() };
    let context_wrapper = ContextWrapper {
        engine_context,
        callback,
    };

    // TODO: return ExternResult to caller instead of panicking?
    scan_metadata
        .visit_scan_files(context_wrapper, rust_callback)
        .unwrap();
}
