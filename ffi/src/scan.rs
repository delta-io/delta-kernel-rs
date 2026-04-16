//! Scan related ffi code

use std::collections::HashMap;
use std::ffi::c_void;
use std::sync::{Arc, Mutex};

use delta_kernel::scan::state::{DvInfo, ScanFile};
use delta_kernel::scan::{Scan, ScanBuilder, ScanMetadata};
use delta_kernel::snapshot::SnapshotRef;
use delta_kernel::{DeltaResult, Error, Expression, ExpressionRef};
use delta_kernel_ffi_macros::handle_descriptor;
use tracing::debug;
use url::Url;

#[cfg(feature = "default-engine-base")]
use crate::engine_data::ArrowFFIData;
use crate::expressions::kernel_visitor::{unwrap_kernel_predicate, KernelExpressionVisitorState};
use crate::expressions::SharedExpression;
use crate::schema_visitor::{extract_kernel_schema, KernelSchemaVisitorState};
use crate::{
    kernel_string_slice, unwrap_and_parse_path_as_url, AllocateStringFn, ExternEngine,
    ExternResult, IntoExternResult, KernelBoolSlice, KernelRowIndexArray, KernelStringSlice,
    NullableCvoid, OptionalValue, SharedExternEngine, SharedSchema, SharedSnapshot,
    TryFromStringSlice,
};

use super::handle::Handle;

#[handle_descriptor(target=Scan, mutable=false, sized=true)]
pub struct SharedScan;

#[handle_descriptor(target=ScanMetadata, mutable=false, sized=true)]
pub struct SharedScanMetadata;

/// An opaque, exclusive handle owning a [`ScanBuilder`].
///
/// The caller must eventually either call [`scan_builder_build`] (which consumes the handle
/// and produces a [`SharedScan`]) or [`free_scan_builder`] (which drops it without building).
#[handle_descriptor(target=ScanBuilder, mutable=true, sized=true)]
pub struct ExclusiveScanBuilder;

/// A predicate that can be used to skip data when scanning.
///
/// Used by [`scan`] and [`scan_builder_with_predicate`]. The engine provides a pointer to its
/// native predicate along with a visitor function that recursively visits it. This engine state
/// must remain valid for the duration of the call. The kernel allocates visitor state internally,
/// which becomes the second argument to the visitor invocation. Thanks to this double indirection,
/// engine and kernel each retain ownership of their respective objects with no need to coordinate
/// memory lifetimes.
#[repr(C)]
pub struct EnginePredicate {
    pub predicate: *mut c_void,
    pub visitor:
        extern "C" fn(predicate: *mut c_void, state: &mut KernelExpressionVisitorState) -> usize,
}

/// An engine-provided schema along with a visitor function to convert it to a kernel schema.
///
/// Used by [`scan`] and [`scan_builder_with_schema`] for projection pushdown, and by
/// [`get_create_table_builder`] to specify the table schema at creation time. The engine
/// provides a pointer to its native schema representation along with a visitor function. The
/// kernel allocates visitor state internally, which becomes the second argument to the schema
/// visitor invocation. Thanks to this double indirection, engine and kernel each retain
/// ownership of their respective objects with no need to coordinate memory lifetimes.
///
/// [`get_create_table_builder`]: crate::transaction::get_create_table_builder
#[repr(C)]
pub struct EngineSchema {
    pub schema: *mut c_void,
    pub visitor: extern "C" fn(schema: *mut c_void, state: &mut KernelSchemaVisitorState) -> usize,
}

/// An engine-provided expression along with a visitor function to convert
/// it to a kernel expression.
///
/// The engine provides a pointer to its own expression representation, along
/// with a visitor function that can convert it to a kernel expression by
/// calling the appropriate visitor methods on the kernel's
/// `KernelExpressionVisitorState`. The visitor function returns an expression
/// ID that can be converted to a kernel expression handle.
#[repr(C)]
pub struct EngineExpression {
    pub expression: *mut c_void,
    pub visitor:
        extern "C" fn(expression: *mut c_void, state: &mut KernelExpressionVisitorState) -> usize,
}

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
    Ok(scan_metadata.scan_files.selection_vector().to_vec().into())
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
    schema: Option<&EngineSchema>,
) -> ExternResult<Handle<SharedScan>> {
    let snapshot = unsafe { snapshot.clone_as_arc() };
    scan_impl(snapshot, predicate, schema).into_extern_result(&engine.as_ref())
}

/// Decode an [`EnginePredicate`] and apply it to a [`ScanBuilder`].
///
/// Returns an error if the engine's visitor fails to produce a valid predicate (i.e. returns
/// an invalid expression ID). A `None` result from the visitor indicates the engine-side
/// predicate construction failed, which would silently produce a full-table scan if ignored.
fn apply_predicate(
    builder: ScanBuilder,
    predicate: &mut EnginePredicate,
) -> DeltaResult<ScanBuilder> {
    let mut visitor_state = KernelExpressionVisitorState::default();
    let pred_id = (predicate.visitor)(predicate.predicate, &mut visitor_state);
    let predicate = unwrap_kernel_predicate(&mut visitor_state, pred_id).ok_or_else(|| {
        delta_kernel::Error::generic(
            "engine predicate visitor returned an invalid expression ID; \
             predicate could not be decoded",
        )
    })?;
    debug!("Got predicate: {:#?}", predicate);
    Ok(builder.with_predicate(Some(Arc::new(predicate))))
}

/// Decode an [`EngineSchema`] and apply it as a column projection to a [`ScanBuilder`].
///
/// Returns an error if the schema visitor produces an invalid schema.
fn apply_schema(builder: ScanBuilder, schema: &EngineSchema) -> DeltaResult<ScanBuilder> {
    let mut visitor_state = KernelSchemaVisitorState::default();
    let schema_id = (schema.visitor)(schema.schema, &mut visitor_state);
    let schema = extract_kernel_schema(&mut visitor_state, schema_id)?;
    debug!("FFI scan projection schema: {:#?}", schema);
    Ok(builder.with_schema(Arc::new(schema)))
}

fn scan_impl(
    snapshot: SnapshotRef,
    predicate: Option<&mut EnginePredicate>,
    schema: Option<&EngineSchema>,
) -> DeltaResult<Handle<SharedScan>> {
    let mut scan_builder = snapshot.scan_builder();
    if let Some(predicate) = predicate {
        scan_builder = apply_predicate(scan_builder, predicate)?;
    }
    if let Some(schema) = schema {
        scan_builder = apply_schema(scan_builder, schema)?;
    }
    Ok(Arc::new(scan_builder.build()?).into())
}

/// Create a [`ScanBuilder`] for the given snapshot.
///
/// The caller owns the returned handle and must eventually call either
/// [`scan_builder_build`] to produce a [`SharedScan`], or [`free_scan_builder`] to drop it
/// without building.
///
/// This function is infallible; constructing a [`ScanBuilder`] from a snapshot always succeeds.
///
/// # Safety
///
/// `snapshot` must be a valid [`SharedSnapshot`] handle.
#[no_mangle]
pub unsafe extern "C" fn scan_builder(
    snapshot: Handle<SharedSnapshot>,
) -> Handle<ExclusiveScanBuilder> {
    let snapshot = unsafe { snapshot.clone_as_arc() };
    Box::new(snapshot.scan_builder()).into()
}

/// Apply a predicate to an [`ExclusiveScanBuilder`] for data skipping and row-level filtering.
///
/// Consumes the `builder` handle and returns a new handle with the predicate applied. The
/// `builder` handle must not be used after this call. Returns an error if the engine's predicate
/// visitor fails to produce a valid predicate (i.e. returns an invalid expression ID). On error,
/// the builder is dropped.
///
/// # Safety
///
/// `builder` and `engine` must be valid handles. The `builder` handle must not be used after this
/// call. `predicate` must be a valid, non-null [`EnginePredicate`] whose `visitor` and `predicate`
/// fields are safe to call and read.
#[no_mangle]
pub unsafe extern "C" fn scan_builder_with_predicate(
    builder: Handle<ExclusiveScanBuilder>,
    engine: Handle<SharedExternEngine>,
    predicate: &mut EnginePredicate,
) -> ExternResult<Handle<ExclusiveScanBuilder>> {
    let engine = unsafe { engine.as_ref() };
    let builder = unsafe { builder.into_inner() };
    apply_predicate(*builder, predicate)
        .map(|b| Box::new(b).into())
        .into_extern_result(&engine)
}

/// Apply a column projection schema to an [`ExclusiveScanBuilder`].
///
/// Consumes the `builder` handle and returns a new handle with the schema applied. The `builder`
/// handle must not be used after this call. Returns an error if the schema visitor produces an
/// invalid schema, such as a non-struct root or unconsumed field IDs. On error, the builder is
/// dropped.
///
/// # Safety
///
/// `builder` and `engine` must be valid handles. The `builder` handle must not be used after this
/// call. `schema` must be a valid, non-null [`EngineSchema`] whose `visitor` and `schema` fields
/// are safe to call and read.
#[no_mangle]
pub unsafe extern "C" fn scan_builder_with_schema(
    builder: Handle<ExclusiveScanBuilder>,
    engine: Handle<SharedExternEngine>,
    schema: &EngineSchema,
) -> ExternResult<Handle<ExclusiveScanBuilder>> {
    let engine = unsafe { engine.as_ref() };
    scan_builder_with_schema_impl(builder, schema).into_extern_result(&engine)
}

fn scan_builder_with_schema_impl(
    builder: Handle<ExclusiveScanBuilder>,
    schema: &EngineSchema,
) -> DeltaResult<Handle<ExclusiveScanBuilder>> {
    let builder = unsafe { builder.into_inner() };
    Ok(Box::new(apply_schema(*builder, schema)?).into())
}

/// Consume an [`ExclusiveScanBuilder`] and produce a [`SharedScan`].
///
/// The `builder` handle is consumed and must not be used afterward. On error, the builder is
/// dropped and an error is returned. It is the responsibility of the caller to free the returned
/// scan handle by calling [`free_scan`].
///
/// # Safety
///
/// `builder` and `engine` must be valid handles. The `builder` handle must not be used after
/// this call.
#[no_mangle]
pub unsafe extern "C" fn scan_builder_build(
    builder: Handle<ExclusiveScanBuilder>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<SharedScan>> {
    let engine = unsafe { engine.as_ref() };
    let builder = unsafe { builder.into_inner() };
    builder
        .build()
        .map(|scan| Arc::new(scan).into())
        .into_extern_result(&engine)
}

/// Free an [`ExclusiveScanBuilder`] without building a scan.
///
/// Only call this if you will not call [`scan_builder_build`]. If you have already called
/// [`scan_builder_build`], the builder handle was consumed and this must not be called.
///
/// # Safety
///
/// `builder` must be a valid handle that has not been previously consumed or freed.
#[no_mangle]
pub unsafe extern "C" fn free_scan_builder(builder: Handle<ExclusiveScanBuilder>) {
    builder.drop_handle();
}

/// Get the table root of a scan.
///
/// # Safety
/// Engine is responsible for providing a valid scan pointer and allocate_fn (for allocating the
/// string)
#[no_mangle]
pub unsafe extern "C" fn scan_table_root(
    scan: Handle<SharedScan>,
    allocate_fn: AllocateStringFn,
) -> NullableCvoid {
    let scan = unsafe { scan.as_ref() };
    let table_root = scan.table_root().to_string();
    allocate_fn(kernel_string_slice!(table_root))
}

/// Get the logical (i.e. output) schema of a scan.
///
/// # Safety
/// Engine is responsible for providing a valid `SharedScan` handle
#[no_mangle]
pub unsafe extern "C" fn scan_logical_schema(scan: Handle<SharedScan>) -> Handle<SharedSchema> {
    let scan = unsafe { scan.as_ref() };
    scan.logical_schema().clone().into()
}

/// Get the kernel view of the physical read schema that an engine should read from parquet file in
/// a scan
///
/// # Safety
/// Engine is responsible for providing a valid `SharedScan` handle
#[no_mangle]
pub unsafe extern "C" fn scan_physical_schema(scan: Handle<SharedScan>) -> Handle<SharedSchema> {
    let scan = unsafe { scan.as_ref() };
    scan.physical_schema().clone().into()
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

/// Contains information that can be used to get a selection vector. If `has_vector` is false, that
/// indicates there is no selection vector to consider. It is always possible to get a vector out of
/// a `DvInfo`, but if `has_vector` is false it will just be an empty vector (indicating all
/// selected). Without this there's no way for a connector using ffi to know if a &DvInfo actually
/// has a vector in it. We have has_vector() on the rust side, but this isn't exposed via ffi. So
/// this just wraps the &DvInfo in another struct which includes a boolean that says if there is a
/// dv to consider or not.  This allows engines to ignore dv info if there isn't any without needing
/// to make another ffi call at all.
#[repr(C)]
pub struct CDvInfo<'a> {
    info: &'a DvInfo,
    has_vector: bool,
}

/// This callback will be invoked for each valid file that needs to be read for a scan.
///
/// The arguments to the callback are:
/// * `context`: a `void*` context this can be anything that engine needs to pass through to each call
/// * `path`: a `KernelStringSlice` which is the path to the file
/// * `size`: an `i64` which is the size of the file
/// * `mod_time`: an `i64` which is the time the file was created, as milliseconds since the epoch
/// * `dv_info`: a [`CDvInfo`] struct, which allows getting the selection vector for this file
/// * `transform`: An optional expression that, if not `NULL`, _must_ be applied to physical data to
///   convert it to the correct logical format. If this is `NULL`, no transform is needed.
/// * `partition_values`: [DEPRECATED] a `HashMap<String, String>` which are partition values
type CScanCallback = extern "C" fn(
    engine_context: NullableCvoid,
    path: KernelStringSlice,
    size: i64,
    mod_time: i64,
    stats: Option<&Stats>,
    dv_info: &CDvInfo,
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
    engine: Handle<SharedExternEngine>,
) -> ExternResult<NullableCvoid> {
    let engine = unsafe { engine.as_ref() };
    get_from_string_map_impl(map, key, allocate_fn).into_extern_result(&engine)
}
fn get_from_string_map_impl(
    map: &CStringMap,
    key: KernelStringSlice,
    allocate_fn: AllocateStringFn,
) -> DeltaResult<NullableCvoid> {
    let string_key = unsafe { TryFromStringSlice::try_from_slice(&key) }?;
    Ok(map
        .values
        .get(string_key)
        .and_then(|v| allocate_fn(kernel_string_slice!(v))))
}

/// Visit all values in a CStringMap. The callback will be called once for each element of the map
///
/// # Safety
///
/// The engine is responsible for providing a valid [`CStringMap`] pointer and callback
#[no_mangle]
pub unsafe extern "C" fn visit_string_map(
    map: &CStringMap,
    engine_context: NullableCvoid,
    visitor: extern "C" fn(
        engine_context: NullableCvoid,
        key: KernelStringSlice,
        value: KernelStringSlice,
    ),
) {
    for (key, val) in map.values.iter() {
        visitor(
            engine_context,
            kernel_string_slice!(key),
            kernel_string_slice!(val),
        );
    }
}

/// Transformation expressions that need to be applied to each row `i` in ScanMetadata. You can use
/// [`get_transform_for_row`] to get the transform for a particular row. If that returns an
/// associated expression, it _must_ be applied to the data read from the file specified by the
/// row. The resultant schema for this expression is guaranteed to be [`scan_logical_schema()`]. If
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
) -> OptionalValue<Handle<SharedExpression>> {
    transforms
        .transforms
        .get(row)
        .cloned()
        .flatten()
        .map(Into::into)
        .into()
}

/// Get a selection vector out of a [`DvInfo`] struct
///
/// # Safety
/// Engine is responsible for providing valid pointers for each argument
#[no_mangle]
pub unsafe extern "C" fn selection_vector_from_dv(
    dv_info: &DvInfo,
    engine: Handle<SharedExternEngine>,
    root_url: KernelStringSlice,
) -> ExternResult<KernelBoolSlice> {
    let engine = unsafe { engine.as_ref() };
    let root_url = unsafe { unwrap_and_parse_path_as_url(root_url) };
    selection_vector_from_dv_impl(dv_info, engine, root_url).into_extern_result(&engine)
}

fn selection_vector_from_dv_impl(
    dv_info: &DvInfo,
    extern_engine: &dyn ExternEngine,
    root_url: DeltaResult<Url>,
) -> DeltaResult<KernelBoolSlice> {
    match dv_info.get_selection_vector(extern_engine.engine().as_ref(), &root_url?)? {
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
    root_url: KernelStringSlice,
) -> ExternResult<KernelRowIndexArray> {
    let engine = unsafe { engine.as_ref() };
    let root_url = unsafe { unwrap_and_parse_path_as_url(root_url) };
    row_indexes_from_dv_impl(dv_info, engine, root_url).into_extern_result(&engine)
}

fn row_indexes_from_dv_impl(
    dv_info: &DvInfo,
    extern_engine: &dyn ExternEngine,
    root_url: DeltaResult<Url>,
) -> DeltaResult<KernelRowIndexArray> {
    match dv_info.get_row_indexes(extern_engine.engine().as_ref(), &root_url?)? {
        Some(v) => Ok(v.into()),
        None => Ok(KernelRowIndexArray::empty()),
    }
}

// Wrapper function that gets called by the kernel, transforms the arguments to make the ffi-able,
// and then calls the ffi specified callback
fn rust_callback(context: &mut ContextWrapper, scan_file: ScanFile) {
    let transform = scan_file.transform.map(|e| e.as_ref().clone());
    let partition_map = CStringMap {
        values: scan_file.partition_values,
    };
    let stats = scan_file.stats.map(|ks| Stats {
        num_records: ks.num_records,
    });
    let cdv_info = CDvInfo {
        info: &scan_file.dv_info,
        has_vector: scan_file.dv_info.has_vector(),
    };
    let path = scan_file.path.as_str();
    (context.callback)(
        context.engine_context,
        kernel_string_slice!(path),
        scan_file.size,
        scan_file.modification_time,
        stats.as_ref(),
        &cdv_info,
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
    engine: Handle<SharedExternEngine>,
    engine_context: NullableCvoid,
    callback: CScanCallback,
) -> ExternResult<bool> {
    let scan_metadata = unsafe { scan_metadata.as_ref() };
    let engine = unsafe { engine.as_ref() };
    visit_scan_metadata_impl(scan_metadata, engine_context, callback).into_extern_result(&engine)
}
fn visit_scan_metadata_impl(
    scan_metadata: &ScanMetadata,
    engine_context: NullableCvoid,
    callback: CScanCallback,
) -> DeltaResult<bool> {
    let context_wrapper = ContextWrapper {
        engine_context,
        callback,
    };
    scan_metadata.visit_scan_files(context_wrapper, rust_callback)?;
    Ok(true)
}

// === Arrow batch-mode scan metadata ===

/// Result of [`scan_metadata_next_arrow`]: an Arrow C Data Interface batch, a selection
/// vector, and per-row transformation expressions.
///
/// The engine must free this by calling [`free_scan_metadata_arrow_result`] exactly once.
#[cfg(feature = "default-engine-base")]
#[repr(C)]
pub struct ScanMetadataArrowResult {
    /// Arrow C Data Interface batch containing scan file metadata (path, size, stats, etc.).
    pub arrow_data: ArrowFFIData,
    /// Boolean selection vector indicating active rows. Length equals the batch row count;
    /// `true` at index `i` means row `i` should be processed.
    pub selection_vector: KernelBoolSlice,
    /// Opaque pointer to per-row transformation expressions. Use [`get_transform_for_row`]
    /// with a row index to retrieve the transform for that row. If non-null, the transform
    /// must be applied to data read from the file to produce the correct logical schema.
    /// Owned by this struct and freed by [`free_scan_metadata_arrow_result`].
    pub transforms: *mut CTransforms,
}

/// Get the next scan metadata batch as Arrow via the C Data Interface.
///
/// Advances the iterator by one batch and returns a [`ScanMetadataArrowResult`] containing:
/// - An Arrow RecordBatch with scan row schema columns (path, size, modificationTime,
///   stats, deletionVector, fileConstantValues)
/// - A boolean selection vector indicating active rows (true = selected)
/// - Per-row transformation expressions (use [`get_transform_for_row`] to access)
///
/// Returns `Ok(non-null)` with the next batch, `Ok(null)` when the iterator is exhausted,
/// or `Err` if an error occurred during iteration.
///
/// This is an alternative to the callback-based [`scan_metadata_next`] +
/// [`visit_scan_metadata`] path, avoiding per-row FFI overhead.
///
/// # Safety
///
/// `data` must be a valid [`SharedScanMetadataIterator`] handle.
/// `engine` must be a valid [`SharedExternEngine`] handle.
#[cfg(feature = "default-engine-base")]
#[no_mangle]
pub unsafe extern "C" fn scan_metadata_next_arrow(
    data: Handle<SharedScanMetadataIterator>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<*mut ScanMetadataArrowResult> {
    let data = unsafe { data.as_ref() };
    let engine = unsafe { engine.as_ref() };
    scan_metadata_next_arrow_impl(data).into_extern_result(&engine)
}

#[cfg(feature = "default-engine-base")]
fn scan_metadata_next_arrow_impl(
    data: &ScanMetadataIterator,
) -> DeltaResult<*mut ScanMetadataArrowResult> {
    let mut iter = data
        .data
        .lock()
        .map_err(|_| Error::generic("poisoned mutex"))?;

    match iter.next().transpose()? {
        Some(scan_metadata) => {
            let (engine_data, selection_vector) = scan_metadata.scan_files.into_parts();
            let arrow_data = ArrowFFIData::try_from_engine_data(engine_data)?;
            let result = Box::new(ScanMetadataArrowResult {
                arrow_data,
                selection_vector: selection_vector.into(),
                transforms: Box::into_raw(Box::new(CTransforms {
                    transforms: scan_metadata.scan_file_transforms,
                })),
            });
            Ok(Box::into_raw(result))
        }
        None => Ok(std::ptr::null_mut()),
    }
}

/// Free a [`ScanMetadataArrowResult`] returned by [`scan_metadata_next_arrow`].
///
/// # Safety
///
/// `result` must be a valid pointer returned by [`scan_metadata_next_arrow`], or null.
/// Must be called at most once per result.
#[cfg(feature = "default-engine-base")]
#[no_mangle]
pub unsafe extern "C" fn free_scan_metadata_arrow_result(result: *mut ScanMetadataArrowResult) {
    if result.is_null() {
        return;
    }
    let ScanMetadataArrowResult {
        arrow_data,
        selection_vector,
        transforms,
    } = unsafe { *Box::from_raw(result) };
    // KernelBoolSlice is a leaked Vec<bool>; reconstitute and drop to free
    let _ = unsafe { selection_vector.into_vec() };
    // ArrowFFIData's FFI_ArrowArray/FFI_ArrowSchema have Drop impls that call
    // their release callbacks if non-null. If the consumer already imported the
    // data (calling release), the pointers are null and drop is a no-op.
    drop(arrow_data);
    // CTransforms was heap-allocated; reconstitute the Box and drop it
    drop(unsafe { Box::from_raw(transforms) });
}

#[cfg(test)]
mod scan_builder_tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use std::ffi::c_void;

    use test_utils::{actions_to_string, TestAction};

    use crate::error::KernelError;
    use crate::expressions::kernel_visitor::{
        visit_expression_column, visit_expression_literal_int, visit_predicate_lt,
        KernelExpressionVisitorState,
    };
    use crate::ffi_test_utils::{allocate_err, ok_or_panic, recover_error, setup_snapshot};
    use crate::schema_visitor::{
        visit_field_integer, visit_field_struct, KernelSchemaVisitorState,
    };
    use crate::{free_engine, free_schema, free_snapshot, kernel_string_slice, ExternResult};

    use super::{
        free_scan, free_scan_builder, scan_builder, scan_builder_build,
        scan_builder_with_predicate, scan_builder_with_schema, scan_logical_schema,
        EnginePredicate, EngineSchema,
    };

    /// Schema visitor that produces `{id: integer (nullable)}` -- a single-column projection of
    /// the standard test table schema.
    extern "C" fn visit_id_only_schema(
        _schema_ptr: *mut c_void,
        state: &mut KernelSchemaVisitorState,
    ) -> usize {
        let id = "id";
        let id_field_id = unsafe {
            ok_or_panic(visit_field_integer(
                state,
                kernel_string_slice!(id),
                true,
                allocate_err,
            ))
        };
        let field_ids = [id_field_id];
        let schema = "schema";
        unsafe {
            ok_or_panic(visit_field_struct(
                state,
                kernel_string_slice!(schema),
                field_ids.as_ptr(),
                1,
                false,
                allocate_err,
            ))
        }
    }

    /// Predicate visitor that constructs `id < 10`.
    extern "C" fn visit_id_lt_10(
        _pred_ptr: *mut c_void,
        state: &mut KernelExpressionVisitorState,
    ) -> usize {
        let id = "id";
        let col = unsafe {
            ok_or_panic(visit_expression_column(
                state,
                kernel_string_slice!(id),
                allocate_err,
            ))
        };
        let lit = visit_expression_literal_int(state, 10);
        visit_predicate_lt(state, col, lit)
    }

    #[tokio::test]
    async fn test_scan_builder_no_pushdown() {
        let (engine, snapshot) = setup_snapshot(actions_to_string(vec![TestAction::Metadata]))
            .await
            .unwrap();
        let builder = unsafe { scan_builder(snapshot.shallow_copy()) };
        let scan = unsafe { ok_or_panic(scan_builder_build(builder, engine.shallow_copy())) };
        // Full schema: both `id` and `val` columns
        let schema = unsafe { scan_logical_schema(scan.shallow_copy()) };
        let schema_ref = unsafe { schema.as_ref() };
        assert_eq!(schema_ref.fields().count(), 2);
        unsafe { free_schema(schema) };
        unsafe { free_scan(scan) };
        unsafe { free_snapshot(snapshot) };
        unsafe { free_engine(engine) };
    }

    #[tokio::test]
    async fn test_scan_builder_with_predicate() {
        let (engine, snapshot) = setup_snapshot(actions_to_string(vec![TestAction::Metadata]))
            .await
            .unwrap();
        let builder = unsafe { scan_builder(snapshot.shallow_copy()) };
        let mut predicate = EnginePredicate {
            predicate: std::ptr::null_mut(),
            visitor: visit_id_lt_10,
        };
        let builder = unsafe {
            ok_or_panic(scan_builder_with_predicate(
                builder,
                engine.shallow_copy(),
                &mut predicate,
            ))
        };
        let scan = unsafe { ok_or_panic(scan_builder_build(builder, engine.shallow_copy())) };
        // Predicate does not reduce columns -- full schema is still returned
        let schema = unsafe { scan_logical_schema(scan.shallow_copy()) };
        let schema_ref = unsafe { schema.as_ref() };
        assert_eq!(schema_ref.fields().count(), 2);
        unsafe { free_schema(schema) };
        unsafe { free_scan(scan) };
        unsafe { free_snapshot(snapshot) };
        unsafe { free_engine(engine) };
    }

    #[tokio::test]
    async fn test_scan_builder_with_schema() {
        let (engine, snapshot) = setup_snapshot(actions_to_string(vec![TestAction::Metadata]))
            .await
            .unwrap();
        let builder = unsafe { scan_builder(snapshot.shallow_copy()) };
        let schema_arg = EngineSchema {
            schema: std::ptr::null_mut(),
            visitor: visit_id_only_schema,
        };
        let builder = unsafe {
            ok_or_panic(scan_builder_with_schema(
                builder,
                engine.shallow_copy(),
                &schema_arg,
            ))
        };
        let scan = unsafe { ok_or_panic(scan_builder_build(builder, engine.shallow_copy())) };
        // Projection to `{id}` -- only one column in the logical schema
        let schema = unsafe { scan_logical_schema(scan.shallow_copy()) };
        let schema_ref = unsafe { schema.as_ref() };
        assert_eq!(schema_ref.fields().count(), 1);
        assert!(schema_ref.field("id").is_some());
        unsafe { free_schema(schema) };
        unsafe { free_scan(scan) };
        unsafe { free_snapshot(snapshot) };
        unsafe { free_engine(engine) };
    }

    #[tokio::test]
    async fn test_scan_builder_with_predicate_and_schema() {
        let (engine, snapshot) = setup_snapshot(actions_to_string(vec![TestAction::Metadata]))
            .await
            .unwrap();
        let builder = unsafe { scan_builder(snapshot.shallow_copy()) };
        let mut predicate = EnginePredicate {
            predicate: std::ptr::null_mut(),
            visitor: visit_id_lt_10,
        };
        let builder = unsafe {
            ok_or_panic(scan_builder_with_predicate(
                builder,
                engine.shallow_copy(),
                &mut predicate,
            ))
        };
        let schema_arg = EngineSchema {
            schema: std::ptr::null_mut(),
            visitor: visit_id_only_schema,
        };
        let builder = unsafe {
            ok_or_panic(scan_builder_with_schema(
                builder,
                engine.shallow_copy(),
                &schema_arg,
            ))
        };
        let scan = unsafe { ok_or_panic(scan_builder_build(builder, engine.shallow_copy())) };
        // Predicate + projection: only `id` in logical schema
        let schema = unsafe { scan_logical_schema(scan.shallow_copy()) };
        let schema_ref = unsafe { schema.as_ref() };
        assert_eq!(schema_ref.fields().count(), 1);
        assert!(schema_ref.field("id").is_some());
        unsafe { free_schema(schema) };
        unsafe { free_scan(scan) };
        unsafe { free_snapshot(snapshot) };
        unsafe { free_engine(engine) };
    }

    /// Schema visitor that returns a bare integer field ID (not wrapped in a struct root).
    /// `extract_kernel_schema` requires a struct root, so this produces an error.
    extern "C" fn visit_invalid_schema_not_struct(
        _schema_ptr: *mut c_void,
        state: &mut KernelSchemaVisitorState,
    ) -> usize {
        let bare_field = "bare_field";
        unsafe {
            ok_or_panic(visit_field_integer(
                state,
                kernel_string_slice!(bare_field),
                true,
                allocate_err,
            ))
        }
    }

    #[tokio::test]
    async fn test_scan_builder_with_schema_then_predicate() {
        // Verify that applying schema before predicate produces the same result as the reverse
        let (engine, snapshot) = setup_snapshot(actions_to_string(vec![TestAction::Metadata]))
            .await
            .unwrap();
        let builder = unsafe { scan_builder(snapshot.shallow_copy()) };
        let schema_arg = EngineSchema {
            schema: std::ptr::null_mut(),
            visitor: visit_id_only_schema,
        };
        let builder = unsafe {
            ok_or_panic(scan_builder_with_schema(
                builder,
                engine.shallow_copy(),
                &schema_arg,
            ))
        };
        let mut predicate = EnginePredicate {
            predicate: std::ptr::null_mut(),
            visitor: visit_id_lt_10,
        };
        let builder = unsafe {
            ok_or_panic(scan_builder_with_predicate(
                builder,
                engine.shallow_copy(),
                &mut predicate,
            ))
        };
        let scan = unsafe { ok_or_panic(scan_builder_build(builder, engine.shallow_copy())) };
        // Projection to `{id}` regardless of application order
        let schema = unsafe { scan_logical_schema(scan.shallow_copy()) };
        let schema_ref = unsafe { schema.as_ref() };
        assert_eq!(schema_ref.fields().count(), 1);
        assert!(schema_ref.field("id").is_some());
        unsafe { free_schema(schema) };
        unsafe { free_scan(scan) };
        unsafe { free_snapshot(snapshot) };
        unsafe { free_engine(engine) };
    }

    #[tokio::test]
    async fn test_scan_builder_with_schema_error_propagates() {
        // An invalid schema (bare primitive field, no struct root) must return ExternResult::Err
        let (engine, snapshot) = setup_snapshot(actions_to_string(vec![TestAction::Metadata]))
            .await
            .unwrap();
        let builder = unsafe { scan_builder(snapshot.shallow_copy()) };
        let schema_arg = EngineSchema {
            schema: std::ptr::null_mut(),
            visitor: visit_invalid_schema_not_struct,
        };
        let result =
            unsafe { scan_builder_with_schema(builder, engine.shallow_copy(), &schema_arg) };
        assert!(
            matches!(result, ExternResult::Err(_)),
            "expected ExternResult::Err for invalid schema"
        );
        if let ExternResult::Err(e) = result {
            let err = unsafe { recover_error(e) };
            assert_eq!(err.etype, KernelError::SchemaError);
        }
        unsafe { free_snapshot(snapshot) };
        unsafe { free_engine(engine) };
    }

    #[tokio::test]
    async fn test_free_scan_builder_without_build() {
        let (engine, snapshot) = setup_snapshot(actions_to_string(vec![TestAction::Metadata]))
            .await
            .unwrap();
        let builder = unsafe { scan_builder(snapshot.shallow_copy()) };
        // Drop without building -- must not panic or leak
        unsafe { free_scan_builder(builder) };
        unsafe { free_snapshot(snapshot) };
        unsafe { free_engine(engine) };
    }
}

#[cfg(all(test, feature = "default-engine-base"))]
mod scan_metadata_arrow_tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use delta_kernel::arrow::array::ffi::from_ffi;
    use delta_kernel::arrow::array::{RecordBatch, StructArray};
    use test_utils::{actions_to_string, TestAction};

    use crate::engine_data::ArrowFFIData;
    use crate::ffi_test_utils::{ok_or_panic, setup_snapshot};
    use crate::{free_engine, free_snapshot};

    use super::{
        free_scan, free_scan_metadata_arrow_result, free_scan_metadata_iter, get_transform_for_row,
        scan_builder, scan_builder_build, scan_metadata_iter_init, scan_metadata_next_arrow_impl,
        CTransforms, ScanMetadataArrowResult, SharedScan, SharedScanMetadataIterator,
    };

    /// Sets up engine, snapshot, scan, and scan metadata iterator from the given test actions.
    async fn setup_scan_iter(
        actions: Vec<TestAction>,
    ) -> (
        crate::handle::Handle<crate::SharedExternEngine>,
        crate::handle::Handle<crate::SharedSnapshot>,
        crate::handle::Handle<SharedScan>,
        crate::handle::Handle<SharedScanMetadataIterator>,
    ) {
        let (engine, snapshot) = setup_snapshot(actions_to_string(actions)).await.unwrap();
        let builder = unsafe { scan_builder(snapshot.shallow_copy()) };
        let scan = unsafe { ok_or_panic(scan_builder_build(builder, engine.shallow_copy())) };
        let iter = unsafe {
            ok_or_panic(scan_metadata_iter_init(
                engine.shallow_copy(),
                scan.shallow_copy(),
            ))
        };
        (engine, snapshot, scan, iter)
    }

    /// Consume a non-null `ScanMetadataArrowResult` pointer, import the Arrow FFI data back
    /// to a `RecordBatch`, and return the batch, selection vector, and transforms.
    unsafe fn import_arrow_result(
        ptr: *mut ScanMetadataArrowResult,
    ) -> (RecordBatch, Vec<bool>, Box<CTransforms>) {
        let ScanMetadataArrowResult {
            arrow_data,
            selection_vector,
            transforms,
        } = *Box::from_raw(ptr);
        let ArrowFFIData { array, schema } = arrow_data;
        let array_data = from_ffi(array, &schema).unwrap();
        let batch: RecordBatch = StructArray::from(array_data).into();
        let sv = selection_vector.into_vec();
        (batch, sv, Box::from_raw(transforms))
    }

    #[tokio::test]
    async fn returns_batch_then_null_on_exhaustion() {
        let (engine, snapshot, scan, iter) = setup_scan_iter(vec![
            TestAction::Metadata,
            TestAction::Add("file1.parquet".into()),
        ])
        .await;

        let iter_ref = unsafe { iter.as_ref() };

        // First call returns a non-null batch
        let ptr = scan_metadata_next_arrow_impl(iter_ref).unwrap();
        assert!(!ptr.is_null());
        unsafe { free_scan_metadata_arrow_result(ptr) };

        // Subsequent calls return null (exhausted)
        let ptr = scan_metadata_next_arrow_impl(iter_ref).unwrap();
        assert!(ptr.is_null());
        let ptr = scan_metadata_next_arrow_impl(iter_ref).unwrap();
        assert!(ptr.is_null());

        unsafe {
            free_scan_metadata_iter(iter);
            free_scan(scan);
            free_snapshot(snapshot);
            free_engine(engine);
        }
    }

    #[tokio::test]
    async fn arrow_data_round_trips_with_correct_schema() {
        let (engine, snapshot, scan, iter) = setup_scan_iter(vec![
            TestAction::Metadata,
            TestAction::Add("file1.parquet".into()),
        ])
        .await;

        let iter_ref = unsafe { iter.as_ref() };
        let ptr = scan_metadata_next_arrow_impl(iter_ref).unwrap();
        assert!(!ptr.is_null());

        let (batch, sv, _transforms) = unsafe { import_arrow_result(ptr) };

        // The batch includes all log entries (commitInfo, protocol, metadata, add) but only
        // the add file row is selected. Verify schema columns match SCAN_ROW_SCHEMA.
        let batch_schema = batch.schema();
        assert!(batch_schema.field_with_name("path").is_ok());
        assert!(batch_schema.field_with_name("size").is_ok());
        assert!(batch_schema.field_with_name("modificationTime").is_ok());
        assert!(batch_schema.field_with_name("stats").is_ok());
        assert!(batch_schema.field_with_name("deletionVector").is_ok());
        assert!(batch_schema.field_with_name("fileConstantValues").is_ok());

        // Selection vector length matches batch rows; exactly 1 row selected (the add file)
        assert_eq!(sv.len(), batch.num_rows());
        assert_eq!(sv.iter().filter(|&&v| v).count(), 1);

        unsafe {
            free_scan_metadata_iter(iter);
            free_scan(scan);
            free_snapshot(snapshot);
            free_engine(engine);
        }
    }

    #[tokio::test]
    async fn multiple_add_files_all_selected() {
        let (engine, snapshot, scan, iter) = setup_scan_iter(vec![
            TestAction::Metadata,
            TestAction::Add("a.parquet".into()),
            TestAction::Add("b.parquet".into()),
            TestAction::Add("c.parquet".into()),
        ])
        .await;

        let iter_ref = unsafe { iter.as_ref() };
        let mut total_selected = 0usize;

        loop {
            let ptr = scan_metadata_next_arrow_impl(iter_ref).unwrap();
            if ptr.is_null() {
                break;
            }
            let (batch, sv, _transforms) = unsafe { import_arrow_result(ptr) };
            assert_eq!(sv.len(), batch.num_rows());
            total_selected += sv.iter().filter(|&&v| v).count();
        }

        assert_eq!(total_selected, 3, "3 add files -> 3 selected rows");

        unsafe {
            free_scan_metadata_iter(iter);
            free_scan(scan);
            free_snapshot(snapshot);
            free_engine(engine);
        }
    }

    #[tokio::test]
    async fn empty_table_returns_null_immediately() {
        let (engine, snapshot, scan, iter) = setup_scan_iter(vec![TestAction::Metadata]).await;

        let iter_ref = unsafe { iter.as_ref() };
        let ptr = scan_metadata_next_arrow_impl(iter_ref).unwrap();
        assert!(ptr.is_null(), "table with no data files -> immediate null");

        unsafe {
            free_scan_metadata_iter(iter);
            free_scan(scan);
            free_snapshot(snapshot);
            free_engine(engine);
        }
    }

    #[tokio::test]
    async fn simple_table_has_no_transforms() {
        let (engine, snapshot, scan, iter) = setup_scan_iter(vec![
            TestAction::Metadata,
            TestAction::Add("file1.parquet".into()),
        ])
        .await;

        let iter_ref = unsafe { iter.as_ref() };
        let ptr = scan_metadata_next_arrow_impl(iter_ref).unwrap();
        assert!(!ptr.is_null());

        let (_batch, _sv, transforms) = unsafe { import_arrow_result(ptr) };

        // Simple table (no partitions, no column mapping) has an empty transforms vec.
        // get_transform_for_row returns None for any row index.
        assert!(transforms.transforms.is_empty());
        let result = unsafe { get_transform_for_row(0, &transforms) };
        assert!(matches!(result, crate::OptionalValue::None));

        unsafe {
            free_scan_metadata_iter(iter);
            free_scan(scan);
            free_snapshot(snapshot);
            free_engine(engine);
        }
    }

    #[tokio::test]
    async fn partitioned_table_has_transforms() {
        let (engine, snapshot) = setup_snapshot(test_utils::actions_to_string_partitioned(vec![
            TestAction::Metadata,
            TestAction::Add("val=a/file1.parquet".into()),
        ]))
        .await
        .unwrap();

        let builder = unsafe { scan_builder(snapshot.shallow_copy()) };
        let scan = unsafe { ok_or_panic(scan_builder_build(builder, engine.shallow_copy())) };
        let iter = unsafe {
            ok_or_panic(scan_metadata_iter_init(
                engine.shallow_copy(),
                scan.shallow_copy(),
            ))
        };

        let iter_ref = unsafe { iter.as_ref() };
        let ptr = scan_metadata_next_arrow_impl(iter_ref).unwrap();
        assert!(!ptr.is_null());

        let (batch, sv, transforms) = unsafe { import_arrow_result(ptr) };

        // Partitioned table produces per-row transforms. The transforms vec has one
        // entry per batch row. Selected (add-file) rows should have Some transform.
        assert_eq!(transforms.transforms.len(), batch.num_rows());
        for (i, &selected) in sv.iter().enumerate() {
            let result = unsafe { get_transform_for_row(i, &transforms) };
            if selected {
                // get_transform_for_row clones the Arc into a Handle; must free it
                match result {
                    crate::OptionalValue::Some(handle) => unsafe { handle.drop_handle() },
                    crate::OptionalValue::None => {
                        panic!("selected row {i} should have a transform for partitioned table")
                    }
                }
            }
        }

        unsafe {
            free_scan_metadata_iter(iter);
            free_scan(scan);
            free_snapshot(snapshot);
            free_engine(engine);
        }
    }

    #[test]
    fn free_null_is_safe() {
        unsafe { free_scan_metadata_arrow_result(std::ptr::null_mut()) };
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, ptr::NonNull};

    use crate::{KernelStringSlice, NullableCvoid, TryFromStringSlice};

    extern "C" fn visit_entry(
        engine_context: NullableCvoid,
        key: KernelStringSlice,
        value: KernelStringSlice,
    ) {
        let map_ptr: *mut HashMap<String, String> = engine_context.unwrap().as_ptr().cast();
        let key = unsafe { String::try_from_slice(&key).unwrap() };
        let value = unsafe { String::try_from_slice(&value).unwrap() };
        unsafe {
            (*map_ptr).insert(key, value);
        }
    }

    #[test]
    fn visit_string_map() {
        let test_map: HashMap<String, String> = HashMap::from([
            ("A".into(), "B".into()),
            ("C".into(), "D".into()),
            ("E".into(), "F".into()),
            ("G".into(), "H".into()),
        ]);
        let cmap: super::CStringMap = test_map.clone().into();
        let context_map: Box<HashMap<String, String>> = Box::default();
        let map_ptr: *mut HashMap<String, String> = Box::into_raw(context_map);
        unsafe {
            let ptr = NonNull::new_unchecked(map_ptr.cast());
            super::visit_string_map(&cmap, Some(ptr), visit_entry);
        }
        let final_map: HashMap<String, String> = *unsafe { Box::from_raw(map_ptr) };
        assert_eq!(test_map, final_map);
    }
}
