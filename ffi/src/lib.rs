//! FFI interface for the delta kernel
//!
//! Exposes that an engine needs to call from C/C++ to interface with kernel

#[cfg(feature = "default-engine-base")]
use std::collections::HashMap;
use std::default::Default;
use std::os::raw::{c_char, c_void};
use std::ptr::NonNull;
use std::sync::Arc;
use tracing::debug;
use url::Url;

use delta_kernel::schema::Schema;
use delta_kernel::snapshot::Snapshot;
use delta_kernel::Version;
use delta_kernel::{DeltaResult, Engine, EngineData};
use delta_kernel_ffi_macros::handle_descriptor;

// cbindgen doesn't understand our use of feature flags here, and by default it parses `mod handle`
// twice. So we tell it to ignore one of the declarations to avoid double-definition errors.
/// cbindgen:ignore
#[cfg(feature = "internal-api")]
pub mod handle;
#[cfg(not(feature = "internal-api"))]
pub(crate) mod handle;

use handle::Handle;

// The handle_descriptor macro needs this, because it needs to emit fully qualified type names. THe
// actual prod code could use `crate::`, but doc tests can't because they're not "inside" the crate.
// relies on `crate::`
extern crate self as delta_kernel_ffi;

mod domain_metadata;
pub use domain_metadata::get_domain_metadata;
pub mod engine_data;
pub mod engine_funcs;
pub mod error;
#[cfg(feature = "default-engine-base")]
pub mod table_changes;
use error::{AllocateError, AllocateErrorFn, ExternResult, IntoExternResult};
pub mod expressions;
#[cfg(feature = "tracing")]
pub mod ffi_tracing;
pub mod scan;
pub mod schema;

#[cfg(test)]
mod ffi_test_utils;
#[cfg(feature = "test-ffi")]
pub mod test_ffi;
pub mod transaction;

pub(crate) type NullableCvoid = Option<NonNull<c_void>>;

/// Model iterators. This allows an engine to specify iteration however it likes, and we simply wrap
/// the engine functions. The engine retains ownership of the iterator.
#[repr(C)]
pub struct EngineIterator {
    /// Opaque data that will be iterated over. This data will be passed to the get_next function
    /// each time a next item is requested from the iterator
    data: NonNull<c_void>,
    /// A function that should advance the iterator and return the next time from the data
    /// If the iterator is complete, it should return null. It should be safe to
    /// call `get_next()` multiple times if it returns null.
    get_next: extern "C" fn(data: NonNull<c_void>) -> *const c_void,
}

impl Iterator for EngineIterator {
    // Todo: Figure out item type
    type Item = *const c_void;

    fn next(&mut self) -> Option<Self::Item> {
        let next_item = (self.get_next)(self.data);
        if next_item.is_null() {
            None
        } else {
            Some(next_item)
        }
    }
}

/// A non-owned slice of a UTF8 string, intended for arg-passing between kernel and engine. The
/// slice is only valid until the function it was passed into returns, and should not be copied.
///
/// # Safety
///
/// Intentionally not Copy, Clone, Send, nor Sync.
///
/// Whoever instantiates the struct must ensure it does not outlive the data it points to. The
/// compiler cannot help us here, because raw pointers don't have lifetimes. A good rule of thumb is
/// to always use the `kernel_string_slice` macro to create string slices, and to avoid returning
/// a string slice from a code block or function (since the move risks over-extending its lifetime):
///
/// ```ignore
/// # // Ignored because this code is pub(crate) and doc tests cannot compile it
/// let dangling_slice = {
///     let tmp = String::from("tmp");
///     kernel_string_slice!(tmp)
/// }
/// ```
///
/// Meanwhile, the callee must assume that the slice is only valid until the function returns, and
/// must not retain any references to the slice or its data that might outlive the function call.
#[repr(C)]
pub struct KernelStringSlice {
    ptr: *const c_char,
    len: usize,
}
impl KernelStringSlice {
    /// Creates a new string slice from a source string. This method is dangerous and can easily
    /// lead to use-after-free scenarios. The [`kernel_string_slice`] macro should be preferred as a
    /// much safer alternative.
    ///
    /// # Safety
    ///
    /// Caller affirms that the source will outlive the statement that creates this slice. The
    /// compiler cannot help as raw pointers do not have lifetimes that the compiler can
    /// verify. Thus, e.g., the following incorrect code would compile and leave `s` dangling,
    /// because the unnamed string arg is dropped as soon as the statement finishes executing.
    ///
    /// ```ignore
    /// # // Ignored because this code is pub(crate) and doc tests cannot compile it
    /// let s = KernelStringSlice::new_unsafe(String::from("bad").as_str());
    /// ```
    pub(crate) unsafe fn new_unsafe(source: &str) -> Self {
        let source = source.as_bytes();
        Self {
            ptr: source.as_ptr().cast(),
            len: source.len(),
        }
    }
}

/// FFI-safe implementation for Rust's `Option<T>`
#[derive(PartialEq, Debug)]
#[repr(C)]
pub enum OptionalValue<T> {
    Some(T),
    None,
}

impl<T> From<Option<T>> for OptionalValue<T> {
    fn from(item: Option<T>) -> Self {
        match item {
            Some(value) => OptionalValue::Some(value),
            None => OptionalValue::None,
        }
    }
}

/// Creates a new [`KernelStringSlice`] from a string reference (which must be an identifier, to
/// ensure it is not immediately dropped). This is the safest way to create a kernel string slice.
///
/// NOTE: It is still possible to misuse the resulting kernel string slice in unsafe ways, such as
/// returning it from the function or code block that owns the string reference.
macro_rules! kernel_string_slice {
    ( $source:ident ) => {{
        // Safety: A named source cannot immediately go out of scope, so the resulting slice must
        // remain valid at least that long. Any dangerous situations will arise from the subsequent
        // misuse of this string slice, not from its creation.
        //
        // NOTE: The `do_it` wrapper avoids an "unnecessary `unsafe` block" clippy warning in case
        // the invocation site of this macro is already in an `unsafe` block. We can't just disable
        // the warning with #[allow(unused_unsafe)] because expression annotation is unstable rust.
        fn do_it(s: &str) -> $crate::KernelStringSlice {
            unsafe { $crate::KernelStringSlice::new_unsafe(s) }
        }
        do_it(&$source)
    }};
}
pub(crate) use kernel_string_slice;

trait TryFromStringSlice<'a>: Sized {
    unsafe fn try_from_slice(slice: &'a KernelStringSlice) -> DeltaResult<Self>;
}

impl<'a> TryFromStringSlice<'a> for String {
    /// Converts a kernel string slice into a `String`.
    ///
    /// # Safety
    ///
    /// The slice must be a valid (non-null) pointer, and must point to the indicated number of
    /// valid utf8 bytes.
    unsafe fn try_from_slice(slice: &'a KernelStringSlice) -> DeltaResult<Self> {
        let slice: &str = unsafe { TryFromStringSlice::try_from_slice(slice) }?;
        Ok(slice.into())
    }
}

impl<'a> TryFromStringSlice<'a> for &'a str {
    /// Converts a kernel string slice into a borrowed `str`. The result does not outlive the kernel
    /// string slice it came from.
    ///
    /// # Safety
    ///
    /// The slice must be a valid (non-null) pointer, and must point to the indicated number of
    /// valid utf8 bytes.
    unsafe fn try_from_slice(slice: &'a KernelStringSlice) -> DeltaResult<Self> {
        let slice = unsafe { std::slice::from_raw_parts(slice.ptr.cast(), slice.len) };
        Ok(std::str::from_utf8(slice)?)
    }
}

/// Allow engines to allocate strings of their own type. the contract of calling a passed allocate
/// function is that `kernel_str` is _only_ valid until the return from this function
pub type AllocateStringFn = extern "C" fn(kernel_str: KernelStringSlice) -> NullableCvoid;

// Put KernelBoolSlice in a sub-module, with non-public members, so rust code cannot instantiate it
// directly. It can only be created by converting `From<Vec<bool>>`.
mod private {
    use std::ptr::NonNull;

    /// Represents an owned slice of boolean values allocated by the kernel. Any time the engine
    /// receives a `KernelBoolSlice` as a return value from a kernel method, engine is responsible
    /// to free that slice, by calling [super::free_bool_slice] exactly once.
    #[repr(C)]
    pub struct KernelBoolSlice {
        ptr: NonNull<bool>,
        len: usize,
    }

    /// An owned slice of u64 row indexes allocated by the kernel. The engine is responsible for
    /// freeing this slice by calling [super::free_row_indexes] once.
    #[repr(C)]
    pub struct KernelRowIndexArray {
        ptr: NonNull<u64>,
        len: usize,
    }

    impl KernelBoolSlice {
        /// Creates an empty slice.
        pub fn empty() -> KernelBoolSlice {
            KernelBoolSlice {
                ptr: NonNull::dangling(),
                len: 0,
            }
        }

        /// Converts this slice back into a `Vec<bool>`.
        ///
        /// # Safety
        ///
        /// The slice must have been originally created `From<Vec<bool>>`, and must not have been
        /// already been consumed by a previous call to this method.
        pub unsafe fn as_ref(&self) -> &[bool] {
            if self.len == 0 {
                Default::default()
            } else {
                unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
            }
        }

        /// Converts this slice back into a `Vec<bool>`.
        ///
        /// # Safety
        ///
        /// The slice must have been originally created `From<Vec<bool>>`, and must not have been
        /// already been consumed by a previous call to this method.
        pub unsafe fn into_vec(self) -> Vec<bool> {
            if self.len == 0 {
                Default::default()
            } else {
                Vec::from_raw_parts(self.ptr.as_ptr(), self.len, self.len)
            }
        }
    }

    impl From<Vec<bool>> for KernelBoolSlice {
        fn from(val: Vec<bool>) -> Self {
            let len = val.len();
            let boxed = val.into_boxed_slice();
            let leaked_ptr = Box::leak(boxed).as_mut_ptr();
            let ptr = NonNull::new(leaked_ptr)
                .expect("This should never be non-null please report this bug.");
            KernelBoolSlice { ptr, len }
        }
    }

    /// # Safety
    ///
    /// Whenever kernel passes a [KernelBoolSlice] to engine, engine assumes ownership of the slice
    /// memory, but must only free it by calling [super::free_bool_slice]. Since the global
    /// allocator is threadsafe, it doesn't matter which engine thread invokes that method.
    unsafe impl Send for KernelBoolSlice {}
    /// # Safety
    ///
    /// This follows the same contract as KernelBoolSlice above, engine assumes ownership of the
    /// slice memory, but must only free it by calling [super::free_row_indexes]. It does not matter
    /// from which thread the engine invoke that method
    unsafe impl Send for KernelRowIndexArray {}

    /// # Safety
    ///
    /// If engine chooses to leverage concurrency, engine is responsible to prevent data races.
    unsafe impl Sync for KernelBoolSlice {}
    /// # Safety
    ///
    /// If engine chooses to leverage concurrency, engine is responsible to prevent data races.
    /// Same contract as KernelBoolSlice above
    unsafe impl Sync for KernelRowIndexArray {}

    impl KernelRowIndexArray {
        /// Converts this slice back into a `Vec<u64>`.
        ///
        /// # Safety
        ///
        /// The slice must have been originally created `From<Vec<u64>>`, and must not have
        /// already been consumed by a previous call to this method.
        pub unsafe fn into_vec(self) -> Vec<u64> {
            Vec::from_raw_parts(self.ptr.as_ptr(), self.len, self.len)
        }

        /// Creates an empty slice.
        pub fn empty() -> KernelRowIndexArray {
            Self {
                ptr: NonNull::dangling(),
                len: 0,
            }
        }
    }

    impl From<Vec<u64>> for KernelRowIndexArray {
        fn from(vec: Vec<u64>) -> Self {
            let len = vec.len();
            let boxed = vec.into_boxed_slice();
            let leaked_ptr = Box::leak(boxed).as_mut_ptr();
            let ptr = NonNull::new(leaked_ptr)
                .expect("This should never be non-null please report this bug.");
            KernelRowIndexArray { ptr, len }
        }
    }
}
pub use private::KernelBoolSlice;
pub use private::KernelRowIndexArray;

/// # Safety
///
/// Caller is responsible for passing a valid handle.
#[no_mangle]
pub unsafe extern "C" fn free_bool_slice(slice: KernelBoolSlice) {
    let vec = unsafe { slice.into_vec() };
    debug!("Dropping bool slice. It is {vec:#?}");
}

/// # Safety
///
/// Caller is responsible for passing a valid handle.
#[no_mangle]
pub unsafe extern "C" fn free_row_indexes(slice: KernelRowIndexArray) {
    let _ = slice.into_vec();
}

// TODO: Do we want this handle at all? Perhaps we should just _always_ pass raw *mut c_void pointers
// that are the engine data? Even if we want the type, should it be a shared handle instead?
/// an opaque struct that encapsulates data read by an engine. this handle can be passed back into
/// some kernel calls to operate on the data, or can be converted into the raw data as read by the
/// [`delta_kernel::Engine`] by calling [`get_raw_engine_data`]
///
/// [`get_raw_engine_data`]: crate::engine_data::get_raw_engine_data
#[handle_descriptor(target=dyn EngineData, mutable=true)]
pub struct ExclusiveEngineData;

/// Drop an `ExclusiveEngineData`.
///
/// # Safety
///
/// Caller is responsible for passing a valid handle as engine_data
#[no_mangle]
pub unsafe extern "C" fn free_engine_data(engine_data: Handle<ExclusiveEngineData>) {
    engine_data.drop_handle();
}

// A wrapper for Engine which defines additional FFI-specific methods.
pub trait ExternEngine: Send + Sync {
    fn engine(&self) -> Arc<dyn Engine>;
    fn error_allocator(&self) -> &dyn AllocateError;
}

#[handle_descriptor(target=dyn ExternEngine, mutable=false)]
pub struct SharedExternEngine;

#[cfg(feature = "default-engine-base")]
struct ExternEngineVtable {
    // Actual engine instance to use
    engine: Arc<dyn Engine>,
    allocate_error: AllocateErrorFn,
}

#[cfg(feature = "default-engine-base")]
impl Drop for ExternEngineVtable {
    fn drop(&mut self) {
        debug!("dropping engine interface");
    }
}

/// # Safety
///
/// Kernel doesn't use any threading or concurrency. If engine chooses to do so, engine is
/// responsible for handling  any races that could result.
#[cfg(feature = "default-engine-base")]
unsafe impl Send for ExternEngineVtable {}

/// # Safety
///
/// Kernel doesn't use any threading or concurrency. If engine chooses to do so, engine is
/// responsible for handling any races that could result.
///
/// These are needed because anything wrapped in Arc "should" implement it
/// Basically, by failing to implement these traits, we forbid the engine from being able to declare
/// its thread-safety (because rust assumes it is not threadsafe). By implementing them, we leave it
/// up to the engine to enforce thread safety if engine chooses to use threads at all.
#[cfg(feature = "default-engine-base")]
unsafe impl Sync for ExternEngineVtable {}

#[cfg(feature = "default-engine-base")]
impl ExternEngine for ExternEngineVtable {
    fn engine(&self) -> Arc<dyn Engine> {
        self.engine.clone()
    }
    fn error_allocator(&self) -> &dyn AllocateError {
        &self.allocate_error
    }
}

/// # Safety
///
/// Caller is responsible for passing a valid path pointer.
unsafe fn unwrap_and_parse_path_as_url(path: KernelStringSlice) -> DeltaResult<Url> {
    let path: &str = unsafe { TryFromStringSlice::try_from_slice(&path) }?;
    delta_kernel::try_parse_uri(path)
}

/// A builder that allows setting options on the `Engine` before actually building it
#[cfg(feature = "default-engine-base")]
pub struct EngineBuilder {
    url: Url,
    allocate_fn: AllocateErrorFn,
    options: HashMap<String, String>,
}

#[cfg(feature = "default-engine-base")]
impl EngineBuilder {
    fn set_option(&mut self, key: String, val: String) {
        self.options.insert(key, val);
    }
}

/// Get a "builder" that can be used to construct an engine. The function
/// [`set_builder_option`] can be used to set options on the builder prior to constructing the
/// actual engine
///
/// # Safety
/// Caller is responsible for passing a valid path pointer.
#[cfg(feature = "default-engine-base")]
#[no_mangle]
pub unsafe extern "C" fn get_engine_builder(
    path: KernelStringSlice,
    allocate_error: AllocateErrorFn,
) -> ExternResult<*mut EngineBuilder> {
    let url = unsafe { unwrap_and_parse_path_as_url(path) };
    get_engine_builder_impl(url, allocate_error).into_extern_result(&allocate_error)
}

#[cfg(feature = "default-engine-base")]
fn get_engine_builder_impl(
    url: DeltaResult<Url>,
    allocate_fn: AllocateErrorFn,
) -> DeltaResult<*mut EngineBuilder> {
    let builder = Box::new(EngineBuilder {
        url: url?,
        allocate_fn,
        options: HashMap::default(),
    });
    Ok(Box::into_raw(builder))
}

/// Set an option on the builder
///
/// # Safety
///
/// Caller must pass a valid EngineBuilder pointer, and valid slices for key and value
#[cfg(feature = "default-engine-base")]
#[no_mangle]
pub unsafe extern "C" fn set_builder_option(
    builder: &mut EngineBuilder,
    key: KernelStringSlice,
    value: KernelStringSlice,
) {
    let key = unsafe { String::try_from_slice(&key) };
    let value = unsafe { String::try_from_slice(&value) };
    // TODO: Return ExternalError if key or value is invalid? (builder has an error allocator)
    builder.set_option(key.unwrap(), value.unwrap());
}

/// Consume the builder and return a `default` engine. After calling, the passed pointer is _no
/// longer valid_. Note that this _consumes_ and frees the builder, so there is no need to
/// drop/free it afterwards.
///
///
/// # Safety
///
/// Caller is responsible to pass a valid EngineBuilder pointer, and to not use it again afterwards
#[cfg(feature = "default-engine-base")]
#[no_mangle]
pub unsafe extern "C" fn builder_build(
    builder: *mut EngineBuilder,
) -> ExternResult<Handle<SharedExternEngine>> {
    let builder_box = unsafe { Box::from_raw(builder) };
    get_default_engine_impl(
        builder_box.url,
        builder_box.options,
        builder_box.allocate_fn,
    )
    .into_extern_result(&builder_box.allocate_fn)
}

/// # Safety
///
/// Caller is responsible for passing a valid path pointer.
#[cfg(feature = "default-engine-base")]
#[no_mangle]
pub unsafe extern "C" fn get_default_engine(
    path: KernelStringSlice,
    allocate_error: AllocateErrorFn,
) -> ExternResult<Handle<SharedExternEngine>> {
    let url = unsafe { unwrap_and_parse_path_as_url(path) };
    get_default_default_engine_impl(url, allocate_error).into_extern_result(&allocate_error)
}

// get the default version of the default engine :)
#[cfg(feature = "default-engine-base")]
fn get_default_default_engine_impl(
    url: DeltaResult<Url>,
    allocate_error: AllocateErrorFn,
) -> DeltaResult<Handle<SharedExternEngine>> {
    get_default_engine_impl(url?, Default::default(), allocate_error)
}

/// Safety
///
/// Caller must free this handle to prevent memory leaks
#[cfg(feature = "default-engine-base")]
fn engine_to_handle(
    engine: Arc<dyn Engine>,
    allocate_error: AllocateErrorFn,
) -> Handle<SharedExternEngine> {
    let engine: Arc<dyn ExternEngine> = Arc::new(ExternEngineVtable {
        engine,
        allocate_error,
    });
    engine.into()
}

#[cfg(feature = "default-engine-base")]
fn get_default_engine_impl(
    url: Url,
    options: HashMap<String, String>,
    allocate_error: AllocateErrorFn,
) -> DeltaResult<Handle<SharedExternEngine>> {
    use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
    use delta_kernel::engine::default::DefaultEngine;
    let engine = DefaultEngine::<TokioBackgroundExecutor>::try_new(
        &url,
        options,
        Arc::new(TokioBackgroundExecutor::new()),
    );
    Ok(engine_to_handle(Arc::new(engine?), allocate_error))
}

/// # Safety
///
/// Caller is responsible for passing a valid handle.
#[no_mangle]
pub unsafe extern "C" fn free_engine(engine: Handle<SharedExternEngine>) {
    debug!("engine released engine");
    engine.drop_handle();
}

#[handle_descriptor(target=Schema, mutable=false, sized=true)]
pub struct SharedSchema;

#[handle_descriptor(target=Snapshot, mutable=false, sized=true)]
pub struct SharedSnapshot;

/// Get the latest snapshot from the specified table
///
/// # Safety
///
/// Caller is responsible for passing valid handles and path pointer.
#[no_mangle]
pub unsafe extern "C" fn snapshot(
    path: KernelStringSlice,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<SharedSnapshot>> {
    let url = unsafe { unwrap_and_parse_path_as_url(path) };
    let engine = unsafe { engine.as_ref() };
    snapshot_impl(url, engine, None).into_extern_result(&engine)
}

/// Get the snapshot from the specified table at a specific version
///
/// # Safety
///
/// Caller is responsible for passing valid handles and path pointer.
#[no_mangle]
pub unsafe extern "C" fn snapshot_at_version(
    path: KernelStringSlice,
    engine: Handle<SharedExternEngine>,
    version: Version,
) -> ExternResult<Handle<SharedSnapshot>> {
    let url = unsafe { unwrap_and_parse_path_as_url(path) };
    let engine = unsafe { engine.as_ref() };
    snapshot_impl(url, engine, version.into()).into_extern_result(&engine)
}

fn snapshot_impl(
    url: DeltaResult<Url>,
    extern_engine: &dyn ExternEngine,
    version: Option<Version>,
) -> DeltaResult<Handle<SharedSnapshot>> {
    let builder = Snapshot::builder_for(url?);
    let builder = if let Some(v) = version {
        // TODO: should we include a `with_version_opt` method for the builder?
        builder.at_version(v)
    } else {
        builder
    };
    let snapshot = builder.build(extern_engine.engine().as_ref())?;
    Ok(snapshot.into())
}

/// # Safety
///
/// Caller is responsible for passing a valid handle.
#[no_mangle]
pub unsafe extern "C" fn free_snapshot(snapshot: Handle<SharedSnapshot>) {
    debug!("engine released snapshot");
    snapshot.drop_handle();
}

/// Get the version of the specified snapshot
///
/// # Safety
///
/// Caller is responsible for passing a valid handle.
#[no_mangle]
pub unsafe extern "C" fn version(snapshot: Handle<SharedSnapshot>) -> u64 {
    let snapshot = unsafe { snapshot.as_ref() };
    snapshot.version()
}

/// Get the logical schema of the specified snapshot
///
/// # Safety
///
/// Caller is responsible for passing a valid snapshot handle.
#[no_mangle]
pub unsafe extern "C" fn logical_schema(snapshot: Handle<SharedSnapshot>) -> Handle<SharedSchema> {
    let snapshot = unsafe { snapshot.as_ref() };
    snapshot.schema().into()
}

/// Free a schema
///
/// # Safety
/// Engine is responsible for providing a valid schema handle.
#[no_mangle]
pub unsafe extern "C" fn free_schema(schema: Handle<SharedSchema>) {
    schema.drop_handle();
}

/// Get the resolved root of the table. This should be used in any future calls that require
/// constructing a path
///
/// # Safety
///
/// Caller is responsible for passing a valid snapshot handle.
#[no_mangle]
pub unsafe extern "C" fn snapshot_table_root(
    snapshot: Handle<SharedSnapshot>,
    allocate_fn: AllocateStringFn,
) -> NullableCvoid {
    let snapshot = unsafe { snapshot.as_ref() };
    let table_root = snapshot.table_root().to_string();
    allocate_fn(kernel_string_slice!(table_root))
}

/// Get a count of the number of partition columns for this snapshot
///
/// # Safety
/// Caller is responsible for passing a valid snapshot handle
#[no_mangle]
pub unsafe extern "C" fn get_partition_column_count(snapshot: Handle<SharedSnapshot>) -> usize {
    let snapshot = unsafe { snapshot.as_ref() };
    snapshot
        .table_configuration()
        .metadata()
        .partition_columns()
        .len()
}

/// Get an iterator of the list of partition columns for this snapshot.
///
/// # Safety
/// Caller is responsible for passing a valid snapshot handle.
#[no_mangle]
pub unsafe extern "C" fn get_partition_columns(
    snapshot: Handle<SharedSnapshot>,
) -> Handle<StringSliceIterator> {
    let snapshot = unsafe { snapshot.as_ref() };
    let iter: Box<StringIter> = Box::new(
        snapshot
            .table_configuration()
            .metadata()
            .partition_columns()
            .clone()
            .into_iter(),
    );
    iter.into()
}

type StringIter = dyn Iterator<Item = String> + Send;

#[handle_descriptor(target=StringIter, mutable=true, sized=false)]
pub struct StringSliceIterator;

/// # Safety
///
/// The iterator must be valid (returned by [`scan_metadata_iter_init`]) and not yet freed by
/// [`free_scan_metadata_iter`]. The visitor function pointer must be non-null.
///
/// [`scan_metadata_iter_init`]: crate::scan::scan_metadata_iter_init
/// [`free_scan_metadata_iter`]: crate::scan::free_scan_metadata_iter
#[no_mangle]
pub unsafe extern "C" fn string_slice_next(
    data: Handle<StringSliceIterator>,
    engine_context: NullableCvoid,
    engine_visitor: extern "C" fn(engine_context: NullableCvoid, slice: KernelStringSlice),
) -> bool {
    string_slice_next_impl(data, engine_context, engine_visitor)
}

fn string_slice_next_impl(
    mut data: Handle<StringSliceIterator>,
    engine_context: NullableCvoid,
    engine_visitor: extern "C" fn(engine_context: NullableCvoid, slice: KernelStringSlice),
) -> bool {
    let data = unsafe { data.as_mut() };
    if let Some(data) = data.next() {
        (engine_visitor)(engine_context, kernel_string_slice!(data));
        true
    } else {
        false
    }
}

/// # Safety
///
/// Caller is responsible for (at most once) passing a valid pointer to a [`StringSliceIterator`]
#[no_mangle]
pub unsafe extern "C" fn free_string_slice_data(data: Handle<StringSliceIterator>) {
    data.drop_handle();
}

/// A set that can identify its contents by address
pub struct ReferenceSet<T> {
    map: std::collections::HashMap<usize, T>,
    next_id: usize,
}

impl<T> ReferenceSet<T> {
    /// Creates a new empty set.
    pub fn new() -> Self {
        Default::default()
    }

    /// Inserts a new value into the set, returning an identifier for the value that can be used
    /// later to take() from the set.
    pub fn insert(&mut self, value: T) -> usize {
        let id = self.next_id;
        self.next_id += 1;
        self.map.insert(id, value);
        id
    }

    /// Attempts to remove a value from the set, if present.
    pub fn take(&mut self, i: usize) -> Option<T> {
        self.map.remove(&i)
    }

    /// True if the set contains an object whose address matches the pointer.
    pub fn contains(&self, id: usize) -> bool {
        self.map.contains_key(&id)
    }

    /// The current size of the set.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

impl<T> Default for ReferenceSet<T> {
    fn default() -> Self {
        Self {
            map: Default::default(),
            // NOTE: 0 is interpreted as None
            next_id: 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{EngineError, KernelError};
    use crate::ffi_test_utils::{
        allocate_err, allocate_str, assert_extern_result_error_with_message, ok_or_panic,
        recover_string,
    };
    use delta_kernel::engine::default::{executor::tokio::TokioBackgroundExecutor, DefaultEngine};
    use object_store::memory::InMemory;
    use test_utils::{actions_to_string, actions_to_string_partitioned, add_commit, TestAction};

    #[no_mangle]
    extern "C" fn allocate_null_err(_: KernelError, _: KernelStringSlice) -> *mut EngineError {
        std::ptr::null_mut()
    }

    #[test]
    fn string_slice() {
        let s = "foo";
        let _ = kernel_string_slice!(s);
    }

    #[test]
    fn bool_slice() {
        let bools = vec![true, false, true];
        let bool_slice = KernelBoolSlice::from(bools);
        unsafe {
            free_bool_slice(bool_slice);
        }
    }

    pub(crate) fn get_default_engine(path: &str) -> Handle<SharedExternEngine> {
        let path = kernel_string_slice!(path);
        let builder = unsafe { ok_or_panic(get_engine_builder(path, allocate_err)) };
        unsafe { ok_or_panic(builder_build(builder)) }
    }

    #[test]
    fn engine_builder() {
        let engine = get_default_engine("memory:///doesntmatter/foo");
        unsafe {
            free_engine(engine);
        }
    }

    #[tokio::test]
    async fn test_snapshot() -> Result<(), Box<dyn std::error::Error>> {
        let storage = Arc::new(InMemory::new());
        add_commit(
            storage.as_ref(),
            0,
            actions_to_string(vec![TestAction::Metadata]),
        )
        .await?;
        let engine = DefaultEngine::new(storage.clone(), Arc::new(TokioBackgroundExecutor::new()));
        let engine = engine_to_handle(Arc::new(engine), allocate_err);
        let path = "memory:///";

        // Test getting latest snapshot
        let snapshot1 =
            unsafe { ok_or_panic(snapshot(kernel_string_slice!(path), engine.shallow_copy())) };
        let version1 = unsafe { version(snapshot1.shallow_copy()) };
        assert_eq!(version1, 0);

        // Test getting snapshot at version
        let snapshot2 = unsafe {
            ok_or_panic(snapshot_at_version(
                kernel_string_slice!(path),
                engine.shallow_copy(),
                0,
            ))
        };
        let version2 = unsafe { version(snapshot2.shallow_copy()) };
        assert_eq!(version2, 0);

        // Test getting non-existent snapshot
        let snapshot_at_non_existent_version =
            unsafe { snapshot_at_version(kernel_string_slice!(path), engine.shallow_copy(), 1) };
        assert_extern_result_error_with_message(snapshot_at_non_existent_version, KernelError::GenericError, "Generic delta kernel error: LogSegment end version 0 not the same as the specified end version 1");

        let table_root = unsafe { snapshot_table_root(snapshot1.shallow_copy(), allocate_str) };
        assert!(table_root.is_some());
        let s = recover_string(table_root.unwrap());
        assert_eq!(&s, path);

        unsafe { free_snapshot(snapshot1) }
        unsafe { free_snapshot(snapshot2) }
        unsafe { free_engine(engine) }
        Ok(())
    }

    #[tokio::test]
    async fn test_snapshot_partition_cols() -> Result<(), Box<dyn std::error::Error>> {
        let storage = Arc::new(InMemory::new());
        add_commit(
            storage.as_ref(),
            0,
            actions_to_string_partitioned(vec![TestAction::Metadata]),
        )
        .await?;
        let engine = DefaultEngine::new(storage.clone(), Arc::new(TokioBackgroundExecutor::new()));
        let engine = engine_to_handle(Arc::new(engine), allocate_err);
        let path = "memory:///";

        let snapshot =
            unsafe { ok_or_panic(snapshot(kernel_string_slice!(path), engine.shallow_copy())) };

        let partition_count = unsafe { get_partition_column_count(snapshot.shallow_copy()) };
        assert_eq!(partition_count, 1, "Should have one partition");

        let partition_iter = unsafe { get_partition_columns(snapshot.shallow_copy()) };

        #[no_mangle]
        extern "C" fn visit_partition(_context: NullableCvoid, slice: KernelStringSlice) {
            let s = unsafe { String::try_from_slice(&slice) }.unwrap();
            assert_eq!(s.as_str(), "val", "Partition col should be 'val'");
        }
        while unsafe { string_slice_next(partition_iter.shallow_copy(), None, visit_partition) } {
            // validate happens inside visit_partition
        }

        unsafe { free_string_slice_data(partition_iter) }
        unsafe { free_snapshot(snapshot) }
        unsafe { free_engine(engine) }
        Ok(())
    }

    #[tokio::test]
    async fn allocate_null_err_okay() -> Result<(), Box<dyn std::error::Error>> {
        let storage = Arc::new(InMemory::new());
        add_commit(
            storage.as_ref(),
            0,
            actions_to_string(vec![TestAction::Metadata]),
        )
        .await?;
        let engine = DefaultEngine::new(storage.clone(), Arc::new(TokioBackgroundExecutor::new()));
        let engine = engine_to_handle(Arc::new(engine), allocate_null_err);
        let path = "memory:///";

        // Get a non-existent snapshot, this will call allocate_null_err
        let snapshot_at_non_existent_version =
            unsafe { snapshot_at_version(kernel_string_slice!(path), engine.shallow_copy(), 1) };
        assert!(snapshot_at_non_existent_version.is_err());

        unsafe { free_engine(engine) }
        Ok(())
    }
}
