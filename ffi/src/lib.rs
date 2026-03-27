//! FFI interface for the delta kernel
//!
//! Exposes that an engine needs to call from C/C++ to interface with kernel

#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
// we re-allow panics in tests
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

use std::default::Default;
use std::os::raw::{c_char, c_void};
use std::ptr::NonNull;
use std::sync::Arc;
use tracing::debug;
use url::Url;
#[cfg(feature = "default-engine-base")]
use {
    delta_kernel::engine::default::executor::tokio::TokioMultiThreadExecutor,
    std::collections::HashMap,
};

use delta_kernel::actions::{Metadata, Protocol};
use delta_kernel::schema::Schema;
use delta_kernel::snapshot::Snapshot;
use delta_kernel::{DeltaResult, Engine, EngineData, LogPath, Version};
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
#[cfg(feature = "catalog-managed")]
pub mod log_path;
pub mod scan;
pub mod schema;
pub mod schema_visitor;
#[cfg(feature = "uc-catalog")]
pub mod uc_catalog;

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

/// An opaque type that rust will understand as a string. This can be obtained by calling
/// [`allocate_kernel_string`] with a [`KernelStringSlice`]
#[handle_descriptor(target=String, mutable=true, sized=true)]
pub struct ExclusiveRustString;

/// Allow engines to create an opaque pointer that Rust will understand as a String. Returns an
/// error if the slice contains invalid utf-8 data.
///
/// # Safety
///
/// Caller is responsible for passing a valid KernelStringSlice
#[no_mangle]
pub unsafe extern "C" fn allocate_kernel_string(
    kernel_str: KernelStringSlice,
    error_fn: AllocateErrorFn,
) -> ExternResult<Handle<ExclusiveRustString>> {
    allocate_kernel_string_impl(kernel_str).into_extern_result(&error_fn)
}

fn allocate_kernel_string_impl(
    kernel_str: KernelStringSlice,
) -> DeltaResult<Handle<ExclusiveRustString>> {
    let s = unsafe { String::try_from_slice(&kernel_str) }?;
    Ok(Box::new(s).into())
}

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
            // safety: Box::leak always returns a valid, non-null pointer
            #[allow(clippy::expect_used)]
            let ptr = NonNull::new(leaked_ptr)
                .expect("This should never be null please report this bug.");
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
            // safety: Box::leak always returns a valid, non-null pointer
            #[allow(clippy::expect_used)]
            let ptr = NonNull::new(leaked_ptr)
                .expect("This should never be null please report this bug.");
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
    /// Configuration for multithreaded executor. If Some, use a multi-threaded executor
    /// If None, use the default single-threaded background executor.
    multithreaded_executor_config: Option<MultithreadedExecutorConfig>,
}

#[cfg(feature = "default-engine-base")]
struct MultithreadedExecutorConfig {
    /// Number of worker threads for the tokio runtime. `None` uses Tokio's default.
    worker_threads: Option<usize>,
    /// Maximum number of threads for blocking operations. `None` uses Tokio's default.
    max_blocking_threads: Option<usize>,
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
        multithreaded_executor_config: None,
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
) -> ExternResult<bool> {
    set_builder_option_impl(builder, key, value).into_extern_result(&builder.allocate_fn)
}
#[cfg(feature = "default-engine-base")]
fn set_builder_option_impl(
    builder: &mut EngineBuilder,
    key: KernelStringSlice,
    value: KernelStringSlice,
) -> DeltaResult<bool> {
    let key = unsafe { String::try_from_slice(&key) }?;
    let value = unsafe { String::try_from_slice(&value) }?;
    builder.set_option(key, value);
    Ok(true)
}

/// Configure the builder to use a multi-threaded executor instead of the default
/// single-threaded background executor.
///
/// # Parameters
/// - `builder`: The engine builder to configure.
/// - `worker_threads`: Number of worker threads. Pass 0 to use Tokio's default.
/// - `max_blocking_threads`: Maximum number of blocking threads. Pass 0 to use Tokio's default.
///
/// # Safety
///
/// Caller must pass a valid EngineBuilder pointer.
#[cfg(feature = "default-engine-base")]
#[no_mangle]
pub unsafe extern "C" fn set_builder_with_multithreaded_executor(
    builder: &mut EngineBuilder,
    worker_threads: usize,
    max_blocking_threads: usize,
) {
    let worker_threads = (worker_threads != 0).then_some(worker_threads);
    let max_blocking_threads = (max_blocking_threads != 0).then_some(max_blocking_threads);

    builder.multithreaded_executor_config = Some(MultithreadedExecutorConfig {
        worker_threads,
        max_blocking_threads,
    });
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
        builder_box.multithreaded_executor_config,
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
    get_default_engine_impl(url?, Default::default(), None, allocate_error)
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

/// Build the default engine
///
/// If `executor_config` is `Some`, uses a multi-threaded executor that owns its runtime. Otherwise,
/// uses the default single-threaded background executor.
#[cfg(feature = "default-engine-base")]
fn get_default_engine_impl(
    url: Url,
    options: HashMap<String, String>,
    executor_config: Option<MultithreadedExecutorConfig>,
    allocate_error: AllocateErrorFn,
) -> DeltaResult<Handle<SharedExternEngine>> {
    use delta_kernel::engine::default::storage::store_from_url_opts;
    use delta_kernel::engine::default::DefaultEngineBuilder;

    let store = store_from_url_opts(&url, options)?;

    let engine: Arc<dyn Engine> = if let Some(config) = executor_config {
        let executor = TokioMultiThreadExecutor::new_owned_runtime(
            config.worker_threads,
            config.max_blocking_threads,
        )?;
        Arc::new(
            DefaultEngineBuilder::new(store)
                .with_task_executor(Arc::new(executor))
                .build(),
        )
    } else {
        Arc::new(DefaultEngineBuilder::new(store).build())
    };

    Ok(engine_to_handle(engine, allocate_error))
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

#[handle_descriptor(target=Protocol, mutable=false, sized=true)]
pub struct SharedProtocol;

#[handle_descriptor(target=Metadata, mutable=false, sized=true)]
pub struct SharedMetadata;

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
    snapshot_impl(url, engine, None, Vec::new()).into_extern_result(&engine)
}

/// Get the latest snapshot from the specified table with optional log tail
///
/// # Safety
///
/// Caller is responsible for passing valid handles and path pointer.
/// The log_paths array and its contents must remain valid for the duration of this call.
#[cfg(feature = "catalog-managed")]
#[no_mangle]
pub unsafe extern "C" fn snapshot_with_log_tail(
    path: KernelStringSlice,
    engine: Handle<SharedExternEngine>,
    log_paths: log_path::LogPathArray,
) -> ExternResult<Handle<SharedSnapshot>> {
    let url = unsafe { unwrap_and_parse_path_as_url(path) };
    let engine_ref = unsafe { engine.as_ref() };

    // Convert LogPathArray to Vec<LogPath>
    let log_tail = match unsafe { log_paths.log_paths() } {
        Ok(paths) => paths,
        Err(err) => return Err(err).into_extern_result(&engine_ref),
    };

    snapshot_impl(url, engine_ref, None, log_tail).into_extern_result(&engine_ref)
}

/// Get the snapshot from the specified table at a specific version. Note this is only safe for
/// non-catalog-managed tables.
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
    snapshot_impl(url, engine, version.into(), Vec::new()).into_extern_result(&engine)
}

/// Get the snapshot from the specified table at a specific version with log tail.
///
/// # Safety
///
/// Caller is responsible for passing valid handles and path pointer.
/// The log_tail array and its contents must remain valid for the duration of this call.
#[cfg(feature = "catalog-managed")]
#[no_mangle]
pub unsafe extern "C" fn snapshot_at_version_with_log_tail(
    path: KernelStringSlice,
    engine: Handle<SharedExternEngine>,
    version: Version,
    log_tail: log_path::LogPathArray,
) -> ExternResult<Handle<SharedSnapshot>> {
    let url = unsafe { unwrap_and_parse_path_as_url(path) };
    let engine_ref = unsafe { engine.as_ref() };

    // Convert LogPathArray to Vec<LogPath>
    let log_tail = match unsafe { log_tail.log_paths() } {
        Ok(paths) => paths,
        Err(err) => return Err(err).into_extern_result(&engine_ref),
    };

    snapshot_impl(url, engine_ref, version.into(), log_tail).into_extern_result(&engine_ref)
}

fn snapshot_impl(
    url: DeltaResult<Url>,
    extern_engine: &dyn ExternEngine,
    version: Option<Version>,
    #[allow(unused_variables)] log_tail: Vec<LogPath>,
) -> DeltaResult<Handle<SharedSnapshot>> {
    let mut builder = Snapshot::builder_for(url?);

    if let Some(v) = version {
        builder = builder.at_version(v);
    }

    #[cfg(feature = "catalog-managed")]
    if !log_tail.is_empty() {
        builder = builder.with_log_tail(log_tail);
    }

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

/// Perform a full checkpoint of the specified snapshot using the supplied engine.
///
/// This writes the checkpoint parquet file and the `_last_checkpoint` file.
///
/// # Safety
///
/// Caller is responsible for passing valid handles.
#[no_mangle]
pub unsafe extern "C" fn checkpoint_snapshot(
    snapshot: Handle<SharedSnapshot>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<bool> {
    let engine_ref = unsafe { engine.as_ref() };
    let snapshot = unsafe { snapshot.clone_as_arc() };
    snapshot_checkpoint_impl(snapshot, engine_ref).into_extern_result(&engine_ref)
}

fn snapshot_checkpoint_impl(
    snapshot: Arc<Snapshot>,
    extern_engine: &dyn ExternEngine,
) -> DeltaResult<bool> {
    snapshot.checkpoint(extern_engine.engine().as_ref())?;
    Ok(true)
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
    snapshot.table_configuration().partition_columns().len()
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
    // NOTE: Clippy doesn't like it, but we need to_vec+into_iter to decouple lifetimes
    let partition_columns = snapshot.table_configuration().partition_columns().to_vec();
    let iter: Box<StringIter> = Box::new(partition_columns.into_iter());
    iter.into()
}

// ── Protocol handle FFI ─────────────────────────────────────────────────────

/// Get the protocol for this snapshot. The returned handle must be freed with [`free_protocol`].
///
/// # Safety
/// Caller is responsible for providing a valid snapshot handle.
#[no_mangle]
pub unsafe extern "C" fn snapshot_get_protocol(
    snapshot: Handle<SharedSnapshot>,
) -> Handle<SharedProtocol> {
    let snapshot = unsafe { snapshot.as_ref() };
    Arc::new(snapshot.table_configuration().protocol().clone()).into()
}

/// Free a protocol handle obtained from [`snapshot_get_protocol`].
///
/// # Safety
/// Caller is responsible for providing a valid, non-freed protocol handle.
#[no_mangle]
pub unsafe extern "C" fn free_protocol(protocol: Handle<SharedProtocol>) {
    protocol.drop_handle();
}

/// Get the minimum reader version from a protocol handle.
///
/// # Safety
/// Caller is responsible for providing a valid protocol handle.
#[no_mangle]
pub unsafe extern "C" fn protocol_min_reader_version(protocol: Handle<SharedProtocol>) -> i32 {
    unsafe { protocol.as_ref() }.min_reader_version()
}

/// Get the minimum writer version from a protocol handle.
///
/// # Safety
/// Caller is responsible for providing a valid protocol handle.
#[no_mangle]
pub unsafe extern "C" fn protocol_min_writer_version(protocol: Handle<SharedProtocol>) -> i32 {
    unsafe { protocol.as_ref() }.min_writer_version()
}

/// Visit each reader feature name in the protocol. If the protocol uses legacy versioning
/// (minReaderVersion < 3), the visitor is never called.
///
/// # Safety
/// Caller is responsible for providing a valid protocol handle, a valid `context` as an opaque
/// pointer passed to each `visitor` invocation, and a valid `visitor` function pointer.
#[no_mangle]
pub unsafe extern "C" fn visit_protocol_reader_features(
    protocol: Handle<SharedProtocol>,
    context: NullableCvoid,
    visitor: extern "C" fn(NullableCvoid, KernelStringSlice),
) {
    let protocol = unsafe { protocol.as_ref() };
    if let Some(features) = protocol.reader_features() {
        for feature in features {
            let name = feature.as_ref();
            visitor(context, kernel_string_slice!(name));
        }
    }
}

/// Visit each writer feature name in the protocol. If the protocol uses legacy versioning
/// (minWriterVersion < 7), the visitor is never called.
///
/// # Safety
/// Caller is responsible for providing a valid protocol handle, a valid `context` as an opaque
/// pointer passed to each `visitor` invocation, and a valid `visitor` function pointer.
#[no_mangle]
pub unsafe extern "C" fn visit_protocol_writer_features(
    protocol: Handle<SharedProtocol>,
    context: NullableCvoid,
    visitor: extern "C" fn(NullableCvoid, KernelStringSlice),
) {
    let protocol = unsafe { protocol.as_ref() };
    if let Some(features) = protocol.writer_features() {
        for feature in features {
            let name = feature.as_ref();
            visitor(context, kernel_string_slice!(name));
        }
    }
}

// ── Metadata handle FFI ─────────────────────────────────────────────────────

/// Get the metadata for this snapshot. The returned handle must be freed with [`free_metadata`].
///
/// # Safety
/// Caller is responsible for providing a valid snapshot handle.
#[no_mangle]
pub unsafe extern "C" fn snapshot_get_metadata(
    snapshot: Handle<SharedSnapshot>,
) -> Handle<SharedMetadata> {
    let snapshot = unsafe { snapshot.as_ref() };
    Arc::new(snapshot.table_configuration().metadata().clone()).into()
}

/// Free a metadata handle obtained from [`snapshot_get_metadata`].
///
/// # Safety
/// Caller is responsible for providing a valid, non-freed metadata handle.
#[no_mangle]
pub unsafe extern "C" fn free_metadata(metadata: Handle<SharedMetadata>) {
    metadata.drop_handle();
}

/// Allocate and return the metadata id string. The string is allocated via the provided
/// `allocate_fn` and must be freed by the caller.
///
/// # Safety
/// Caller is responsible for providing a valid metadata handle and a valid `allocate_fn`.
#[no_mangle]
pub unsafe extern "C" fn metadata_id(
    metadata: Handle<SharedMetadata>,
    allocate_fn: AllocateStringFn,
) -> NullableCvoid {
    let metadata = unsafe { metadata.as_ref() };
    let id = metadata.id();
    allocate_fn(kernel_string_slice!(id))
}

/// Allocate and return the metadata name string, or null if not set. The string (when present)
/// is allocated via the provided `allocate_fn` and must be freed by the caller.
///
/// # Safety
/// Caller is responsible for providing a valid metadata handle and a valid `allocate_fn`.
#[no_mangle]
pub unsafe extern "C" fn metadata_name(
    metadata: Handle<SharedMetadata>,
    allocate_fn: AllocateStringFn,
) -> NullableCvoid {
    let metadata = unsafe { metadata.as_ref() };
    match metadata.name() {
        Some(s) => allocate_fn(kernel_string_slice!(s)),
        None => None,
    }
}

/// Allocate and return the metadata description string, or null if not set. The string (when
/// present) is allocated via the provided `allocate_fn` and must be freed by the caller.
///
/// # Safety
/// Caller is responsible for providing a valid metadata handle and a valid `allocate_fn`.
#[no_mangle]
pub unsafe extern "C" fn metadata_description(
    metadata: Handle<SharedMetadata>,
    allocate_fn: AllocateStringFn,
) -> NullableCvoid {
    let metadata = unsafe { metadata.as_ref() };
    match metadata.description() {
        Some(s) => allocate_fn(kernel_string_slice!(s)),
        None => None,
    }
}

/// Allocate and return the metadata format provider string (e.g. `"parquet"`). The string is
/// allocated via the provided `allocate_fn` and must be freed by the caller.
///
/// # Safety
/// Caller is responsible for providing a valid metadata handle and a valid `allocate_fn`.
#[no_mangle]
pub unsafe extern "C" fn metadata_format_provider(
    metadata: Handle<SharedMetadata>,
    allocate_fn: AllocateStringFn,
) -> NullableCvoid {
    let metadata = unsafe { metadata.as_ref() };
    let provider = metadata.format_provider();
    allocate_fn(kernel_string_slice!(provider))
}

/// Returns true if this metadata has a `createdTime` value set.
///
/// # Safety
/// Caller is responsible for providing a valid metadata handle.
#[no_mangle]
pub unsafe extern "C" fn metadata_has_created_time(metadata: Handle<SharedMetadata>) -> bool {
    unsafe { metadata.as_ref() }.created_time().is_some()
}

/// Returns the `createdTime` value in milliseconds since Unix epoch.
/// Only valid to call if [`metadata_has_created_time`] returned `true`.
///
/// # Safety
/// Caller is responsible for providing a valid metadata handle.
#[no_mangle]
pub unsafe extern "C" fn metadata_created_time(metadata: Handle<SharedMetadata>) -> i64 {
    unsafe { metadata.as_ref() }.created_time().unwrap_or(0)
}

// ── Snapshot-level computed property FFI ─────────────────────────────────────

/// Get the column mapping mode for this snapshot as a u32. Values: 0=None, 1=Id, 2=Name.
///
/// # Safety
/// Caller is responsible for providing a valid snapshot handle.
#[no_mangle]
pub unsafe extern "C" fn snapshot_column_mapping_mode(snapshot: Handle<SharedSnapshot>) -> u32 {
    use delta_kernel::table_features::ColumnMappingMode;
    let snapshot = unsafe { snapshot.as_ref() };
    match snapshot.table_configuration().column_mapping_mode() {
        ColumnMappingMode::None => 0,
        ColumnMappingMode::Id => 1,
        ColumnMappingMode::Name => 2,
    }
}

/// Check whether the given table feature (by camelCase name) is supported by this snapshot's
/// protocol. Returns false for unrecognised feature names.
///
/// # Safety
/// Caller is responsible for providing a valid snapshot handle and `feature_name` slice.
#[no_mangle]
pub unsafe extern "C" fn snapshot_is_feature_supported(
    snapshot: Handle<SharedSnapshot>,
    feature_name: KernelStringSlice,
) -> bool {
    use delta_kernel::table_features::TableFeature;
    let snapshot = unsafe { snapshot.as_ref() };
    let name: &str = match unsafe { TryFromStringSlice::try_from_slice(&feature_name) } {
        Ok(s) => s,
        Err(_) => return false,
    };
    // TableFeature derives EnumString with a default variant (Unknown), so parse is infallible
    #[allow(clippy::unwrap_used)]
    let feature: TableFeature = name.parse().unwrap();
    snapshot
        .table_configuration()
        .is_feature_supported(&feature)
}

/// Check whether row tracking is enabled for this snapshot (feature supported + not suspended).
///
/// # Safety
/// Caller is responsible for providing a valid snapshot handle.
#[no_mangle]
pub unsafe extern "C" fn snapshot_is_row_tracking_enabled(
    snapshot: Handle<SharedSnapshot>,
) -> bool {
    use delta_kernel::table_features::TableFeature;
    let snapshot = unsafe { snapshot.as_ref() };
    snapshot
        .table_configuration()
        .is_feature_supported(&TableFeature::RowTracking)
}

/// Check whether deletion vectors are readable for this snapshot.
///
/// # Safety
/// Caller is responsible for providing a valid snapshot handle.
#[no_mangle]
pub unsafe extern "C" fn snapshot_is_deletion_vector_readable(
    snapshot: Handle<SharedSnapshot>,
) -> bool {
    use delta_kernel::table_features::TableFeature;
    let snapshot = unsafe { snapshot.as_ref() };
    snapshot
        .table_configuration()
        .is_feature_enabled(&TableFeature::DeletionVectors)
}

/// Check whether IcebergCompatV2 is enabled for this snapshot.
///
/// # Safety
/// Caller is responsible for providing a valid snapshot handle.
#[no_mangle]
pub unsafe extern "C" fn snapshot_is_iceberg_compat_v2_enabled(
    snapshot: Handle<SharedSnapshot>,
) -> bool {
    use delta_kernel::table_features::TableFeature;
    let snapshot = unsafe { snapshot.as_ref() };
    snapshot
        .table_configuration()
        .is_feature_enabled(&TableFeature::IcebergCompatV2)
}

/// Check whether IcebergCompatV3 is enabled for this snapshot.
/// Note: IcebergCompatV3 is not yet a recognized kernel feature; this checks the protocol's
/// writer features for an "icebergCompatV3" entry (which parses as `TableFeature::Unknown`).
///
/// # Safety
/// Caller is responsible for providing a valid snapshot handle.
#[no_mangle]
pub unsafe extern "C" fn snapshot_is_iceberg_compat_v3_enabled(
    snapshot: Handle<SharedSnapshot>,
) -> bool {
    use delta_kernel::table_features::TableFeature;
    let snapshot = unsafe { snapshot.as_ref() };
    // IcebergCompatV3 is not yet a recognized kernel variant; parsing produces Unknown("icebergCompatV3").
    // is_feature_supported checks the protocol's writer features list for this entry.
    #[allow(clippy::unwrap_used)]
    let feature: TableFeature = "icebergCompatV3".parse().unwrap();
    snapshot
        .table_configuration()
        .is_feature_supported(&feature)
}

/// Allocate and return the materialized row ID column name, or null if row tracking is not
/// configured. The string (when present) is allocated via `allocate_fn`.
///
/// # Safety
/// Caller is responsible for providing a valid snapshot handle and `allocate_fn`.
#[no_mangle]
pub unsafe extern "C" fn snapshot_materialized_row_id_column_name(
    snapshot: Handle<SharedSnapshot>,
    allocate_fn: AllocateStringFn,
) -> NullableCvoid {
    let snapshot = unsafe { snapshot.as_ref() };
    match snapshot
        .table_configuration()
        .table_properties()
        .materialized_row_id_column_name
        .as_deref()
    {
        Some(s) => allocate_fn(kernel_string_slice!(s)),
        None => None,
    }
}

/// Allocate and return the materialized row commit version column name, or null if row tracking
/// is not configured. The string (when present) is allocated via `allocate_fn`.
///
/// # Safety
/// Caller is responsible for providing a valid snapshot handle and `allocate_fn`.
#[no_mangle]
pub unsafe extern "C" fn snapshot_materialized_row_commit_version_column_name(
    snapshot: Handle<SharedSnapshot>,
    allocate_fn: AllocateStringFn,
) -> NullableCvoid {
    let snapshot = unsafe { snapshot.as_ref() };
    match snapshot
        .table_configuration()
        .table_properties()
        .materialized_row_commit_version_column_name
        .as_deref()
    {
        Some(s) => allocate_fn(kernel_string_slice!(s)),
        None => None,
    }
}

/// Allocate and return the metadata id for the snapshot. The string is allocated via `allocate_fn`.
///
/// # Safety
/// Caller is responsible for providing a valid snapshot handle and `allocate_fn`.
#[no_mangle]
pub unsafe extern "C" fn snapshot_metadata_id(
    snapshot: Handle<SharedSnapshot>,
    allocate_fn: AllocateStringFn,
) -> NullableCvoid {
    let snapshot = unsafe { snapshot.as_ref() };
    let id = snapshot.table_configuration().metadata().id();
    allocate_fn(kernel_string_slice!(id))
}

/// Allocate and return the metadata format provider for the snapshot. The string is allocated
/// via `allocate_fn`.
///
/// # Safety
/// Caller is responsible for providing a valid snapshot handle and `allocate_fn`.
#[no_mangle]
pub unsafe extern "C" fn snapshot_metadata_format_provider(
    snapshot: Handle<SharedSnapshot>,
    allocate_fn: AllocateStringFn,
) -> NullableCvoid {
    let snapshot = unsafe { snapshot.as_ref() };
    let provider = snapshot.table_configuration().metadata().format_provider();
    allocate_fn(kernel_string_slice!(provider))
}

/// Get the minimum reader version from this snapshot's protocol.
///
/// # Safety
/// Caller is responsible for providing a valid snapshot handle.
#[no_mangle]
pub unsafe extern "C" fn snapshot_min_reader_version(snapshot: Handle<SharedSnapshot>) -> i32 {
    let snapshot = unsafe { snapshot.as_ref() };
    snapshot
        .table_configuration()
        .protocol()
        .min_reader_version()
}

/// Get the minimum writer version from this snapshot's protocol.
///
/// # Safety
/// Caller is responsible for providing a valid snapshot handle.
#[no_mangle]
pub unsafe extern "C" fn snapshot_min_writer_version(snapshot: Handle<SharedSnapshot>) -> i32 {
    let snapshot = unsafe { snapshot.as_ref() };
    snapshot
        .table_configuration()
        .protocol()
        .min_writer_version()
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
    use delta_kernel::engine::default::executor::tokio::TokioMultiThreadExecutor;
    use delta_kernel::engine::default::DefaultEngineBuilder;
    use delta_kernel::object_store::memory::InMemory;
    use delta_kernel::object_store::path::Path;
    use delta_kernel::object_store::ObjectStore;
    use serde_json::Value;
    use test_utils::{
        actions_to_string, actions_to_string_partitioned, add_commit, TestAction, METADATA,
    };

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
        let table_root = "memory:///test_table/";
        add_commit(
            table_root,
            storage.as_ref(),
            0,
            actions_to_string(vec![TestAction::Metadata]),
        )
        .await?;
        let engine = DefaultEngineBuilder::new(storage.clone()).build();
        let engine = engine_to_handle(Arc::new(engine), allocate_err);

        // Test getting latest snapshot
        let snapshot1 = unsafe {
            ok_or_panic(snapshot(
                kernel_string_slice!(table_root),
                engine.shallow_copy(),
            ))
        };
        let version1 = unsafe { version(snapshot1.shallow_copy()) };
        assert_eq!(version1, 0);

        // Test getting snapshot at version
        let snapshot2 = unsafe {
            ok_or_panic(snapshot_at_version(
                kernel_string_slice!(table_root),
                engine.shallow_copy(),
                0,
            ))
        };
        let version2 = unsafe { version(snapshot2.shallow_copy()) };
        assert_eq!(version2, 0);

        // Test getting non-existent snapshot
        let snapshot_at_non_existent_version = unsafe {
            snapshot_at_version(kernel_string_slice!(table_root), engine.shallow_copy(), 1)
        };
        assert_extern_result_error_with_message(snapshot_at_non_existent_version, KernelError::GenericError, "Generic delta kernel error: LogSegment end version 0 not the same as the specified end version 1");

        let snapshot_table_root_str =
            unsafe { snapshot_table_root(snapshot1.shallow_copy(), allocate_str) };
        assert!(snapshot_table_root_str.is_some());
        let s = recover_string(snapshot_table_root_str.unwrap());
        assert_eq!(&s, table_root);

        unsafe { free_snapshot(snapshot1) }
        unsafe { free_snapshot(snapshot2) }
        unsafe { free_engine(engine) }
        Ok(())
    }

    // NOTE: Snapshot::checkpoint requires a multi-threaded tokio task executor to avoid deadlocks.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_snapshot_checkpoint() -> Result<(), Box<dyn std::error::Error>> {
        let storage = Arc::new(InMemory::new());
        let table_root = "memory:///";

        // Create a minimal table history: initial metadata+protocol (no commitInfo), then some
        // add/remove commits.
        let protocol_and_metadata = METADATA
            .lines()
            .skip(1) // skip commitInfo
            .collect::<Vec<_>>()
            .join("\n");
        add_commit(table_root, storage.as_ref(), 0, protocol_and_metadata).await?;
        add_commit(
            table_root,
            storage.as_ref(),
            1,
            actions_to_string(vec![
                TestAction::Add("file1.parquet".into()),
                TestAction::Add("file2.parquet".into()),
            ]),
        )
        .await?;
        add_commit(
            table_root,
            storage.as_ref(),
            2,
            actions_to_string(vec![
                TestAction::Add("file3.parquet".into()),
                TestAction::Remove("file1.parquet".into()),
            ]),
        )
        .await?;

        let executor = Arc::new(TokioMultiThreadExecutor::new(
            tokio::runtime::Handle::current(),
        ));
        let engine = DefaultEngineBuilder::new(storage.clone())
            .with_task_executor(executor)
            .build();
        let engine = engine_to_handle(Arc::new(engine), allocate_err);

        let snapshot = unsafe {
            ok_or_panic(snapshot(
                kernel_string_slice!(table_root),
                engine.shallow_copy(),
            ))
        };

        let did_checkpoint = unsafe {
            ok_or_panic(checkpoint_snapshot(
                snapshot.shallow_copy(),
                engine.shallow_copy(),
            ))
        };
        assert!(did_checkpoint);

        // Verify `_last_checkpoint` exists and looks sane.
        let last_checkpoint = storage
            .get(&Path::from("_delta_log/_last_checkpoint"))
            .await?;
        let last_checkpoint_bytes = last_checkpoint.bytes().await?;
        let v: Value = serde_json::from_slice(last_checkpoint_bytes.as_ref())?;
        assert_eq!(v["version"].as_u64(), Some(2));
        // Here file1 was removed, so only file2 and
        // file3 remain.
        assert_eq!(v["numOfAddFiles"].as_u64(), Some(2));
        // size = 1 protocol + 1 metadata + 2 live adds
        assert_eq!(v["size"].as_u64(), Some(4));

        // Cross-check checkpoint file size against `_last_checkpoint.sizeInBytes`.
        let checkpoint_path = Path::from("_delta_log/00000000000000000002.checkpoint.parquet");
        let checkpoint_size = storage.head(&checkpoint_path).await?.size;
        assert_eq!(v["sizeInBytes"].as_u64(), Some(checkpoint_size));

        unsafe { free_snapshot(snapshot) }
        unsafe { free_engine(engine) }
        Ok(())
    }

    // Test checkpoint using FFI engine builder APIs with multithreaded executor.
    // NOTE: We made this a sync test to simulate the expected case: C code calling FFI APIs to build engine without existing tokio runtime.
    #[cfg(feature = "default-engine-base")]
    #[test]
    fn test_setting_multithread_executor() -> Result<(), Box<dyn std::error::Error>> {
        use delta_kernel::object_store::local::LocalFileSystem;
        use tempfile::tempdir;

        let tmp_dir = tempdir()?;
        let tmp_path = tmp_dir.path();
        let table_root = tmp_path
            .to_str()
            .ok_or_else(|| delta_kernel::Error::generic("Invalid path"))?;
        let storage = Arc::new(LocalFileSystem::new());

        // Create a minimal table history: initial metadata+protocol (no commitInfo), then some
        // add/remove commits.
        let protocol_and_metadata = METADATA
            .lines()
            .skip(1) // skip commitInfo
            .collect::<Vec<_>>()
            .join("\n");

        // Use a temporary runtime for async setup, then drop it before FFI calls
        {
            let rt = tokio::runtime::Runtime::new()?;
            rt.block_on(async {
                add_commit(&table_root, storage.as_ref(), 0, protocol_and_metadata).await?;
                add_commit(
                    &table_root,
                    storage.as_ref(),
                    1,
                    actions_to_string(vec![
                        TestAction::Add("file1.parquet".into()),
                        TestAction::Add("file2.parquet".into()),
                    ]),
                )
                .await?;
                add_commit(
                    &table_root,
                    storage.as_ref(),
                    2,
                    actions_to_string(vec![
                        TestAction::Add("file3.parquet".into()),
                        TestAction::Remove("file1.parquet".into()),
                    ]),
                )
                .await?;
                Ok::<_, Box<dyn std::error::Error>>(())
            })?;
        } // runtime dropped here, before FFI calls

        // Build engine using FFI APIs
        let builder = unsafe {
            ok_or_panic(get_engine_builder(
                kernel_string_slice!(table_root),
                allocate_err,
            ))
        };
        unsafe { set_builder_with_multithreaded_executor(builder.as_mut().unwrap(), 2, 0) };
        let engine = unsafe { ok_or_panic(builder_build(builder)) };

        let snapshot = unsafe {
            ok_or_panic(snapshot(
                kernel_string_slice!(table_root),
                engine.shallow_copy(),
            ))
        };

        let did_checkpoint = unsafe {
            ok_or_panic(checkpoint_snapshot(
                snapshot.shallow_copy(),
                engine.shallow_copy(),
            ))
        };
        assert!(did_checkpoint);

        unsafe { free_snapshot(snapshot) }
        unsafe { free_engine(engine) }
        Ok(())
    }

    #[tokio::test]
    async fn test_snapshot_partition_cols() -> Result<(), Box<dyn std::error::Error>> {
        let storage = Arc::new(InMemory::new());
        let table_root = "memory:///test_table/";

        add_commit(
            table_root,
            storage.as_ref(),
            0,
            actions_to_string_partitioned(vec![TestAction::Metadata]),
        )
        .await?;
        let engine = DefaultEngineBuilder::new(storage.clone()).build();
        let engine = engine_to_handle(Arc::new(engine), allocate_err);

        let snapshot = unsafe {
            ok_or_panic(snapshot(
                kernel_string_slice!(table_root),
                engine.shallow_copy(),
            ))
        };

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
        let table_root = "memory:///";

        add_commit(
            table_root,
            storage.as_ref(),
            0,
            actions_to_string(vec![TestAction::Metadata]),
        )
        .await?;
        let engine = DefaultEngineBuilder::new(storage.clone()).build();
        let engine = engine_to_handle(Arc::new(engine), allocate_null_err);

        // Get a non-existent snapshot, this will call allocate_null_err
        let snapshot_at_non_existent_version = unsafe {
            snapshot_at_version(kernel_string_slice!(table_root), engine.shallow_copy(), 1)
        };
        assert!(snapshot_at_non_existent_version.is_err());

        unsafe { free_engine(engine) }
        Ok(())
    }

    #[cfg(feature = "catalog-managed")]
    #[tokio::test]
    async fn test_snapshot_log_tail() -> Result<(), Box<dyn std::error::Error>> {
        use test_utils::add_staged_commit;
        let storage = Arc::new(InMemory::new());
        let table_root = "memory:///test_table/";

        add_commit(
            table_root,
            storage.as_ref(),
            0,
            actions_to_string(vec![TestAction::Metadata]),
        )
        .await?;
        let commit1 = add_staged_commit(
            table_root,
            storage.as_ref(),
            1,
            actions_to_string(vec![TestAction::Add("path1".into())]),
        )
        .await?;
        let engine = DefaultEngineBuilder::new(storage.clone()).build();
        let engine = engine_to_handle(Arc::new(engine), allocate_err);

        let commit1_path = format!(
            "{}_delta_log/_staged_commits/{}",
            table_root,
            commit1.filename().unwrap()
        );
        let log_path =
            log_path::FfiLogPath::new(kernel_string_slice!(commit1_path), 123456789, 100);
        let log_tail = [log_path];
        let log_tail = log_path::LogPathArray {
            ptr: log_tail.as_ptr(),
            len: log_tail.len(),
        };
        let snapshot = unsafe {
            ok_or_panic(snapshot_with_log_tail(
                kernel_string_slice!(table_root),
                engine.shallow_copy(),
                log_tail.clone(),
            ))
        };
        let snapshot_version = unsafe { version(snapshot.shallow_copy()) };
        assert_eq!(snapshot_version, 1);

        // Test getting snapshot at version
        let snapshot2 = unsafe {
            ok_or_panic(snapshot_at_version_with_log_tail(
                kernel_string_slice!(table_root),
                engine.shallow_copy(),
                1,
                log_tail,
            ))
        };
        let snapshot_version = unsafe { version(snapshot.shallow_copy()) };
        assert_eq!(snapshot_version, 1);

        unsafe { free_snapshot(snapshot) }
        unsafe { free_snapshot(snapshot2) }
        unsafe { free_engine(engine) }
        Ok(())
    }

    // A modern (table-features) protocol + metadata commit for feature/config tests.
    // Protocol: minReaderVersion=3, minWriterVersion=7 with columnMapping + rowTracking.
    // Metadata: id set, name set, createdTime set, column mapping mode = "name".
    const METADATA_WITH_FEATURES: &str = concat!(
        r#"{"commitInfo":{"timestamp":1587968586154,"operation":"WRITE","operationParameters":{},"isBlindAppend":true}}"#,
        "\n",
        r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["columnMapping"],"writerFeatures":["columnMapping","rowTracking"]}}"#,
        "\n",
        r#"{"metaData":{"id":"deadbeef-1234-5678-abcd-000000000000","name":"test_table","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[]}","partitionColumns":[],"configuration":{"delta.columnMapping.mode":"name","delta.rowTracking.enabled":"true","delta.rowTracking.materializedRowIdColumnName":"_row_id","delta.rowTracking.materializedRowCommitVersionColumnName":"_row_commit_version"},"createdTime":1234567890000}}"#,
    );

    #[tokio::test]
    async fn test_protocol_handle() -> Result<(), Box<dyn std::error::Error>> {
        let storage = Arc::new(InMemory::new());
        let table_root = "memory:///test_protocol/";
        // Default METADATA has minReaderVersion=1, minWriterVersion=2 (legacy, no explicit features)
        add_commit(table_root, storage.as_ref(), 0, METADATA.to_string()).await?;

        let engine = engine_to_handle(
            Arc::new(DefaultEngineBuilder::new(storage.clone()).build()),
            allocate_err,
        );
        let snap = unsafe {
            ok_or_panic(snapshot(
                kernel_string_slice!(table_root),
                engine.shallow_copy(),
            ))
        };

        let proto = unsafe { snapshot_get_protocol(snap.shallow_copy()) };

        assert_eq!(
            unsafe { protocol_min_reader_version(proto.shallow_copy()) },
            1
        );
        assert_eq!(
            unsafe { protocol_min_writer_version(proto.shallow_copy()) },
            2
        );

        // Legacy protocol: visitor should never be called for reader or writer features.
        static READER_CALLED: std::sync::atomic::AtomicBool =
            std::sync::atomic::AtomicBool::new(false);
        static WRITER_CALLED: std::sync::atomic::AtomicBool =
            std::sync::atomic::AtomicBool::new(false);
        extern "C" fn reader_visitor(_ctx: NullableCvoid, _s: KernelStringSlice) {
            READER_CALLED.store(true, std::sync::atomic::Ordering::SeqCst);
        }
        extern "C" fn writer_visitor(_ctx: NullableCvoid, _s: KernelStringSlice) {
            WRITER_CALLED.store(true, std::sync::atomic::Ordering::SeqCst);
        }
        unsafe { visit_protocol_reader_features(proto.shallow_copy(), None, reader_visitor) };
        unsafe { visit_protocol_writer_features(proto.shallow_copy(), None, writer_visitor) };
        assert!(!READER_CALLED.load(std::sync::atomic::Ordering::SeqCst));
        assert!(!WRITER_CALLED.load(std::sync::atomic::Ordering::SeqCst));

        unsafe { free_protocol(proto) };
        unsafe { free_snapshot(snap) };
        unsafe { free_engine(engine) };
        Ok(())
    }

    #[tokio::test]
    async fn test_protocol_features_handle() -> Result<(), Box<dyn std::error::Error>> {
        let storage = Arc::new(InMemory::new());
        let table_root = "memory:///test_protocol_features/";
        add_commit(
            table_root,
            storage.as_ref(),
            0,
            METADATA_WITH_FEATURES.to_string(),
        )
        .await?;

        let engine = engine_to_handle(
            Arc::new(DefaultEngineBuilder::new(storage.clone()).build()),
            allocate_err,
        );
        let snap = unsafe {
            ok_or_panic(snapshot(
                kernel_string_slice!(table_root),
                engine.shallow_copy(),
            ))
        };
        let proto = unsafe { snapshot_get_protocol(snap.shallow_copy()) };

        assert_eq!(
            unsafe { protocol_min_reader_version(proto.shallow_copy()) },
            3
        );
        assert_eq!(
            unsafe { protocol_min_writer_version(proto.shallow_copy()) },
            7
        );

        // Reader features: should yield "columnMapping"
        let reader_features: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());
        extern "C" fn collect_reader(ctx: NullableCvoid, s: KernelStringSlice) {
            let vec = unsafe { &*(ctx.unwrap().as_ptr() as *const std::sync::Mutex<Vec<String>>) };
            vec.lock()
                .unwrap()
                .push(unsafe { String::try_from_slice(&s) }.unwrap());
        }
        let ctx = std::ptr::NonNull::new(&reader_features as *const _ as *mut std::ffi::c_void);
        unsafe { visit_protocol_reader_features(proto.shallow_copy(), ctx, collect_reader) };
        let rf = reader_features.into_inner().unwrap();
        assert_eq!(rf, vec!["columnMapping"]);

        // Writer features: should yield "columnMapping" and "rowTracking"
        let writer_features: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());
        extern "C" fn collect_writer(ctx: NullableCvoid, s: KernelStringSlice) {
            let vec = unsafe { &*(ctx.unwrap().as_ptr() as *const std::sync::Mutex<Vec<String>>) };
            vec.lock()
                .unwrap()
                .push(unsafe { String::try_from_slice(&s) }.unwrap());
        }
        let ctx = std::ptr::NonNull::new(&writer_features as *const _ as *mut std::ffi::c_void);
        unsafe { visit_protocol_writer_features(proto.shallow_copy(), ctx, collect_writer) };
        let mut wf = writer_features.into_inner().unwrap();
        wf.sort(); // order not guaranteed
        assert_eq!(wf, vec!["columnMapping", "rowTracking"]);

        unsafe { free_protocol(proto) };
        unsafe { free_snapshot(snap) };
        unsafe { free_engine(engine) };
        Ok(())
    }

    #[tokio::test]
    async fn test_metadata_handle() -> Result<(), Box<dyn std::error::Error>> {
        let storage = Arc::new(InMemory::new());
        let table_root = "memory:///test_metadata/";
        // Default METADATA: id=5fba94ed-..., no name/description, provider=parquet, createdTime set
        add_commit(table_root, storage.as_ref(), 0, METADATA.to_string()).await?;

        let engine = engine_to_handle(
            Arc::new(DefaultEngineBuilder::new(storage.clone()).build()),
            allocate_err,
        );
        let snap = unsafe {
            ok_or_panic(snapshot(
                kernel_string_slice!(table_root),
                engine.shallow_copy(),
            ))
        };
        let meta = unsafe { snapshot_get_metadata(snap.shallow_copy()) };

        // id
        let id = recover_string(unsafe { metadata_id(meta.shallow_copy(), allocate_str) }.unwrap());
        assert_eq!(id, "5fba94ed-9794-4965-ba6e-6ee3c0d22af9");

        // name: not set in default METADATA
        let name_ptr = unsafe { metadata_name(meta.shallow_copy(), allocate_str) };
        assert!(
            name_ptr.is_none(),
            "name should be null for default metadata"
        );

        // description: not set
        let desc_ptr = unsafe { metadata_description(meta.shallow_copy(), allocate_str) };
        assert!(
            desc_ptr.is_none(),
            "description should be null for default metadata"
        );

        // format provider
        let provider = recover_string(
            unsafe { metadata_format_provider(meta.shallow_copy(), allocate_str) }.unwrap(),
        );
        assert_eq!(provider, "parquet");

        // createdTime
        assert!(unsafe { metadata_has_created_time(meta.shallow_copy()) });
        assert_eq!(
            unsafe { metadata_created_time(meta.shallow_copy()) },
            1587968585495
        );

        unsafe { free_metadata(meta) };
        unsafe { free_snapshot(snap) };
        unsafe { free_engine(engine) };
        Ok(())
    }

    #[tokio::test]
    async fn test_metadata_handle_with_name() -> Result<(), Box<dyn std::error::Error>> {
        let storage = Arc::new(InMemory::new());
        let table_root = "memory:///test_metadata_name/";
        // METADATA_WITH_FEATURES has name="test_table" and id="deadbeef-..."
        add_commit(
            table_root,
            storage.as_ref(),
            0,
            METADATA_WITH_FEATURES.to_string(),
        )
        .await?;

        let engine = engine_to_handle(
            Arc::new(DefaultEngineBuilder::new(storage.clone()).build()),
            allocate_err,
        );
        let snap = unsafe {
            ok_or_panic(snapshot(
                kernel_string_slice!(table_root),
                engine.shallow_copy(),
            ))
        };
        let meta = unsafe { snapshot_get_metadata(snap.shallow_copy()) };

        let id = recover_string(unsafe { metadata_id(meta.shallow_copy(), allocate_str) }.unwrap());
        assert_eq!(id, "deadbeef-1234-5678-abcd-000000000000");

        let name =
            recover_string(unsafe { metadata_name(meta.shallow_copy(), allocate_str) }.unwrap());
        assert_eq!(name, "test_table");

        assert!(unsafe { metadata_has_created_time(meta.shallow_copy()) });
        assert_eq!(
            unsafe { metadata_created_time(meta.shallow_copy()) },
            1234567890000
        );

        unsafe { free_metadata(meta) };
        unsafe { free_snapshot(snap) };
        unsafe { free_engine(engine) };
        Ok(())
    }

    #[tokio::test]
    async fn test_snapshot_computed_properties() -> Result<(), Box<dyn std::error::Error>> {
        let storage = Arc::new(InMemory::new());
        let table_root = "memory:///test_computed/";
        add_commit(
            table_root,
            storage.as_ref(),
            0,
            METADATA_WITH_FEATURES.to_string(),
        )
        .await?;

        let engine = engine_to_handle(
            Arc::new(DefaultEngineBuilder::new(storage.clone()).build()),
            allocate_err,
        );
        let snap = unsafe {
            ok_or_panic(snapshot(
                kernel_string_slice!(table_root),
                engine.shallow_copy(),
            ))
        };

        // column mapping mode: "name" => 2
        assert_eq!(
            unsafe { snapshot_column_mapping_mode(snap.shallow_copy()) },
            2
        );

        // feature support
        let col_mapping = "columnMapping";
        let row_tracking = "rowTracking";
        let unknown = "unknownFeatureXyz";
        assert!(unsafe {
            snapshot_is_feature_supported(snap.shallow_copy(), kernel_string_slice!(col_mapping))
        });
        assert!(unsafe {
            snapshot_is_feature_supported(snap.shallow_copy(), kernel_string_slice!(row_tracking))
        });
        assert!(!unsafe {
            snapshot_is_feature_supported(snap.shallow_copy(), kernel_string_slice!(unknown))
        });

        // row tracking: supported (in protocol) — should_write_row_tracking checks suspended flag;
        // is_feature_supported for rowTracking returns true here
        assert!(unsafe { snapshot_is_row_tracking_enabled(snap.shallow_copy()) });

        // deletion vectors: not in this protocol
        assert!(!unsafe { snapshot_is_deletion_vector_readable(snap.shallow_copy()) });

        // materialized row tracking columns
        let row_id_col = recover_string(
            unsafe { snapshot_materialized_row_id_column_name(snap.shallow_copy(), allocate_str) }
                .unwrap(),
        );
        assert_eq!(row_id_col, "_row_id");

        let commit_ver_col = recover_string(
            unsafe {
                snapshot_materialized_row_commit_version_column_name(
                    snap.shallow_copy(),
                    allocate_str,
                )
            }
            .unwrap(),
        );
        assert_eq!(commit_ver_col, "_row_commit_version");

        unsafe { free_snapshot(snap) };
        unsafe { free_engine(engine) };
        Ok(())
    }
}
