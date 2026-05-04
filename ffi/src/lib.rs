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

use delta_kernel::actions::{Metadata, Protocol};
use delta_kernel::schema::Schema;
use delta_kernel::snapshot::{Snapshot, SnapshotRef};
use delta_kernel::{DeltaResult, Engine, EngineData, LogPath, Version};
use delta_kernel_ffi_macros::handle_descriptor;
use tracing::debug;
use url::Url;
#[cfg(feature = "default-engine-base")]
use {
    delta_kernel::engine::default::executor::tokio::TokioMultiThreadExecutor,
    std::collections::HashMap,
};

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
#[cfg(feature = "delta-kernel-unity-catalog")]
pub mod delta_kernel_unity_catalog;
pub mod expressions;
#[cfg(feature = "tracing")]
pub mod ffi_tracing;
pub mod log_path;
pub mod scan;
pub mod schema;
pub mod schema_visitor;

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
pub use private::{KernelBoolSlice, KernelRowIndexArray};

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

// TODO: Do we want this handle at all? Perhaps we should just _always_ pass raw *mut c_void
// pointers that are the engine data? Even if we want the type, should it be a shared handle
// instead?
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
    parquet_writer_config: delta_kernel::table_properties::ParquetWriterConfig,
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
        parquet_writer_config: Default::default(),
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

/// Set the Parquet compression codec on the builder.
///
/// Accepted codec names (case-insensitive): `"snappy"`, `"zstd"`, `"uncompressed"`. Unrecognized
/// names are silently ignored and the default codec (`zstd`) continues to apply.
///
/// # Safety
///
/// Caller must pass a valid `EngineBuilder` pointer and a valid string slice for `codec`.
#[cfg(feature = "default-engine-base")]
#[no_mangle]
pub unsafe extern "C" fn set_builder_parquet_compression(
    builder: &mut EngineBuilder,
    codec: KernelStringSlice,
) {
    use delta_kernel::table_properties::{ParquetCompression, ParquetWriterConfig};
    if let Ok(s) = unsafe { String::try_from_slice(&codec) } {
        if let Ok(compression) = ParquetCompression::try_from(s.as_str()) {
            builder.parquet_writer_config = ParquetWriterConfig { compression };
        }
    }
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
        builder_box.parquet_writer_config,
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
    get_default_engine_impl(
        url?,
        Default::default(),
        None,
        Default::default(),
        allocate_error,
    )
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
    parquet_writer_config: delta_kernel::table_properties::ParquetWriterConfig,
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
                .with_parquet_writer_config(parquet_writer_config)
                .build(),
        )
    } else {
        Arc::new(
            DefaultEngineBuilder::new(store)
                .with_parquet_writer_config(parquet_writer_config)
                .build(),
        )
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

/// Opaque builder for constructing a [`SharedSnapshot`].
///
/// Create with [`get_snapshot_builder`] (from a table path) or [`get_snapshot_builder_from`]
/// (incrementally from an existing snapshot). Configure with [`snapshot_builder_set_version`],
/// [`snapshot_builder_set_log_tail`], and [`snapshot_builder_set_max_catalog_version`] (for
/// catalog-managed tables). Finally, call [`snapshot_builder_build`] to consume the builder and
/// obtain the snapshot. If you need to discard the builder without building, call
/// [`free_snapshot_builder`].
pub struct FfiSnapshotBuilder {
    engine: Arc<dyn ExternEngine>,
    source: FfiSnapshotBuilderSource,
    version: Option<Version>,
    log_tail: Vec<LogPath>,
    max_catalog_version: Option<Version>,
}

/// An opaque handle with exclusive (Box-like) ownership of a [`FfiSnapshotBuilder`].
#[handle_descriptor(target=FfiSnapshotBuilder, mutable=true, sized=true)]
pub struct MutableFfiSnapshotBuilder;

enum FfiSnapshotBuilderSource {
    TableRoot(Url),
    ExistingSnapshot(SnapshotRef),
}

fn make_snapshot_builder(
    source: FfiSnapshotBuilderSource,
    engine: Arc<dyn ExternEngine>,
) -> DeltaResult<Handle<MutableFfiSnapshotBuilder>> {
    Ok(Box::new(FfiSnapshotBuilder {
        engine,
        source,
        version: None,
        log_tail: Vec::new(),
        max_catalog_version: None,
    })
    .into())
}

/// Get a builder for creating a [`SharedSnapshot`] from a table path.
///
/// Use [`snapshot_builder_set_version`] to pin a specific version, then call
/// [`snapshot_builder_build`] to obtain the snapshot. The caller owns the returned handle and must
/// eventually call either [`snapshot_builder_build`] to produce a [`SharedSnapshot`], or
/// [`free_snapshot_builder`] to drop it without building.
///
/// # Safety
///
/// Caller is responsible for passing a valid path and engine handle.
#[no_mangle]
pub unsafe extern "C" fn get_snapshot_builder(
    path: KernelStringSlice,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<MutableFfiSnapshotBuilder>> {
    let engine_ref = unsafe { engine.as_ref() };
    let engine_arc = unsafe { engine.clone_as_arc() };
    let url = unsafe { unwrap_and_parse_path_as_url(path) };
    let source = match url {
        Ok(url) => FfiSnapshotBuilderSource::TableRoot(url),
        Err(e) => return DeltaResult::Err(e).into_extern_result(&engine_ref),
    };
    make_snapshot_builder(source, engine_arc).into_extern_result(&engine_ref)
}

/// Get a builder for incrementally updating an existing snapshot.
///
/// This avoids re-reading the full log. Use [`snapshot_builder_set_version`] to target a specific
/// version, then call [`snapshot_builder_build`] to obtain the updated snapshot. The caller owns
/// the returned handle and must eventually call either [`snapshot_builder_build`] to produce a
/// [`SharedSnapshot`], or [`free_snapshot_builder`] to drop it without building.
///
/// # Safety
///
/// Caller is responsible for passing valid handles.
#[no_mangle]
pub unsafe extern "C" fn get_snapshot_builder_from(
    prev_snapshot: Handle<SharedSnapshot>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<MutableFfiSnapshotBuilder>> {
    let engine_ref = unsafe { engine.as_ref() };
    let engine_arc = unsafe { engine.clone_as_arc() };
    let snapshot_arc = unsafe { prev_snapshot.clone_as_arc() };
    make_snapshot_builder(
        FfiSnapshotBuilderSource::ExistingSnapshot(snapshot_arc),
        engine_arc,
    )
    .into_extern_result(&engine_ref)
}

/// Set the target version on a snapshot builder. When omitted, the snapshot is created at the
/// latest version of the table.
///
/// # Safety
///
/// Caller must pass a valid builder pointer.
#[no_mangle]
pub unsafe extern "C" fn snapshot_builder_set_version(
    builder: &mut Handle<MutableFfiSnapshotBuilder>,
    version: Version,
) {
    unsafe { builder.as_mut() }.version = Some(version);
}

/// Set the log tail on a snapshot builder for catalog-managed tables.
///
/// # Safety
///
/// Caller must pass a valid builder pointer. The log_tail array and its contents must remain valid
/// for the duration of this call.
#[no_mangle]
pub unsafe extern "C" fn snapshot_builder_set_log_tail(
    builder: &mut Handle<MutableFfiSnapshotBuilder>,
    log_tail: log_path::LogPathArray,
) -> ExternResult<bool> {
    let builder_mut = unsafe { builder.as_mut() };
    let engine_arc = builder_mut.engine.clone();
    let engine_ref = engine_arc.as_ref();
    snapshot_builder_set_log_tail_impl(builder_mut, log_tail).into_extern_result(&engine_ref)
}

unsafe fn snapshot_builder_set_log_tail_impl(
    builder: &mut FfiSnapshotBuilder,
    log_tail: log_path::LogPathArray,
) -> DeltaResult<bool> {
    builder.log_tail = unsafe { log_tail.log_paths() }?;
    Ok(true)
}

/// Set the max catalog version on a snapshot builder for catalog-managed tables. This bounds the
/// snapshot version to what the catalog has ratified.
///
/// # Safety
///
/// Caller must pass a valid builder pointer.
#[no_mangle]
pub unsafe extern "C" fn snapshot_builder_set_max_catalog_version(
    builder: &mut Handle<MutableFfiSnapshotBuilder>,
    max_catalog_version: Version,
) {
    unsafe { builder.as_mut() }.max_catalog_version = Some(max_catalog_version);
}

/// Consume the builder and return a snapshot. After calling, the builder pointer is _no longer
/// valid_. The builder is always freed by this call, whether or not it succeeds.
///
/// # Safety
///
/// Caller must pass a valid builder pointer and must not use it again after this call.
#[no_mangle]
pub unsafe extern "C" fn snapshot_builder_build(
    mut builder: Handle<MutableFfiSnapshotBuilder>,
) -> ExternResult<Handle<SharedSnapshot>> {
    // Clone the engine Arc before consuming the handle so we can still use it for error reporting
    let engine_arc = unsafe { builder.as_mut() }.engine.clone();
    let engine_ref = engine_arc.as_ref();
    let builder_box = unsafe { builder.into_inner() };
    snapshot_builder_build_impl(*builder_box).into_extern_result(&engine_ref)
}

fn snapshot_builder_build_impl(builder: FfiSnapshotBuilder) -> DeltaResult<Handle<SharedSnapshot>> {
    let engine = builder.engine.engine();
    let mut rust_builder = match builder.source {
        FfiSnapshotBuilderSource::TableRoot(url) => Snapshot::builder_for(url),
        FfiSnapshotBuilderSource::ExistingSnapshot(snap) => Snapshot::builder_from(snap),
    };
    if let Some(v) = builder.version {
        rust_builder = rust_builder.at_version(v);
    }
    if !builder.log_tail.is_empty() {
        rust_builder = rust_builder.with_log_tail(builder.log_tail);
    }
    if let Some(mcv) = builder.max_catalog_version {
        rust_builder = rust_builder.with_max_catalog_version(mcv);
    }
    let snapshot = rust_builder.build(engine.as_ref())?;
    Ok(snapshot.into())
}

/// Free a snapshot builder without building a snapshot (e.g. on an error path).
///
/// # Safety
///
/// Caller must pass a valid builder pointer and must not use it again after this call.
#[no_mangle]
pub unsafe extern "C" fn free_snapshot_builder(builder: Handle<MutableFfiSnapshotBuilder>) {
    builder.drop_handle();
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
// TODO: Expose the updated snapshot via a new FFI function that returns a snapshot handle.
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
    let (_result, _updated) = snapshot.checkpoint(extern_engine.engine().as_ref(), None)?;
    // We ignore the CheckpointWriteResult because both Written and AlreadyExists are non-error
    // outcomes at the FFI layer.
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

/// Get the timestamp of the specified snapshot in milliseconds since the Unix epoch.
///
/// When In-Commit Timestamp (ICT) is enabled, returns the ICT value from the commit's
/// `CommitInfo` action. Otherwise, falls back to the filesystem last-modified time of
/// the latest commit file.
///
/// Returns an error if the commit file is missing, the ICT configuration is invalid, or the
/// ICT value cannot be read.
///
/// # Safety
///
/// Caller is responsible for passing valid snapshot handle and engine handle.
#[no_mangle]
pub unsafe extern "C" fn snapshot_timestamp(
    snapshot: Handle<SharedSnapshot>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<i64> {
    let engine_ref = unsafe { engine.as_ref() };
    let snapshot = unsafe { snapshot.as_ref() };
    snapshot
        .get_timestamp(engine_ref.engine().as_ref())
        .into_extern_result(&engine_ref)
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

/// Visit each metadata configuration (key/value pair) for the specified snapshot by invoking the
/// provided `visitor` callback once per entry.
///
/// # Safety
///
/// Caller is responsible for passing a valid snapshot handle, a valid `engine_context` as an
/// opaque pointer passed to each `visitor` invocation, and a valid `visitor` function pointer.
#[no_mangle]
pub unsafe extern "C" fn visit_metadata_configuration(
    snapshot: Handle<SharedSnapshot>,
    engine_context: NullableCvoid,
    visitor: extern "C" fn(
        engine_context: NullableCvoid,
        key: KernelStringSlice,
        value: KernelStringSlice,
    ),
) {
    let snapshot = unsafe { snapshot.as_ref() };
    snapshot
        .table_configuration()
        .metadata()
        .configuration()
        .iter()
        .for_each(|(key, value)| {
            visitor(
                engine_context,
                kernel_string_slice!(key),
                kernel_string_slice!(value),
            );
        });
}
// === Protocol handle FFI ===

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

/// Visit all fields of the protocol in a single FFI call. The caller provides:
/// - `visit_versions`: called once with `(context, min_reader_version, min_writer_version)`
/// - `visit_feature`: called once per feature with `(context, is_reader, feature_name)`.
///   `is_reader` is `true` for reader features, `false` for writer features. If the protocol uses
///   legacy versioning (no explicit feature lists), the `visit_feature` callback will not fire.
///
/// # Safety
/// Caller is responsible for providing a valid protocol handle, a valid `context` pointer, and
/// valid function pointers for `visit_versions` and `visit_feature`.
#[no_mangle]
pub unsafe extern "C" fn visit_protocol(
    protocol: Handle<SharedProtocol>,
    context: NullableCvoid,
    visit_versions: extern "C" fn(context: NullableCvoid, min_reader: i32, min_writer: i32),
    visit_feature: extern "C" fn(
        context: NullableCvoid,
        is_reader: bool,
        feature: KernelStringSlice,
    ),
) {
    let protocol = unsafe { protocol.as_ref() };
    visit_versions(
        context,
        protocol.min_reader_version(),
        protocol.min_writer_version(),
    );
    if let Some(features) = protocol.reader_features() {
        for f in features {
            let name = f.as_ref();
            visit_feature(context, true, kernel_string_slice!(name));
        }
    }
    if let Some(features) = protocol.writer_features() {
        for f in features {
            let name = f.as_ref();
            visit_feature(context, false, kernel_string_slice!(name));
        }
    }
}

// === Metadata handle FFI ===

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

/// Visit all fields of the metadata in a single FFI call. String fields are passed as
/// [`KernelStringSlice`] references that borrow from the metadata handle -- they are only valid
/// for the duration of the callback.
///
/// The visitor receives:
/// - `id`: always present
/// - `name`: `OptionalValue::None` if not set
/// - `description`: `OptionalValue::None` if not set
/// - `format_provider`: always present
/// - `has_created_time`: whether `created_time_ms` is meaningful
/// - `created_time_ms`: milliseconds since epoch (only valid when `has_created_time` is true)
///
/// # Safety
/// Caller is responsible for providing a valid metadata handle, a valid `context` pointer, and
/// a valid `visit_metadata_fields` function pointer. String slices must not be retained past
/// the callback return.
#[no_mangle]
pub unsafe extern "C" fn visit_metadata(
    metadata: Handle<SharedMetadata>,
    context: NullableCvoid,
    visit_metadata_fields: extern "C" fn(
        context: NullableCvoid,
        id: KernelStringSlice,
        name: OptionalValue<KernelStringSlice>,
        description: OptionalValue<KernelStringSlice>,
        format_provider: KernelStringSlice,
        has_created_time: bool,
        created_time_ms: i64,
    ),
) {
    let metadata = unsafe { metadata.as_ref() };
    let id_str = metadata.id();
    let id = kernel_string_slice!(id_str);
    let name = metadata.name().map(|s| kernel_string_slice!(s)).into();
    let description = metadata
        .description()
        .map(|s| kernel_string_slice!(s))
        .into();
    let fp_str = metadata.format_provider();
    let format_provider = kernel_string_slice!(fp_str);
    let (has_created_time, created_time_ms) = match metadata.created_time() {
        Some(t) => (true, t),
        None => (false, 0),
    };
    visit_metadata_fields(
        context,
        id,
        name,
        description,
        format_provider,
        has_created_time,
        created_time_ms,
    );
}

// === Snapshot-level computed property FFI ===

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
    use std::collections::HashMap;

    use delta_kernel::engine::default::executor::tokio::TokioMultiThreadExecutor;
    use delta_kernel::engine::default::DefaultEngineBuilder;
    use delta_kernel::object_store::memory::InMemory;
    use delta_kernel::object_store::path::Path;
    use delta_kernel::object_store::ObjectStoreExt as _;
    use delta_kernel::schema::StructType;
    use delta_kernel::table_properties::{ParquetCompression, ParquetWriterConfig};
    use rstest::rstest;
    use serde_json::Value;
    use test_utils::{
        actions_to_string, actions_to_string_catalog_managed, actions_to_string_partitioned,
        actions_to_string_with_metadata, add_commit, add_staged_commit, create_table, TestAction,
        METADATA, METADATA_WITH_FEATURES, METADATA_WITH_TABLE_PROPERTIES,
    };
    use url::Url;

    use super::*;
    use crate::error::{EngineError, KernelError};
    use crate::ffi_test_utils::{
        allocate_err, allocate_str, assert_extern_result_error_with_message, build_snapshot,
        ok_or_panic, recover_string, setup_snapshot,
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

    /// Create an in-memory table with a single version-0 metadata commit, returning the storage,
    /// engine handle, and a snapshot at version 0. The caller is responsible for freeing the
    /// engine and snapshot handles.
    async fn make_engine_and_v0_snapshot(
        path: &str,
    ) -> Result<
        (
            Arc<InMemory>,
            Handle<SharedExternEngine>,
            Handle<SharedSnapshot>,
        ),
        Box<dyn std::error::Error>,
    > {
        let storage = Arc::new(InMemory::new());
        add_commit(
            path,
            storage.as_ref(),
            0,
            actions_to_string(vec![TestAction::Metadata]),
        )
        .await?;
        let engine = engine_to_handle(
            Arc::new(DefaultEngineBuilder::new(storage.clone()).build()),
            allocate_err,
        );
        let snap = unsafe { build_snapshot(kernel_string_slice!(path), engine.shallow_copy()) };
        Ok((storage, engine, snap))
    }

    /// Like [`make_engine_and_v0_snapshot`] but creates a catalog-managed table. The returned
    /// snapshot is built with `max_catalog_version = 0`.
    async fn make_catalog_managed_engine_and_v0_snapshot(
        path: &str,
    ) -> Result<
        (
            Arc<InMemory>,
            Handle<SharedExternEngine>,
            Handle<SharedSnapshot>,
        ),
        Box<dyn std::error::Error>,
    > {
        let storage = Arc::new(InMemory::new());
        add_commit(
            path,
            storage.as_ref(),
            0,
            actions_to_string_catalog_managed(vec![TestAction::Metadata]),
        )
        .await?;
        let engine = engine_to_handle(
            Arc::new(DefaultEngineBuilder::new(storage.clone()).build()),
            allocate_err,
        );
        let snap = unsafe {
            let mut ptr = ok_or_panic(get_snapshot_builder(
                kernel_string_slice!(path),
                engine.shallow_copy(),
            ));
            snapshot_builder_set_max_catalog_version(&mut ptr, 0);
            ok_or_panic(snapshot_builder_build(ptr))
        };
        Ok((storage, engine, snap))
    }

    /// Build a snapshot with version, log tail, and optional max catalog version via the FFI
    /// builder API. Returns the raw `ExternResult` so callers can test error cases.
    unsafe fn snapshot_at_version_with_log_tail(
        path: KernelStringSlice,
        engine: Handle<SharedExternEngine>,
        version: Version,
        log_tail: log_path::LogPathArray,
        max_catalog_version: Option<Version>,
    ) -> ExternResult<Handle<SharedSnapshot>> {
        let mut ptr = ok_or_panic(get_snapshot_builder(path, engine));
        snapshot_builder_set_version(&mut ptr, version);
        ok_or_panic(snapshot_builder_set_log_tail(&mut ptr, log_tail));
        if let Some(mcv) = max_catalog_version {
            snapshot_builder_set_max_catalog_version(&mut ptr, mcv);
        }
        snapshot_builder_build(ptr)
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
        let table_root = "memory:///test_table/";
        let (_, engine, snapshot1) = make_engine_and_v0_snapshot(table_root).await?;

        // Test getting latest snapshot
        let version1 = unsafe { version(snapshot1.shallow_copy()) };
        assert_eq!(version1, 0);

        // Test getting snapshot at version
        let snapshot2 = unsafe {
            let mut ptr = ok_or_panic(get_snapshot_builder(
                kernel_string_slice!(table_root),
                engine.shallow_copy(),
            ));
            snapshot_builder_set_version(&mut ptr, 0);
            ok_or_panic(snapshot_builder_build(ptr))
        };
        let version2 = unsafe { version(snapshot2.shallow_copy()) };
        assert_eq!(version2, 0);

        // Test getting non-existent snapshot
        let snapshot_at_non_existent_version = unsafe {
            let mut ptr = ok_or_panic(get_snapshot_builder(
                kernel_string_slice!(table_root),
                engine.shallow_copy(),
            ));
            snapshot_builder_set_version(&mut ptr, 1);
            snapshot_builder_build(ptr)
        };
        assert_extern_result_error_with_message(snapshot_at_non_existent_version, KernelError::GenericError, Some("Generic delta kernel error: LogSegment end version 0 not the same as the specified end version 1"));

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

    // TODO: (PR #2307) will introduce a helper function for setting up storage, engine.
    // The test will need to refactor to use the helper function.
    #[tokio::test]
    async fn test_snapshot_timestamp_no_ict() -> Result<(), Box<dyn std::error::Error>> {
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
        let snap =
            unsafe { build_snapshot(kernel_string_slice!(table_root), engine.shallow_copy()) };

        let ts = unsafe {
            ok_or_panic(snapshot_timestamp(
                snap.shallow_copy(),
                engine.shallow_copy(),
            ))
        };
        // ICT is not enabled -- falls back to commit file mtime (written "now").
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let two_days_ms = 2 * 24 * 60 * 60 * 1000_i64;
        assert!(
            (now_ms - two_days_ms..=now_ms).contains(&ts),
            "timestamp {ts} not within 2 days of now {now_ms}"
        );

        unsafe { free_snapshot(snap) }
        unsafe { free_engine(engine) }
        Ok(())
    }

    // TODO: (PR #2307) will introduce a helper function for setting up storage, engine.
    // The test will need to refactor to use the helper function.
    #[tokio::test]
    async fn test_snapshot_timestamp_ict_enabled() -> Result<(), Box<dyn std::error::Error>> {
        let storage = Arc::new(InMemory::new());
        let table_root = "memory:///test_table/";

        // create_table with "inCommitTimestamp" in writer_features sets up:
        //   - protocol v3.7 with writerFeatures=["inCommitTimestamp"]
        //   - metadata config: enableInCommitTimestamps=true, enablement version/timestamp
        //   - commitInfo with inCommitTimestamp=1612345678 (fixed test value)
        create_table(
            storage.clone(),
            Url::parse(table_root)?,
            Arc::new(StructType::try_new([]).unwrap()),
            &[],
            true,
            vec![],
            vec!["inCommitTimestamp"],
        )
        .await?;

        let engine = DefaultEngineBuilder::new(storage.clone()).build();
        let engine = engine_to_handle(Arc::new(engine), allocate_err);
        let snap =
            unsafe { build_snapshot(kernel_string_slice!(table_root), engine.shallow_copy()) };

        let ts = unsafe {
            ok_or_panic(snapshot_timestamp(
                snap.shallow_copy(),
                engine.shallow_copy(),
            ))
        };
        assert_eq!(ts, 1612345678_i64);

        unsafe { free_snapshot(snap) }
        unsafe { free_engine(engine) }
        Ok(())
    }

    #[rstest]
    #[case(
        METADATA_WITH_TABLE_PROPERTIES,
        HashMap::from([
            (String::from("delta.appendOnly"), String::from("true")),
            (String::from("custom.key"), String::from("custom_value")),
        ])
    )]
    #[case(METADATA, HashMap::new())]
    #[tokio::test]
    async fn test_visit_metadata_configuration(
        #[case] metadata: &str,
        #[case] expected: HashMap<String, String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let table_root = "memory:///";
        let storage = Arc::new(InMemory::new());
        add_commit(
            table_root,
            storage.as_ref(),
            0,
            actions_to_string_with_metadata(vec![TestAction::Metadata], metadata),
        )
        .await?;

        let engine = DefaultEngineBuilder::new(storage.clone()).build();
        let engine = engine_to_handle(Arc::new(engine), allocate_err);

        let snap =
            unsafe { build_snapshot(kernel_string_slice!(table_root), engine.shallow_copy()) };

        extern "C" fn collect_property(
            engine_context: NullableCvoid,
            key: KernelStringSlice,
            value: KernelStringSlice,
        ) {
            let map =
                unsafe { &mut *(engine_context.unwrap().as_ptr() as *mut HashMap<String, String>) };
            let k = unsafe { String::try_from_slice(&key) }.unwrap();
            let v = unsafe { String::try_from_slice(&value) }.unwrap();
            map.insert(k, v);
        }

        let mut collected: HashMap<String, String> = HashMap::new();
        let ctx = NonNull::new(&mut collected as *mut _ as *mut c_void);
        unsafe { visit_metadata_configuration(snap.shallow_copy(), ctx, collect_property) };

        assert_eq!(collected, expected);

        unsafe { free_snapshot(snap) }
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

        let snapshot =
            unsafe { build_snapshot(kernel_string_slice!(table_root), engine.shallow_copy()) };

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
    // NOTE: We made this a sync test to simulate the expected case: C code calling FFI APIs to
    // build engine without existing tokio runtime.
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
        // Miri cannot execute the zstd-sys C library; use uncompressed so the test runs under Miri.
        #[cfg(miri)]
        {
            let codec = "uncompressed";
            unsafe {
                set_builder_parquet_compression(
                    builder.as_mut().unwrap(),
                    kernel_string_slice!(codec),
                )
            };
        }
        let engine = unsafe { ok_or_panic(builder_build(builder)) };

        let snapshot =
            unsafe { build_snapshot(kernel_string_slice!(table_root), engine.shallow_copy()) };

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

    // Test that set_builder_parquet_compression applies valid codec strings.
    #[cfg(feature = "default-engine-base")]
    #[rstest]
    #[case("snappy", ParquetCompression::Snappy)]
    #[case("SNAPPY", ParquetCompression::Snappy)]
    #[case("uncompressed", ParquetCompression::Uncompressed)]
    #[case("UNCOMPRESSED", ParquetCompression::Uncompressed)]
    #[case("zstd", ParquetCompression::Zstd)]
    fn test_set_builder_parquet_valid_codec(
        #[case] codec: &str,
        #[case] expected: ParquetCompression,
    ) {
        let table_root = "memory:///test_table/";
        let builder_ptr = unsafe {
            ok_or_panic(get_engine_builder(
                kernel_string_slice!(table_root),
                allocate_err,
            ))
        };
        let builder = unsafe { builder_ptr.as_mut().unwrap() };
        unsafe { set_builder_parquet_compression(builder, kernel_string_slice!(codec)) };
        assert_eq!(
            builder.parquet_writer_config,
            ParquetWriterConfig {
                compression: expected
            }
        );
        unsafe { drop(Box::from_raw(builder_ptr)) };
    }

    // Test that set_builder_parquet_compression silently ignores an unrecognized codec string.
    #[cfg(feature = "default-engine-base")]
    #[test]
    fn test_set_builder_parquet_invalid_codec_ignored() {
        let table_root = "memory:///test_table/";
        let builder_ptr = unsafe {
            ok_or_panic(get_engine_builder(
                kernel_string_slice!(table_root),
                allocate_err,
            ))
        };
        let builder = unsafe { builder_ptr.as_mut().unwrap() };
        let codec = "invalid_codec";
        unsafe { set_builder_parquet_compression(builder, kernel_string_slice!(codec)) };
        assert_eq!(
            builder.parquet_writer_config,
            ParquetWriterConfig { compression: ParquetCompression::Zstd },
            "unrecognized codec should leave config unchanged"
        );
        unsafe { drop(Box::from_raw(builder_ptr)) };
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

        let snapshot =
            unsafe { build_snapshot(kernel_string_slice!(table_root), engine.shallow_copy()) };

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
            let mut ptr = ok_or_panic(get_snapshot_builder(
                kernel_string_slice!(table_root),
                engine.shallow_copy(),
            ));
            snapshot_builder_set_version(&mut ptr, 1);
            snapshot_builder_build(ptr)
        };
        assert!(snapshot_at_non_existent_version.is_err());

        unsafe { free_engine(engine) }
        Ok(())
    }

    #[tokio::test]
    async fn test_snapshot_log_tail() -> Result<(), Box<dyn std::error::Error>> {
        let table_root = "memory:///test_table/";
        let (storage, engine, snap) =
            make_catalog_managed_engine_and_v0_snapshot(table_root).await?;
        unsafe { free_snapshot(snap) };
        let commit1 = add_staged_commit(
            table_root,
            storage.as_ref(),
            1,
            actions_to_string(vec![TestAction::Add("path1".into())]),
        )
        .await?;

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
            let mut ptr = ok_or_panic(get_snapshot_builder(
                kernel_string_slice!(table_root),
                engine.shallow_copy(),
            ));
            ok_or_panic(snapshot_builder_set_log_tail(&mut ptr, log_tail.clone()));
            snapshot_builder_set_max_catalog_version(&mut ptr, 1);
            ok_or_panic(snapshot_builder_build(ptr))
        };
        let snapshot_version = unsafe { version(snapshot.shallow_copy()) };
        assert_eq!(snapshot_version, 1);

        // Test for failure when not passing in max_catalog_version
        let invalid_snapshot = unsafe {
            snapshot_at_version_with_log_tail(
                kernel_string_slice!(table_root),
                engine.shallow_copy(),
                1,
                log_tail.clone(),
                None, // max_catalog_version
            )
        };
        assert_extern_result_error_with_message(
            invalid_snapshot,
            KernelError::GenericError,
            Some(concat!(
                "Generic delta kernel error: Staged commits in log_tail require ",
                "max_catalog_version to be set. Use with_max_catalog_version() ",
                "when providing staged commits."
            )),
        );

        // Test getting snapshot at version
        let snapshot2 = unsafe {
            let mut ptr = ok_or_panic(get_snapshot_builder(
                kernel_string_slice!(table_root),
                engine.shallow_copy(),
            ));
            snapshot_builder_set_version(&mut ptr, 1);
            ok_or_panic(snapshot_builder_set_log_tail(&mut ptr, log_tail));
            snapshot_builder_set_max_catalog_version(&mut ptr, 1);
            ok_or_panic(snapshot_builder_build(ptr))
        };
        let snapshot_version = unsafe { version(snapshot2.shallow_copy()) };
        assert_eq!(snapshot_version, 1);

        unsafe { free_snapshot(snapshot) }
        unsafe { free_snapshot(snapshot2) }
        unsafe { free_engine(engine) }
        Ok(())
    }

    #[tokio::test]
    async fn test_builder_from_existing_snapshot_advances_to_latest_and_pinned_version(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let path = "memory:///";
        let (storage, engine, snapshot_at_v0) = make_engine_and_v0_snapshot(path).await?;
        assert_eq!(unsafe { version(snapshot_at_v0.shallow_copy()) }, 0);

        add_commit(
            path,
            storage.as_ref(),
            1,
            actions_to_string(vec![TestAction::Add("file1.parquet".into())]),
        )
        .await?;
        add_commit(
            path,
            storage.as_ref(),
            2,
            actions_to_string(vec![TestAction::Add("file2.parquet".into())]),
        )
        .await?;

        let snapshot_at_v2 = unsafe {
            let ptr = ok_or_panic(get_snapshot_builder_from(
                snapshot_at_v0.shallow_copy(),
                engine.shallow_copy(),
            ));
            ok_or_panic(snapshot_builder_build(ptr))
        };
        assert_eq!(unsafe { version(snapshot_at_v2.shallow_copy()) }, 2);

        let snapshot_at_v1 = unsafe {
            let mut ptr = ok_or_panic(get_snapshot_builder_from(
                snapshot_at_v0.shallow_copy(),
                engine.shallow_copy(),
            ));
            snapshot_builder_set_version(&mut ptr, 1);
            ok_or_panic(snapshot_builder_build(ptr))
        };
        assert_eq!(unsafe { version(snapshot_at_v1.shallow_copy()) }, 1);

        unsafe { free_snapshot(snapshot_at_v2) }
        unsafe { free_snapshot(snapshot_at_v1) }
        unsafe { free_snapshot(snapshot_at_v0) }
        unsafe { free_engine(engine) }
        Ok(())
    }

    #[tokio::test]
    async fn test_builder_from_existing_snapshot_rejects_earlier_version(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let path = "memory:///";
        let (storage, engine, snapshot_at_v0) = make_engine_and_v0_snapshot(path).await?;

        add_commit(
            path,
            storage.as_ref(),
            1,
            actions_to_string(vec![TestAction::Add("file1.parquet".into())]),
        )
        .await?;
        add_commit(
            path,
            storage.as_ref(),
            2,
            actions_to_string(vec![TestAction::Add("file2.parquet".into())]),
        )
        .await?;

        // build a v2 snapshot to use as the base
        let snapshot_at_v2 = unsafe {
            let ptr = ok_or_panic(get_snapshot_builder_from(
                snapshot_at_v0.shallow_copy(),
                engine.shallow_copy(),
            ));
            ok_or_panic(snapshot_builder_build(ptr))
        };
        assert_eq!(unsafe { version(snapshot_at_v2.shallow_copy()) }, 2);

        // pinning to a version older than the hint snapshot is rejected
        let result = unsafe {
            let mut ptr = ok_or_panic(get_snapshot_builder_from(
                snapshot_at_v2.shallow_copy(),
                engine.shallow_copy(),
            ));
            snapshot_builder_set_version(&mut ptr, 1);
            snapshot_builder_build(ptr)
        };
        assert_extern_result_error_with_message(
            result,
            KernelError::GenericError,
            Some("Generic delta kernel error: Requested snapshot version 1 is older than snapshot hint version 2"),
        );

        unsafe { free_snapshot(snapshot_at_v2) }
        unsafe { free_snapshot(snapshot_at_v0) }
        unsafe { free_engine(engine) }
        Ok(())
    }

    #[tokio::test]
    async fn test_snapshot_with_prev_snapshot_and_log_tail(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let path = "memory:///";
        let (storage, engine, snapshot_at_v0) =
            make_catalog_managed_engine_and_v0_snapshot(path).await?;
        assert_eq!(unsafe { version(snapshot_at_v0.shallow_copy()) }, 0);

        // Add staged commit (version 1)
        let commit1 = add_staged_commit(
            path,
            storage.as_ref(),
            1,
            actions_to_string(vec![TestAction::Add("path1.parquet".into())]),
        )
        .await?;

        // Add another staged commit (version 2)
        let commit2 = add_staged_commit(
            path,
            storage.as_ref(),
            2,
            actions_to_string(vec![TestAction::Add("path2.parquet".into())]),
        )
        .await?;

        // Build log tail with both commits
        let commit1_path = format!(
            "{}_delta_log/_staged_commits/{}",
            path,
            commit1.filename().unwrap()
        );
        let commit2_path = format!(
            "{}_delta_log/_staged_commits/{}",
            path,
            commit2.filename().unwrap()
        );
        let log_path1 =
            log_path::FfiLogPath::new(kernel_string_slice!(commit1_path), 123456789, 100);
        let log_path2 =
            log_path::FfiLogPath::new(kernel_string_slice!(commit2_path), 123456790, 101);
        let log_tail = [log_path1, log_path2];
        let log_tail_array = log_path::LogPathArray {
            ptr: log_tail.as_ptr(),
            len: log_tail.len(),
        };

        let snapshot_at_v2 = unsafe {
            let mut ptr = ok_or_panic(get_snapshot_builder_from(
                snapshot_at_v0.shallow_copy(),
                engine.shallow_copy(),
            ));
            ok_or_panic(snapshot_builder_set_log_tail(
                &mut ptr,
                log_tail_array.clone(),
            ));
            snapshot_builder_set_max_catalog_version(&mut ptr, 2);
            ok_or_panic(snapshot_builder_build(ptr))
        };
        assert_eq!(unsafe { version(snapshot_at_v2.shallow_copy()) }, 2);

        let snapshot_at_v1 = unsafe {
            let mut ptr = ok_or_panic(get_snapshot_builder_from(
                snapshot_at_v0.shallow_copy(),
                engine.shallow_copy(),
            ));
            snapshot_builder_set_version(&mut ptr, 1);
            ok_or_panic(snapshot_builder_set_log_tail(&mut ptr, log_tail_array));
            snapshot_builder_set_max_catalog_version(&mut ptr, 2);
            ok_or_panic(snapshot_builder_build(ptr))
        };
        assert_eq!(unsafe { version(snapshot_at_v1.shallow_copy()) }, 1);

        unsafe { free_snapshot(snapshot_at_v2) }
        unsafe { free_snapshot(snapshot_at_v1) }
        unsafe { free_snapshot(snapshot_at_v0) }
        unsafe { free_engine(engine) }
        Ok(())
    }

    #[tokio::test]
    async fn test_builder_from_table_path_builds_latest_version(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let storage = Arc::new(InMemory::new());
        let path = "memory:///";
        add_commit(
            path,
            storage.as_ref(),
            0,
            actions_to_string(vec![TestAction::Metadata]),
        )
        .await?;
        add_commit(
            path,
            storage.as_ref(),
            1,
            actions_to_string(vec![TestAction::Add("file1.parquet".into())]),
        )
        .await?;
        let engine = engine_to_handle(
            Arc::new(DefaultEngineBuilder::new(storage).build()),
            allocate_err,
        );

        let snapshot_at_v1 = unsafe {
            let ptr = ok_or_panic(get_snapshot_builder(
                kernel_string_slice!(path),
                engine.shallow_copy(),
            ));
            ok_or_panic(snapshot_builder_build(ptr))
        };
        assert_eq!(unsafe { version(snapshot_at_v1.shallow_copy()) }, 1);

        unsafe { free_snapshot(snapshot_at_v1) }
        unsafe { free_engine(engine) }
        Ok(())
    }

    #[tokio::test]
    async fn test_free_snapshot_builder_without_building() -> Result<(), Box<dyn std::error::Error>>
    {
        let path = "memory:///";
        let (_, engine, snap) = make_engine_and_v0_snapshot(path).await?;
        unsafe { free_snapshot(snap) };

        let ptr = unsafe {
            ok_or_panic(get_snapshot_builder(
                kernel_string_slice!(path),
                engine.shallow_copy(),
            ))
        };

        unsafe { free_snapshot_builder(ptr) };
        unsafe { free_engine(engine) }
        Ok(())
    }

    // === Shared visitor state and callbacks for protocol/metadata tests ===

    struct ProtocolVisitState {
        min_reader: i32,
        min_writer: i32,
        reader_features: Vec<String>,
        writer_features: Vec<String>,
    }

    impl ProtocolVisitState {
        fn new() -> Self {
            Self {
                min_reader: 0,
                min_writer: 0,
                reader_features: Vec::new(),
                writer_features: Vec::new(),
            }
        }
    }

    extern "C" fn protocol_version_cb(ctx: NullableCvoid, min_reader: i32, min_writer: i32) {
        let state = unsafe { &mut *(ctx.unwrap().as_ptr() as *mut ProtocolVisitState) };
        state.min_reader = min_reader;
        state.min_writer = min_writer;
    }

    extern "C" fn protocol_feature_cb(
        ctx: NullableCvoid,
        is_reader: bool,
        feature: KernelStringSlice,
    ) {
        let state = unsafe { &mut *(ctx.unwrap().as_ptr() as *mut ProtocolVisitState) };
        let name = unsafe { String::try_from_slice(&feature) }.unwrap();
        if is_reader {
            state.reader_features.push(name);
        } else {
            state.writer_features.push(name);
        }
    }

    /// Visit protocol on a snapshot and return the collected state.
    fn collect_protocol_state(snap: &handle::Handle<SharedSnapshot>) -> ProtocolVisitState {
        let proto = unsafe { snapshot_get_protocol(snap.shallow_copy()) };
        let mut state = ProtocolVisitState::new();
        let ctx = NonNull::new(&mut state as *mut ProtocolVisitState as *mut c_void);
        unsafe {
            visit_protocol(
                proto.shallow_copy(),
                ctx,
                protocol_version_cb,
                protocol_feature_cb,
            )
        };
        unsafe { free_protocol(proto) };
        state
    }

    struct MetadataVisitState {
        id: Option<String>,
        name: Option<String>,
        description: Option<String>,
        format_provider: Option<String>,
        has_created_time: bool,
        created_time_ms: i64,
    }

    impl MetadataVisitState {
        fn new() -> Self {
            Self {
                id: None,
                name: None,
                description: None,
                format_provider: None,
                has_created_time: false,
                created_time_ms: 0,
            }
        }
    }

    /// Convert a [`KernelStringSlice`] to a [`String`] (test-only helper).
    fn slice_to_string(slice: KernelStringSlice) -> String {
        unsafe { String::try_from_slice(&slice) }.unwrap()
    }

    extern "C" fn metadata_visit_cb(
        ctx: NullableCvoid,
        id: KernelStringSlice,
        name: OptionalValue<KernelStringSlice>,
        description: OptionalValue<KernelStringSlice>,
        format_provider: KernelStringSlice,
        has_created_time: bool,
        created_time_ms: i64,
    ) {
        let state = unsafe { &mut *(ctx.unwrap().as_ptr() as *mut MetadataVisitState) };
        state.id = Some(slice_to_string(id));
        state.name = match name {
            OptionalValue::Some(s) => Some(slice_to_string(s)),
            OptionalValue::None => None,
        };
        state.description = match description {
            OptionalValue::Some(s) => Some(slice_to_string(s)),
            OptionalValue::None => None,
        };
        state.format_provider = Some(slice_to_string(format_provider));
        state.has_created_time = has_created_time;
        state.created_time_ms = created_time_ms;
    }

    /// Visit metadata on a snapshot and return the collected state.
    fn collect_metadata_state(snap: &handle::Handle<SharedSnapshot>) -> MetadataVisitState {
        let meta = unsafe { snapshot_get_metadata(snap.shallow_copy()) };
        let mut state = MetadataVisitState::new();
        let ctx = NonNull::new(&mut state as *mut MetadataVisitState as *mut c_void);
        unsafe { visit_metadata(meta.shallow_copy(), ctx, metadata_visit_cb) };
        unsafe { free_metadata(meta) };
        state
    }

    // === visit_protocol tests ===

    #[tokio::test]
    async fn test_visit_protocol_legacy() -> Result<(), Box<dyn std::error::Error>> {
        let (engine, snap) = setup_snapshot(METADATA.to_string()).await?;
        let state = collect_protocol_state(&snap);

        assert_eq!(state.min_reader, 1);
        assert_eq!(state.min_writer, 2);
        assert!(state.reader_features.is_empty());
        assert!(state.writer_features.is_empty());

        unsafe { free_snapshot(snap) };
        unsafe { free_engine(engine) };
        Ok(())
    }

    #[tokio::test]
    async fn test_builder_with_nonexistent_path_returns_error(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let storage = Arc::new(InMemory::new());
        let engine = engine_to_handle(
            Arc::new(DefaultEngineBuilder::new(storage).build()),
            allocate_err,
        );

        let result = unsafe {
            let invalid_path = "not a valid url!";
            get_snapshot_builder(kernel_string_slice!(invalid_path), engine.shallow_copy())
        };
        assert_extern_result_error_with_message(
            result,
            KernelError::InvalidTableLocationError,
            None,
        );

        unsafe { free_engine(engine) }
        Ok(())
    }

    #[tokio::test]
    async fn test_builder_at_nonexistent_version_returns_error(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let path = "memory:///";
        let (_, engine, snap) = make_engine_and_v0_snapshot(path).await?;
        unsafe { free_snapshot(snap) };

        let result = unsafe {
            let mut ptr = ok_or_panic(get_snapshot_builder(
                kernel_string_slice!(path),
                engine.shallow_copy(),
            ));
            snapshot_builder_set_version(&mut ptr, 99);
            snapshot_builder_build(ptr)
        };
        assert_extern_result_error_with_message(result, KernelError::GenericError, None);

        unsafe { free_engine(engine) }
        Ok(())
    }

    #[tokio::test]
    async fn test_visit_protocol_with_features() -> Result<(), Box<dyn std::error::Error>> {
        let (engine, snap) = setup_snapshot(METADATA_WITH_FEATURES.to_string()).await?;
        let state = collect_protocol_state(&snap);

        assert_eq!(state.min_reader, 3);
        assert_eq!(state.min_writer, 7);
        assert_eq!(state.reader_features, vec!["columnMapping"]);
        let mut wf = state.writer_features.clone();
        wf.sort();
        assert_eq!(wf, vec!["columnMapping", "domainMetadata", "rowTracking"]);

        unsafe { free_snapshot(snap) };
        unsafe { free_engine(engine) };
        Ok(())
    }

    #[tokio::test]
    async fn test_visit_metadata_default() -> Result<(), Box<dyn std::error::Error>> {
        let (engine, snap) = setup_snapshot(METADATA.to_string()).await?;
        let state = collect_metadata_state(&snap);

        assert_eq!(
            state.id.as_deref(),
            Some("5fba94ed-9794-4965-ba6e-6ee3c0d22af9")
        );
        assert!(
            state.name.is_none(),
            "name should be None for default metadata"
        );
        assert!(
            state.description.is_none(),
            "description should be None for default metadata"
        );
        assert_eq!(state.format_provider.as_deref(), Some("parquet"));
        assert!(state.has_created_time);
        assert_eq!(state.created_time_ms, 1587968585495);

        unsafe { free_snapshot(snap) };
        unsafe { free_engine(engine) };
        Ok(())
    }

    #[tokio::test]
    async fn test_visit_metadata_with_name() -> Result<(), Box<dyn std::error::Error>> {
        let (engine, snap) = setup_snapshot(METADATA_WITH_FEATURES.to_string()).await?;
        let state = collect_metadata_state(&snap);

        assert_eq!(
            state.id.as_deref(),
            Some("deadbeef-1234-5678-abcd-000000000000")
        );
        assert_eq!(state.name.as_deref(), Some("test_table"));
        assert!(state.description.is_none(), "description should be None");
        assert_eq!(state.format_provider.as_deref(), Some("parquet"));
        assert!(state.has_created_time);
        assert_eq!(state.created_time_ms, 1234567890000);

        unsafe { free_snapshot(snap) };
        unsafe { free_engine(engine) };
        Ok(())
    }

    #[tokio::test]
    async fn test_visit_metadata_with_description() -> Result<(), Box<dyn std::error::Error>> {
        let metadata_with_desc = concat!(
            r#"{"commitInfo":{"timestamp":1587968586154,"operation":"WRITE","operationParameters":{},"isBlindAppend":true}}"#,
            "\n",
            r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
            "\n",
            r#"{"metaData":{"id":"5fba94ed-9794-4965-ba6e-6ee3c0d22af9","name":"my_table","description":"A test table","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[]}","partitionColumns":[],"configuration":{},"createdTime":1587968585495}}"#,
        );
        let (engine, snap) = setup_snapshot(metadata_with_desc.to_string()).await?;
        let state = collect_metadata_state(&snap);

        assert_eq!(state.name.as_deref(), Some("my_table"));
        assert_eq!(state.description.as_deref(), Some("A test table"));
        assert_eq!(state.format_provider.as_deref(), Some("parquet"));
        assert!(state.has_created_time);

        unsafe { free_snapshot(snap) };
        unsafe { free_engine(engine) };
        Ok(())
    }

    #[tokio::test]
    async fn test_visit_metadata_without_created_time() -> Result<(), Box<dyn std::error::Error>> {
        let metadata_no_time = concat!(
            r#"{"commitInfo":{"timestamp":1587968586154,"operation":"WRITE","operationParameters":{},"isBlindAppend":true}}"#,
            "\n",
            r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
            "\n",
            r#"{"metaData":{"id":"5fba94ed-9794-4965-ba6e-6ee3c0d22af9","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[]}","partitionColumns":[],"configuration":{}}}"#,
        );
        let (engine, snap) = setup_snapshot(metadata_no_time.to_string()).await?;
        let state = collect_metadata_state(&snap);

        assert_eq!(
            state.id.as_deref(),
            Some("5fba94ed-9794-4965-ba6e-6ee3c0d22af9")
        );
        assert!(!state.has_created_time);
        assert_eq!(state.created_time_ms, 0);

        unsafe { free_snapshot(snap) };
        unsafe { free_engine(engine) };
        Ok(())
    }
}
