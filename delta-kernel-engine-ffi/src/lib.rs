use std::collections::HashMap;
use std::default::Default;
use std::sync::Arc;

use url::Url;

use delta_kernel::DeltaResult;
use delta_kernel_engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel_engine::default::DefaultEngine;
use delta_kernel_ffi::error::{AllocateErrorFn, ExternResult};
use delta_kernel_ffi::handle::Handle;
use delta_kernel_ffi::{
    engine_to_handle, error::IntoExternResult, unwrap_and_parse_path_as_url, KernelStringSlice,
    SharedExternEngine, TryFromStringSlice,
};

/// A builder that allows setting options on the `Engine` before actually building it
pub struct EngineBuilder {
    url: Url,
    allocate_fn: AllocateErrorFn,
    options: HashMap<String, String>,
}

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
#[no_mangle]
pub unsafe extern "C" fn get_engine_builder(
    path: KernelStringSlice,
    allocate_error: AllocateErrorFn,
) -> ExternResult<*mut EngineBuilder> {
    let url = unsafe { unwrap_and_parse_path_as_url(path) };
    get_engine_builder_impl(url, allocate_error).into_extern_result(&allocate_error)
}

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
#[no_mangle]
pub unsafe extern "C" fn get_default_engine(
    path: KernelStringSlice,
    allocate_error: AllocateErrorFn,
) -> ExternResult<Handle<SharedExternEngine>> {
    let url = unsafe { unwrap_and_parse_path_as_url(path) };
    get_default_default_engine_impl(url, allocate_error).into_extern_result(&allocate_error)
}

// get the default version of the default engine :)
fn get_default_default_engine_impl(
    url: DeltaResult<Url>,
    allocate_error: AllocateErrorFn,
) -> DeltaResult<Handle<SharedExternEngine>> {
    get_default_engine_impl(url?, Default::default(), allocate_error)
}

fn get_default_engine_impl(
    url: Url,
    options: HashMap<String, String>,
    allocate_error: AllocateErrorFn,
) -> DeltaResult<Handle<SharedExternEngine>> {
    let engine = DefaultEngine::<TokioBackgroundExecutor>::try_new(
        &url,
        options,
        Arc::new(TokioBackgroundExecutor::new()),
    );
    Ok(engine_to_handle(Arc::new(engine?), allocate_error))
}
