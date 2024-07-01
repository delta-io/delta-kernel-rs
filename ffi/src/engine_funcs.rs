//! Generate functions to perform the "normal" engine operations

use std::sync::Arc;

use delta_kernel::{schema::Schema, DeltaResult, FileDataReadResultIterator};
use delta_kernel_ffi_macros::handle_descriptor;
use tracing::debug;
use url::Url;

use crate::{
    scan::SharedSchema, ExclusiveEngineData, ExternEngine, ExternResult, IntoExternResult,
    KernelStringSlice, NullableCvoid, SharedExternEngine, TryFromStringSlice,
};

use super::handle::Handle;

#[repr(C)]
pub struct FileMeta {
    pub path: KernelStringSlice,
    pub last_modified: i64,
    pub size: usize,
}

// Intentionally opaque to the engine.
pub struct FileReadResultIterator {
    // Box -> Wrap its unsized content this struct is fixed-size with thin pointers.
    // Item = Box<dyn EngineData>, see above, Vec<bool> -> can become a KernelBoolSlice
    data: FileDataReadResultIterator,

    // Also keep a reference to the external engine for its error allocator.
    // Parquet and Json handlers don't hold any reference to the tokio reactor, so the iterator
    // terminates early if the last engine goes out of scope.
    engine: Arc<dyn ExternEngine>,
}

#[handle_descriptor(target=FileReadResultIterator, mutable=true, sized=true)]
pub struct ExclusiveFileReadResultIterator;

impl Drop for FileReadResultIterator {
    fn drop(&mut self) {
        debug!("dropping FileReadResultIterator");
    }
}

/// Call the engine back with the next `EngingeData` batch read by Parquet/Json handler. The
/// _engine_ "owns" the data that is passed into the `engine_visitor`, since it is allocated by the
/// `Engine` being used for log-replay. If the engine wants the kernel to free this data, it _must_
/// call [`free_engine_data`] on it.
///
/// # Safety
///
/// The iterator must be valid (returned by [`read_parquet_file`]) and not yet freed by
/// [`free_read_result_iter`]. The visitor function pointer must be non-null.
#[no_mangle]
pub unsafe extern "C" fn read_result_next(
    mut data: Handle<ExclusiveFileReadResultIterator>,
    engine_context: NullableCvoid,
    engine_visitor: extern "C" fn(
        engine_context: NullableCvoid,
        engine_data: Handle<ExclusiveEngineData>,
    ),
) -> ExternResult<bool> {
    let iter = unsafe { data.as_mut() };
    read_result_next_impl(iter, engine_context, engine_visitor)
        .into_extern_result(iter.engine.error_allocator())
}

fn read_result_next_impl(
    iter: &mut FileReadResultIterator,
    engine_context: NullableCvoid,
    engine_visitor: extern "C" fn(
        engine_context: NullableCvoid,
        engine_data: Handle<ExclusiveEngineData>,
    ),
) -> DeltaResult<bool> {
    if let Some(data) = iter.data.next().transpose()? {
        (engine_visitor)(engine_context, data.into());
        Ok(true)
    } else {
        Ok(false)
    }
}

/// Free the memory from the passed read result iterator
/// # Safety
///
/// Caller is responsible for (at most once) passing a valid pointer returned by a call to
/// [`read_parquet_file`].
#[no_mangle]
pub unsafe extern "C" fn free_read_result_iter(data: Handle<ExclusiveFileReadResultIterator>) {
    data.drop_handle();
}

/// Use the specified engine's [`delta_kernel::ParquetHandler`] to read the specified file.
///
/// # Safety
/// Caller is responsible for calling with a valid `ExternEngineHandle` and `FileMeta`
#[no_mangle]
pub unsafe extern "C" fn read_parquet_file(
    engine: Handle<SharedExternEngine>,
    file: &FileMeta,
    physical_schema: Handle<SharedSchema>,
) -> ExternResult<Handle<ExclusiveFileReadResultIterator>> {
    let engine = unsafe { engine.clone_as_arc() };
    let physical_schema = unsafe { physical_schema.clone_as_arc() };
    let path = unsafe { String::try_from_slice(&file.path) };
    let res = read_parquet_file_impl(engine.clone(), path, file, physical_schema);
    res.into_extern_result(&engine.as_ref())
}

fn read_parquet_file_impl(
    extern_engine: Arc<dyn ExternEngine>,
    path: DeltaResult<String>,
    file: &FileMeta,
    physical_schema: Arc<Schema>,
) -> DeltaResult<Handle<ExclusiveFileReadResultIterator>> {
    let engine = extern_engine.engine();
    let parquet_handler = engine.get_parquet_handler();
    let location = Url::parse(&path?)?;
    let delta_fm = delta_kernel::FileMeta {
        location,
        last_modified: file.last_modified,
        size: file.size,
    };
    let data = parquet_handler.read_parquet_files(&[delta_fm], physical_schema, None)?;
    let res = Box::new(FileReadResultIterator {
        data,
        engine: extern_engine,
    });
    Ok(res.into())
}
