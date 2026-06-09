//! Custom [`StorageHandler`] driven by an engine-supplied vtable.
//!
//! All five storage operations (`list_from`, `read_files`, `copy_atomic`,
//! `put`, `head`) upcall into [`CustomStorageCallbacks`]. Listing and bulk
//! reads return engine-managed iterator states that the kernel pumps via the
//! `*_iter_next` callbacks; the remaining operations are single synchronous
//! upcalls reporting a bare `u32` status code.
//!
//! ## Status-code convention
//!
//! Callbacks that report a bare `u32` use `0` for success. A few non-zero
//! codes carry typed meaning so the commit path can detect conflicts and
//! missing files; everything else maps to a generic error:
//! - [`STORAGE_FILE_NOT_FOUND`] -> [`Error::FileNotFound`]
//! - [`STORAGE_FILE_ALREADY_EXISTS`] -> [`Error::FileAlreadyExists`]

use std::ffi::c_void;
use std::sync::Arc;

use bytes::Bytes;
use delta_kernel::{DeltaResult, Error, FileMeta as KernelFileMeta, FileSlice, StorageHandler};
use url::Url;

use crate::KernelStringSlice;

/// Success status for storage callbacks that report a bare `u32`.
pub const STORAGE_OK: u32 = 0;
/// Status code meaning "file does not exist" (maps to [`Error::FileNotFound`]).
pub const STORAGE_FILE_NOT_FOUND: u32 = 2;
/// Status code meaning "file already exists" (maps to [`Error::FileAlreadyExists`]).
pub const STORAGE_FILE_ALREADY_EXISTS: u32 = 3;

/// Function-pointer table the connector supplies to drive the storage handler.
///
/// `engine_state` is opaque to the kernel and threaded as the first argument
/// to every callback. All pointers are required and must stay valid for the
/// lifetime of the resulting engine. `free_engine_state` runs exactly once
/// when the last handler reference (handler + any live iterators) drops.
#[repr(C)]
pub struct CustomStorageCallbacks {
    /// Engine-managed opaque pointer threaded through every callback.
    pub engine_state: *mut c_void,
    /// Begin a recursive listing greater than `path`. On success the engine
    /// writes a non-null iterator state the kernel pumps via `list_iter_next`.
    pub list_from: extern "C" fn(
        engine_state: *mut c_void,
        path: KernelStringSlice,
        out: *mut CustomFileIterResult,
    ),
    /// Begin reading the given byte ranges. On success the engine writes a
    /// non-null iterator state the kernel pumps via `bytes_iter_next`. The
    /// engine yields one chunk per requested file, in request order.
    pub read_files: extern "C" fn(
        engine_state: *mut c_void,
        files: *const FileSliceFFI,
        files_len: usize,
        out: *mut CustomBytesIterResult,
    ),
    /// Atomically copy `src` to `dst`, failing if `dst` exists.
    pub copy_atomic: extern "C" fn(
        engine_state: *mut c_void,
        src: KernelStringSlice,
        dst: KernelStringSlice,
        out_error: *mut u32,
    ),
    /// Write `data` to `path`. If `overwrite` is false and the file exists,
    /// the engine must report [`STORAGE_FILE_ALREADY_EXISTS`].
    pub put: extern "C" fn(
        engine_state: *mut c_void,
        path: KernelStringSlice,
        data: *const u8,
        data_len: usize,
        overwrite: bool,
        out_error: *mut u32,
    ),
    /// Fetch metadata for a single file. If the file is missing, the engine
    /// must report [`STORAGE_FILE_NOT_FOUND`].
    pub head: extern "C" fn(
        engine_state: *mut c_void,
        path: KernelStringSlice,
        out: *mut CustomFileMetaResult,
    ),
    /// Pump a listing iterator returned by `list_from`.
    pub list_iter_next: extern "C" fn(
        engine_state: *mut c_void,
        iter_state: *mut c_void,
        out: *mut CustomFileMetaResult,
    ),
    /// Release a listing iterator state.
    pub list_iter_free: extern "C" fn(engine_state: *mut c_void, iter_state: *mut c_void),
    /// Pump a bytes iterator returned by `read_files`.
    pub bytes_iter_next: extern "C" fn(
        engine_state: *mut c_void,
        iter_state: *mut c_void,
        out: *mut CustomBytesResult,
    ),
    /// Release a bytes iterator state.
    pub bytes_iter_free: extern "C" fn(engine_state: *mut c_void, iter_state: *mut c_void),
    /// Free the engine state when the last handler reference drops.
    pub free_engine_state: extern "C" fn(engine_state: *mut c_void),
}

// SAFETY: the vtable is a struct of plain function pointers plus an opaque
// engine-managed pointer the kernel never dereferences. Thread safety of the
// underlying state is the engine's concern -- mirrors `ExternEngineVtable`.
unsafe impl Send for CustomStorageCallbacks {}
unsafe impl Sync for CustomStorageCallbacks {}

/// Result of [`CustomStorageCallbacks::list_from`].
#[repr(C)]
pub struct CustomFileIterResult {
    /// Engine-managed iterator state, non-null on success and ignored on error.
    pub iter_state: *mut c_void,
    /// `0` (success) or a status code.
    pub error: u32,
}

/// Result of [`CustomStorageCallbacks::read_files`].
#[repr(C)]
pub struct CustomBytesIterResult {
    /// Engine-managed iterator state, non-null on success and ignored on error.
    pub iter_state: *mut c_void,
    /// `0` (success) or a status code.
    pub error: u32,
}

/// A single file's metadata, returned by `head` and the listing iterator.
#[repr(C)]
pub struct CustomFileMetaResult {
    /// Fully-qualified URL of the file. Borrowed for the duration of the
    /// upcall; the kernel copies it before returning.
    pub path: KernelStringSlice,
    /// Last-modified time in milliseconds since the unix epoch.
    pub last_modified: i64,
    /// File size in bytes.
    pub size: u64,
    /// Set to `true` by the iterator when no more entries remain.
    pub done: bool,
    /// `0` (success) or a status code.
    pub error: u32,
}

/// A single byte chunk returned by the bytes iterator.
#[repr(C)]
pub struct CustomBytesResult {
    /// Pointer to the chunk bytes, valid for the duration of the upcall.
    pub data: *const u8,
    /// Length of the chunk in bytes.
    pub data_len: usize,
    /// Set to `true` by the iterator when no more chunks remain.
    pub done: bool,
    /// `0` (success) or a status code. [`STORAGE_FILE_NOT_FOUND`] indicates
    /// the file at the current position is missing without ending iteration.
    pub error: u32,
}

/// A byte range to read from a file, marshaled across the FFI boundary.
#[repr(C)]
pub struct FileSliceFFI {
    /// Fully-qualified URL of the file, borrowed for the duration of the upcall.
    pub path: KernelStringSlice,
    /// Inclusive start offset. `start == end == 0` means "whole file".
    pub start_offset: u64,
    /// Exclusive end offset. `start == end == 0` means "whole file".
    pub end_offset: u64,
}

/// Shared state for the storage handler. The engine state is freed exactly
/// once, when the last `Arc` reference (held by the handler plus any live
/// iterators) drops.
pub(crate) struct StorageHandlerState {
    pub(crate) callbacks: CustomStorageCallbacks,
}

impl Drop for StorageHandlerState {
    fn drop(&mut self) {
        (self.callbacks.free_engine_state)(self.callbacks.engine_state);
    }
}

/// [`StorageHandler`] that forwards every operation to a connector-supplied
/// vtable.
pub struct CustomStorageHandler {
    state: Arc<StorageHandlerState>,
}

impl CustomStorageHandler {
    pub(crate) fn new(callbacks: CustomStorageCallbacks) -> Self {
        Self {
            state: Arc::new(StorageHandlerState { callbacks }),
        }
    }
}

impl StorageHandler for CustomStorageHandler {
    fn list_from(
        &self,
        path: &Url,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<KernelFileMeta>>>> {
        // SAFETY: `path.as_str()` borrows `path`, valid for this synchronous upcall.
        let slice = unsafe { KernelStringSlice::new_unsafe(path.as_str()) };
        let mut result = CustomFileIterResult {
            iter_state: std::ptr::null_mut(),
            error: STORAGE_OK,
        };
        (self.state.callbacks.list_from)(self.state.callbacks.engine_state, slice, &mut result);
        if result.error != STORAGE_OK {
            return Err(Error::generic(format!(
                "CustomStorageHandler::list_from signalled error code {}",
                result.error
            )));
        }
        if result.iter_state.is_null() {
            return Err(Error::generic(
                "CustomStorageHandler::list_from returned a null iterator state on success",
            ));
        }
        Ok(Box::new(CustomFileMetaIter {
            iter_state: result.iter_state,
            handler: self.state.clone(),
            done: false,
        }))
    }

    fn read_files(
        &self,
        files: Vec<FileSlice>,
    ) -> DeltaResult<Box<dyn Iterator<Item = DeltaResult<Bytes>>>> {
        // Stash the URL strings so the borrowed `KernelStringSlice`s stay valid
        // through the synchronous upcall.
        let url_strs: Vec<String> = files.iter().map(|(u, _)| u.as_str().to_owned()).collect();
        let slices: Vec<FileSliceFFI> = files
            .iter()
            .zip(url_strs.iter())
            .map(|((_, range), url)| {
                let (start, end) = match range {
                    Some(r) => (r.start, r.end),
                    None => (0u64, 0u64),
                };
                FileSliceFFI {
                    // SAFETY: `url` outlives this synchronous upcall.
                    path: unsafe { KernelStringSlice::new_unsafe(url.as_str()) },
                    start_offset: start,
                    end_offset: end,
                }
            })
            .collect();
        let mut result = CustomBytesIterResult {
            iter_state: std::ptr::null_mut(),
            error: STORAGE_OK,
        };
        (self.state.callbacks.read_files)(
            self.state.callbacks.engine_state,
            slices.as_ptr(),
            slices.len(),
            &mut result,
        );
        if result.error != STORAGE_OK {
            return Err(Error::generic(format!(
                "CustomStorageHandler::read_files signalled error code {}",
                result.error
            )));
        }
        if result.iter_state.is_null() {
            return Err(Error::generic(
                "CustomStorageHandler::read_files returned a null iterator state on success",
            ));
        }
        Ok(Box::new(CustomBytesIter {
            iter_state: result.iter_state,
            handler: self.state.clone(),
            done: false,
            urls: url_strs,
            cursor: 0,
        }))
    }

    fn copy_atomic(&self, src: &Url, dst: &Url) -> DeltaResult<()> {
        // SAFETY: both `&str`s outlive this synchronous upcall.
        let src_slice = unsafe { KernelStringSlice::new_unsafe(src.as_str()) };
        let dst_slice = unsafe { KernelStringSlice::new_unsafe(dst.as_str()) };
        let mut error = STORAGE_OK;
        (self.state.callbacks.copy_atomic)(
            self.state.callbacks.engine_state,
            src_slice,
            dst_slice,
            &mut error,
        );
        map_status(error, || format!("copy_atomic to {dst}"), dst.as_str())
    }

    fn put(&self, path: &Url, data: Bytes, overwrite: bool) -> DeltaResult<()> {
        // SAFETY: `path.as_str()` outlives this synchronous upcall.
        let slice = unsafe { KernelStringSlice::new_unsafe(path.as_str()) };
        let mut error = STORAGE_OK;
        (self.state.callbacks.put)(
            self.state.callbacks.engine_state,
            slice,
            data.as_ptr(),
            data.len(),
            overwrite,
            &mut error,
        );
        map_status(error, || format!("put to {path}"), path.as_str())
    }

    fn head(&self, path: &Url) -> DeltaResult<KernelFileMeta> {
        // SAFETY: `path.as_str()` outlives this synchronous upcall.
        let slice = unsafe { KernelStringSlice::new_unsafe(path.as_str()) };
        let mut result = CustomFileMetaResult {
            // The engine overwrites `path` before returning; we copy it out
            // below before any engine-side free could run.
            path: unsafe { KernelStringSlice::new_unsafe("") },
            last_modified: 0,
            size: 0,
            done: false,
            error: STORAGE_OK,
        };
        (self.state.callbacks.head)(self.state.callbacks.engine_state, slice, &mut result);
        if result.error != STORAGE_OK {
            if result.error == STORAGE_FILE_NOT_FOUND {
                return Err(Error::file_not_found(path.as_str()));
            }
            return Err(Error::generic(format!(
                "CustomStorageHandler::head signalled error code {} for {}",
                result.error, path
            )));
        }
        file_meta_from_result(&result)
    }
}

/// Map a bare status code from a single-shot storage upcall to a typed error.
fn map_status(error: u32, context: impl FnOnce() -> String, path: &str) -> DeltaResult<()> {
    match error {
        STORAGE_OK => Ok(()),
        STORAGE_FILE_NOT_FOUND => Err(Error::file_not_found(path)),
        STORAGE_FILE_ALREADY_EXISTS => Err(Error::FileAlreadyExists(path.to_string())),
        other => Err(Error::generic(format!(
            "CustomStorageHandler::{} signalled error code {other}",
            context()
        ))),
    }
}

/// Build a [`KernelFileMeta`] from a populated [`CustomFileMetaResult`],
/// copying the engine-owned path string out before it can be freed.
fn file_meta_from_result(result: &CustomFileMetaResult) -> DeltaResult<KernelFileMeta> {
    // SAFETY: the engine guarantees `path` is valid UTF-8 for the duration of
    // the upcall; we copy it into an owned `Url` here.
    let returned: &str = unsafe { crate::TryFromStringSlice::try_from_slice(&result.path) }?;
    // Raw URL parsing: `try_parse_uri` is for table-root URIs and forces
    // trailing-slash/directory checks that don't apply to file URLs.
    let location = Url::parse(returned).map_err(|e| {
        Error::generic(format!(
            "CustomStorageHandler returned an unparseable URL {returned:?}: {e}"
        ))
    })?;
    Ok(KernelFileMeta {
        location,
        last_modified: result.last_modified,
        size: result.size,
    })
}

/// Iterator over engine-supplied [`KernelFileMeta`] values, pumping
/// `list_iter_next` until the engine signals done.
struct CustomFileMetaIter {
    iter_state: *mut c_void,
    handler: Arc<StorageHandlerState>,
    done: bool,
}

// SAFETY: same justification as `CustomStorageCallbacks`.
unsafe impl Send for CustomFileMetaIter {}

impl Drop for CustomFileMetaIter {
    fn drop(&mut self) {
        if !self.iter_state.is_null() {
            (self.handler.callbacks.list_iter_free)(
                self.handler.callbacks.engine_state,
                self.iter_state,
            );
        }
    }
}

impl Iterator for CustomFileMetaIter {
    type Item = DeltaResult<KernelFileMeta>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let mut result = CustomFileMetaResult {
            path: unsafe { KernelStringSlice::new_unsafe("") },
            last_modified: 0,
            size: 0,
            done: false,
            error: STORAGE_OK,
        };
        (self.handler.callbacks.list_iter_next)(
            self.handler.callbacks.engine_state,
            self.iter_state,
            &mut result,
        );
        if result.error != STORAGE_OK {
            self.done = true;
            return Some(Err(Error::generic(format!(
                "CustomStorageHandler::list_iter_next signalled error code {}",
                result.error
            ))));
        }
        if result.done {
            self.done = true;
            return None;
        }
        match file_meta_from_result(&result) {
            Ok(meta) => Some(Ok(meta)),
            Err(e) => {
                self.done = true;
                Some(Err(e))
            }
        }
    }
}

/// Iterator over engine-supplied byte chunks (one per requested file, in
/// order), pumping `bytes_iter_next` until the engine signals done.
struct CustomBytesIter {
    iter_state: *mut c_void,
    handler: Arc<StorageHandlerState>,
    done: bool,
    /// URLs of the files in the original `read_files` request, used to build
    /// informative `FileNotFound` errors on per-file failures.
    urls: Vec<String>,
    /// Index of the next file the engine yields against; advances per result.
    cursor: usize,
}

// SAFETY: same justification as `CustomStorageCallbacks`.
unsafe impl Send for CustomBytesIter {}

impl Drop for CustomBytesIter {
    fn drop(&mut self) {
        if !self.iter_state.is_null() {
            (self.handler.callbacks.bytes_iter_free)(
                self.handler.callbacks.engine_state,
                self.iter_state,
            );
        }
    }
}

impl Iterator for CustomBytesIter {
    type Item = DeltaResult<Bytes>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let mut result = CustomBytesResult {
            data: std::ptr::null(),
            data_len: 0,
            done: false,
            error: STORAGE_OK,
        };
        (self.handler.callbacks.bytes_iter_next)(
            self.handler.callbacks.engine_state,
            self.iter_state,
            &mut result,
        );
        if result.error != STORAGE_OK {
            // A missing file at this position does not end iteration -- the
            // kernel may still want subsequent files. Yield FileNotFound and
            // advance the cursor; any other code is fatal.
            let url_for_pos = self.urls.get(self.cursor).cloned().unwrap_or_default();
            self.cursor += 1;
            if result.error == STORAGE_FILE_NOT_FOUND {
                return Some(Err(Error::file_not_found(url_for_pos)));
            }
            self.done = true;
            return Some(Err(Error::generic(format!(
                "CustomStorageHandler::bytes_iter_next signalled error code {} for {}",
                result.error, url_for_pos
            ))));
        }
        if result.done {
            self.done = true;
            return None;
        }
        // Copy out: the engine may free the buffer once this upcall returns.
        let bytes = if result.data.is_null() || result.data_len == 0 {
            Bytes::new()
        } else {
            // SAFETY: the engine guarantees the buffer is valid for `data_len`
            // bytes during this synchronous upcall.
            let slice = unsafe { std::slice::from_raw_parts(result.data, result.data_len) };
            Bytes::copy_from_slice(slice)
        };
        self.cursor += 1;
        Some(Ok(bytes))
    }
}

#[cfg(test)]
mod tests {
    //! Pure-Rust shim backing the storage vtable with an in-memory map, so the
    //! five storage operations can be exercised end-to-end without a connector.
    use std::collections::BTreeMap;
    use std::ffi::c_void;
    use std::sync::Mutex;

    use super::*;

    struct StorageShim {
        files: Mutex<BTreeMap<String, Vec<u8>>>,
    }

    struct ListShim {
        entries: Vec<(String, u64)>,
        cursor: usize,
    }

    struct BytesShim {
        chunks: Vec<Vec<u8>>,
        cursor: usize,
    }

    fn shim_ref(engine_state: *mut c_void) -> &'static StorageShim {
        unsafe { &*(engine_state as *const StorageShim) }
    }

    extern "C" fn shim_put(
        engine_state: *mut c_void,
        path: KernelStringSlice,
        data: *const u8,
        data_len: usize,
        overwrite: bool,
        out_error: *mut u32,
    ) {
        let shim = shim_ref(engine_state);
        let key: String = unsafe { crate::TryFromStringSlice::try_from_slice(&path) }.unwrap();
        let bytes = unsafe { std::slice::from_raw_parts(data, data_len) }.to_vec();
        let mut files = shim.files.lock().unwrap();
        if !overwrite && files.contains_key(&key) {
            unsafe { *out_error = STORAGE_FILE_ALREADY_EXISTS };
            return;
        }
        files.insert(key, bytes);
        unsafe { *out_error = STORAGE_OK };
    }

    extern "C" fn shim_head(
        engine_state: *mut c_void,
        path: KernelStringSlice,
        out: *mut CustomFileMetaResult,
    ) {
        let shim = shim_ref(engine_state);
        let key: String = unsafe { crate::TryFromStringSlice::try_from_slice(&path) }.unwrap();
        match shim.files.lock().unwrap().get(&key) {
            Some(bytes) => unsafe {
                // Echo the caller-owned path slice back; the kernel copies it
                // before this call returns.
                (*out).path = path;
                (*out).size = bytes.len() as u64;
                (*out).last_modified = 0;
                (*out).error = STORAGE_OK;
            },
            None => unsafe { (*out).error = STORAGE_FILE_NOT_FOUND },
        }
    }

    extern "C" fn shim_list_from(
        engine_state: *mut c_void,
        path: KernelStringSlice,
        out: *mut CustomFileIterResult,
    ) {
        let shim = shim_ref(engine_state);
        let from: String = unsafe { crate::TryFromStringSlice::try_from_slice(&path) }.unwrap();
        let entries: Vec<(String, u64)> = shim
            .files
            .lock()
            .unwrap()
            .iter()
            .filter(|(k, _)| k.as_str() > from.as_str())
            .map(|(k, v)| (k.clone(), v.len() as u64))
            .collect();
        let iter = Box::new(ListShim { entries, cursor: 0 });
        unsafe {
            *out = CustomFileIterResult {
                iter_state: Box::into_raw(iter) as *mut c_void,
                error: STORAGE_OK,
            };
        }
    }

    extern "C" fn shim_list_next(
        _engine_state: *mut c_void,
        iter_state: *mut c_void,
        out: *mut CustomFileMetaResult,
    ) {
        let iter = unsafe { &mut *(iter_state as *mut ListShim) };
        if iter.cursor >= iter.entries.len() {
            unsafe { (*out).done = true };
            return;
        }
        let (path, size) = &iter.entries[iter.cursor];
        unsafe {
            // The path string lives in `iter` until `list_iter_free`, which
            // outlives this call.
            (*out).path = KernelStringSlice::new_unsafe(path.as_str());
            (*out).size = *size;
            (*out).last_modified = 0;
            (*out).done = false;
            (*out).error = STORAGE_OK;
        }
        iter.cursor += 1;
    }

    extern "C" fn shim_list_free(_engine_state: *mut c_void, iter_state: *mut c_void) {
        if !iter_state.is_null() {
            unsafe { drop(Box::from_raw(iter_state as *mut ListShim)) };
        }
    }

    extern "C" fn shim_read_files(
        engine_state: *mut c_void,
        files: *const FileSliceFFI,
        files_len: usize,
        out: *mut CustomBytesIterResult,
    ) {
        let shim = shim_ref(engine_state);
        let slices = unsafe { std::slice::from_raw_parts(files, files_len) };
        let map = shim.files.lock().unwrap();
        let chunks: Vec<Vec<u8>> = slices
            .iter()
            .map(|s| {
                let key: String =
                    unsafe { crate::TryFromStringSlice::try_from_slice(&s.path) }.unwrap();
                map.get(&key).cloned().unwrap_or_default()
            })
            .collect();
        let iter = Box::new(BytesShim { chunks, cursor: 0 });
        unsafe {
            *out = CustomBytesIterResult {
                iter_state: Box::into_raw(iter) as *mut c_void,
                error: STORAGE_OK,
            };
        }
    }

    extern "C" fn shim_bytes_next(
        _engine_state: *mut c_void,
        iter_state: *mut c_void,
        out: *mut CustomBytesResult,
    ) {
        let iter = unsafe { &mut *(iter_state as *mut BytesShim) };
        if iter.cursor >= iter.chunks.len() {
            unsafe { (*out).done = true };
            return;
        }
        let chunk = &iter.chunks[iter.cursor];
        unsafe {
            (*out).data = chunk.as_ptr();
            (*out).data_len = chunk.len();
            (*out).done = false;
            (*out).error = STORAGE_OK;
        }
        iter.cursor += 1;
    }

    extern "C" fn shim_bytes_free(_engine_state: *mut c_void, iter_state: *mut c_void) {
        if !iter_state.is_null() {
            unsafe { drop(Box::from_raw(iter_state as *mut BytesShim)) };
        }
    }

    extern "C" fn shim_copy_atomic(
        _engine_state: *mut c_void,
        _src: KernelStringSlice,
        _dst: KernelStringSlice,
        out_error: *mut u32,
    ) {
        unsafe { *out_error = STORAGE_OK };
    }

    extern "C" fn shim_free_engine(engine_state: *mut c_void) {
        if !engine_state.is_null() {
            unsafe { drop(Box::from_raw(engine_state as *mut StorageShim)) };
        }
    }

    fn make_handler() -> CustomStorageHandler {
        let shim = Box::new(StorageShim {
            files: Mutex::new(BTreeMap::new()),
        });
        let callbacks = CustomStorageCallbacks {
            engine_state: Box::into_raw(shim) as *mut c_void,
            list_from: shim_list_from,
            read_files: shim_read_files,
            copy_atomic: shim_copy_atomic,
            put: shim_put,
            head: shim_head,
            list_iter_next: shim_list_next,
            list_iter_free: shim_list_free,
            bytes_iter_next: shim_bytes_next,
            bytes_iter_free: shim_bytes_free,
            free_engine_state: shim_free_engine,
        };
        CustomStorageHandler::new(callbacks)
    }

    #[test]
    fn storage_handler_round_trips_put_head_list_read() {
        let handler = make_handler();
        let a = Url::parse("memory:///t/a").unwrap();
        let b = Url::parse("memory:///t/b").unwrap();
        handler
            .put(&a, Bytes::from_static(b"alpha"), false)
            .unwrap();
        handler
            .put(&b, Bytes::from_static(b"bravo"), false)
            .unwrap();

        // head
        let meta = handler.head(&a).unwrap();
        assert_eq!(meta.location, a);
        assert_eq!(meta.size, 5);

        // list_from the directory returns both, sorted.
        let dir = Url::parse("memory:///t/").unwrap();
        let listed: Vec<KernelFileMeta> = handler
            .list_from(&dir)
            .unwrap()
            .collect::<DeltaResult<Vec<_>>>()
            .unwrap();
        let names: Vec<&str> = listed.iter().map(|f| f.location.as_str()).collect();
        assert_eq!(names, vec![a.as_str(), b.as_str()]);

        // read_files returns each file's bytes in request order.
        let bytes: Vec<Bytes> = handler
            .read_files(vec![(a.clone(), None), (b.clone(), None)])
            .unwrap()
            .collect::<DeltaResult<Vec<_>>>()
            .unwrap();
        assert_eq!(
            bytes,
            vec![Bytes::from_static(b"alpha"), Bytes::from_static(b"bravo")]
        );
    }

    #[test]
    fn storage_handler_maps_typed_status_codes() {
        let handler = make_handler();
        let p = Url::parse("memory:///t/dup").unwrap();
        handler.put(&p, Bytes::from_static(b"x"), false).unwrap();
        // Re-put without overwrite -> FileAlreadyExists.
        let err = handler
            .put(&p, Bytes::from_static(b"y"), false)
            .expect_err("duplicate put should fail");
        assert!(matches!(err, Error::FileAlreadyExists(_)), "got: {err}");
        // head on a missing file -> FileNotFound.
        let missing = Url::parse("memory:///t/missing").unwrap();
        let err = handler.head(&missing).expect_err("head should fail");
        assert!(matches!(err, Error::FileNotFound(_)), "got: {err}");
    }
}
