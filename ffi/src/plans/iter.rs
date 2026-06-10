//! This module contains types needed to implement Engine-owned iterators returned from plan
//! execution.
//!
//! Each `C*Iterator` type contains an opaque engine `state` pointer, a `next` function pointer that
//! yields one iterator item per call, and a `free` function pointer that releases the iterator
//! state.
//!
//! # `next()` protocol
//!
//! Each `next` callback writes its result as an `OptionalValue<EngineExecResult<T>>` (mirroring
//! Rust's `Option<Result<T>>`) into a caller-provided out pointer. This allows us to implement
//! `DeltaResultIterator<T>`:
//!     - The outer Option represents whether iteration is complete (`None` = done)
//!     - The inner [`EngineExecResult`] represents the item (`Success`) or an engine-side error
//!       (`Failure`)
//!
//! # Iterator Memory Management
//! There are 3 aspects to iterator memory management:
//!
//! 1. Iterator State - owned by the engine and freed by the kernel via the iterator's own `free`
//!    function pointer. Each `Ffi*Iter` adapter invokes `free` exactly once when it is dropped.
//!
//! 2. Iterator Items - each data item (e.g EngineData, Bytes, FileMeta) yielded by the iterator has
//!    its own separate lifetime and may have its own memory management rules. However we generally
//!    use FFI_ArrowArray as a carrier for the item (which internally manages its own release
//!    callback).
//!
//! 3. Errors - each [`EngineExecError`](crate::error::EngineExecError) carries an engine-allocated
//!    `ExclusiveRustString` message handle. Kernel takes ownership of the message and frees it when
//!    converting the error into a kernel error (via `From<EngineExecError> for Error`).
//!
//! # Safety
//! The engine is responsible for ensuring that all `state`, `next`, and `free` pointers
//! are safe to send between threads.

use std::mem::MaybeUninit;

use bytes::Bytes;
use delta_kernel::arrow::array::ffi::{self as arrow_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use delta_kernel::arrow::array::{
    self as arrow_array, Array, BinaryArray, Int64Array, StringArray, StructArray, UInt64Array,
};
use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Fields};
use delta_kernel::{DeltaResult, EngineData, Error};
use url::Url;

use crate::error::EngineExecResult;
use crate::handle::Handle;
use crate::{ExclusiveEngineData, NullableCvoid, OptionalValue};

// ============================================================================
// repr(C) iterator types
// ============================================================================

/// Function pointer an engine iterator uses to yield its next item into `out`.
///
/// See "`next()` protocol" in the module docs.
///
/// # Safety
///
/// The engine MUST fully initialize `*out` before returning. Kernel reads
/// `*out` via [`MaybeUninit::assume_init`](std::mem::MaybeUninit::assume_init), so returning
/// without writing it will cause undefined behavior.
pub type CIterNextFn<T> =
    extern "C" fn(state: NullableCvoid, out: *mut OptionalValue<EngineExecResult<T>>);

/// Function pointer that releases an engine iterator's `state`. Invoked exactly once by kernel once
/// iteration is complete.
pub type CIterFreeFn = extern "C" fn(state: NullableCvoid);

/// An engine-owned iterator of [`EngineData`] batches.
///
/// See module level docs for memory management and safety requirements.
#[repr(C)]
pub struct CEngineDataIterator {
    pub state: NullableCvoid,
    pub next: CIterNextFn<Handle<ExclusiveEngineData>>,
    pub free: CIterFreeFn,
}

/// An engine-owned iterator of byte buffers.
///
/// See module level docs for memory management and safety requirements.
///
/// Each byte array is transferred across the FFI boundary as an [`FFI_ArrowArray`].
/// Invocation of `next` MUST yield an Arrow array that is a `BinaryArray` containing a single
/// row of non-null bytes. Kernel assumes the schema is [`ArrowDataType::Binary`] (the schema is NOT
/// passed along through FFI).
///
/// FFI_ArrowArray serves as a convenient container for receiving bytes because it internally
/// manages its own memory release callback.
///
/// TODO: ArrowDataType::Binary uses i32 offsets, so each byte array is limited to 2 GiB. If this is
/// a problem, we need to support ArrowDataType::LargeBinary instead.
#[repr(C)]
pub struct CBytesIterator {
    pub state: NullableCvoid,
    pub next: CIterNextFn<FFI_ArrowArray>,
    pub free: CIterFreeFn,
}

/// An engine-owned iterator of [`FileMeta`](delta_kernel::FileMeta) batches.
///
/// See module level docs for memory management and safety requirements.
///
/// Similar to `CBytesIterator`, CFileMetaIterator uses `FFI_ArrowArray` as a convenient container
/// for passing FileMeta entries across the FFI boundary. Each `next` invocation MUST yield a batch
/// of one or more FileMeta rows as a `StructArray` whose fields are
/// `{location: Utf8, last_modified: Int64, size: UInt64}`, all non-null. Kernel assumes this fixed
/// schema when converting into FileMeta (the schema is NOT passed along through FFI).
/// Empty or otherwise invalid batches surface as errors and permanently terminate iteration
/// (kernel will not call `next` again).
#[repr(C)]
pub struct CFileMetaIterator {
    pub state: NullableCvoid,
    pub next: CIterNextFn<FFI_ArrowArray>,
    pub free: CIterFreeFn,
}

// ============================================================================
// Rust Iterator adapters
// ============================================================================

/// An abstraction for invoking an engine iterator's `free` callback exactly once, when dropped.
///
/// Embedding this in every `Ffi*Iter` adapter provides a single shared mechanism for ensuring the
/// iterator is dropped correctly.
pub(crate) struct IterCleanup {
    state: NullableCvoid,
    free: CIterFreeFn,
}

impl IterCleanup {
    fn new(state: NullableCvoid, free: CIterFreeFn) -> Self {
        Self { state, free }
    }

    /// The opaque engine state pointer, forwarded to each `next` call.
    fn state(&self) -> NullableCvoid {
        self.state
    }
}

impl Drop for IterCleanup {
    fn drop(&mut self) {
        (self.free)(self.state);
    }
}

/// Provides the Rust `Iterator` trait for [`EngineData`] batches on top of a
/// [`CEngineDataIterator`].
pub(crate) struct FfiEngineDataIter {
    next: CIterNextFn<Handle<ExclusiveEngineData>>,
    cleanup: IterCleanup,
}

impl FfiEngineDataIter {
    pub(crate) fn new(iter: CEngineDataIterator) -> Self {
        let CEngineDataIterator { state, next, free } = iter;
        Self {
            next,
            cleanup: IterCleanup::new(state, free),
        }
    }
}

impl Iterator for FfiEngineDataIter {
    type Item = DeltaResult<Box<dyn EngineData>>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut out = MaybeUninit::uninit();
        (self.next)(self.cleanup.state(), out.as_mut_ptr());
        // SAFETY: the engine contract (see `CIterNextFn`) requires `next` to initialize `out`
        // before returning.
        match unsafe { out.assume_init() } {
            OptionalValue::None => None,
            OptionalValue::Some(EngineExecResult::Failure(err)) => Some(Err(err.into())),
            OptionalValue::Some(EngineExecResult::Success(handle)) => {
                // into_inner transfers ownership of the EngineData to kernel, and kernel
                // will free it when the yielded EngineData is dropped.
                Some(Ok(unsafe { handle.into_inner() }))
            }
        }
    }
}

// # Safety
// The engine must ensure that all raw pointers sent are Send-safe (see module level docs)
unsafe impl Send for FfiEngineDataIter {}

/// Provides the Rust `Iterator` trait for [`Bytes`] buffers on top of a [`CBytesIterator`].
pub(crate) struct FfiBytesIter {
    next: CIterNextFn<FFI_ArrowArray>,
    cleanup: IterCleanup,
}

impl FfiBytesIter {
    pub(crate) fn new(iter: CBytesIterator) -> Self {
        let CBytesIterator { state, next, free } = iter;
        Self {
            next,
            cleanup: IterCleanup::new(state, free),
        }
    }

    fn arrow_array_to_bytes(array: FFI_ArrowArray) -> DeltaResult<Bytes> {
        let schema = FFI_ArrowSchema::try_from(&ArrowDataType::Binary)?;
        let array_data = unsafe { arrow_ffi::from_ffi(array, &schema) }?;
        let array = arrow_array::make_array(array_data);

        let Some(binary) = array.as_any().downcast_ref::<BinaryArray>() else {
            return Err(Error::generic(format!(
                "CBytesIterator must yield BinaryArray, got {:?}",
                array.data_type()
            )));
        };
        if binary.len() != 1 {
            return Err(Error::generic(format!(
                "CBytesIterator array must contain exactly one row, got {}",
                binary.len()
            )));
        }
        if binary.is_null(0) {
            return Err(Error::generic("CBytesIterator array row must not be null"));
        }

        // TODO: this copies the payload bytes, but could be made zero-copy by
        // using `Bytes::from_owner` on a custom struct that implements `AsRef<[u8]>`
        Ok(Bytes::copy_from_slice(binary.value(0)))
    }
}

impl Iterator for FfiBytesIter {
    type Item = DeltaResult<Bytes>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut out = MaybeUninit::uninit();
        (self.next)(self.cleanup.state(), out.as_mut_ptr());
        // SAFETY: the engine contract (see `CIterNextFn`) requires `next` to initialize `out`
        // before returning.
        match unsafe { out.assume_init() } {
            OptionalValue::None => None,
            OptionalValue::Some(EngineExecResult::Failure(err)) => Some(Err(err.into())),
            OptionalValue::Some(EngineExecResult::Success(array)) => {
                Some(Self::arrow_array_to_bytes(array))
            }
        }
    }
}

// # Safety
// The engine must ensure that all raw pointers sent are Send-safe (see module level docs)
unsafe impl Send for FfiBytesIter {}

/// Provides the Rust `Iterator` trait for [`FileMeta`] entries on top of a
/// [`CFileMetaIterator`].
///
/// Buffers each engine-yielded batch and surfaces one row per `Iterator::next` call. Permanantly
/// terminates if the engine returns None or a batch fails to decode. Errors surfaced directly by
/// the engine `next` callback are considered transient and do NOT terminate iteration.
pub(crate) struct FfiFileMetaIter {
    next: CIterNextFn<FFI_ArrowArray>,
    cleanup: IterCleanup,
    // Decoded rows from the most recent engine batch
    buffer: std::vec::IntoIter<delta_kernel::FileMeta>,
    terminated: bool,
}

impl FfiFileMetaIter {
    pub(crate) fn new(iter: CFileMetaIterator) -> Self {
        let CFileMetaIterator { state, next, free } = iter;
        Self {
            next,
            cleanup: IterCleanup::new(state, free),
            buffer: Vec::new().into_iter(),
            terminated: false,
        }
    }

    /// The fixed Arrow type expected by [`CFileMetaIterator`]: a non-null struct of
    /// `{location: Utf8, last_modified: Int64, size: UInt64}`, all non-null.
    fn arrow_schema() -> DeltaResult<FFI_ArrowSchema> {
        let schema = ArrowDataType::Struct(Fields::from(vec![
            ArrowField::new("location", ArrowDataType::Utf8, false),
            ArrowField::new("last_modified", ArrowDataType::Int64, false),
            ArrowField::new("size", ArrowDataType::UInt64, false),
        ]));
        Ok(FFI_ArrowSchema::try_from(&schema)?)
    }

    /// Decodes a single engine batch into a `Vec<FileMeta>`.
    fn arrow_array_to_file_metas(
        array: FFI_ArrowArray,
    ) -> DeltaResult<Vec<delta_kernel::FileMeta>> {
        let schema = Self::arrow_schema()?;
        let array_data = unsafe { arrow_ffi::from_ffi(array, &schema) }?;
        let array = arrow_array::make_array(array_data);

        let Some(struct_array) = array.as_any().downcast_ref::<StructArray>() else {
            return Err(Error::generic(format!(
                "CFileMetaIterator must yield StructArray, got {:?}",
                array.data_type()
            )));
        };
        if struct_array.is_empty() {
            return Err(Error::generic(
                "CFileMetaIterator batch must contain at least one row",
            ));
        }

        // The fixed schema guarantees three columns in this order, all non-null. `from_ffi`
        // above already validated the per-column data types match what we synthesized, so the
        // downcasts cannot fail; the `ok_or_else` arms exist only to keep the converter
        // panic-free.
        let location_col = struct_array
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Error::generic("CFileMetaIterator: location column is not Utf8"))?;
        let last_modified_col = struct_array
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                Error::generic("CFileMetaIterator: last_modified column is not Int64")
            })?;
        let size_col = struct_array
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| Error::generic("CFileMetaIterator: size column is not UInt64"))?;

        // The fixed schema declares every column non-null; reject any batch that violates that
        // contract up front so the per-row decode below can safely call `value(i)`.
        if location_col.null_count() != 0
            || last_modified_col.null_count() != 0
            || size_col.null_count() != 0
        {
            return Err(Error::generic(
                "CFileMetaIterator batch must not contain null fields",
            ));
        }

        (0..struct_array.len())
            .map(|i| {
                Ok(delta_kernel::FileMeta {
                    location: Url::parse(location_col.value(i))?,
                    last_modified: last_modified_col.value(i),
                    size: size_col.value(i),
                })
            })
            .collect()
    }
}

impl Iterator for FfiFileMetaIter {
    type Item = DeltaResult<delta_kernel::FileMeta>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.terminated {
                return None;
            }

            if let Some(meta) = self.buffer.next() {
                return Some(Ok(meta));
            }

            // Buffer drained and iteration is still live; pull the next batch from the engine.
            let mut out = MaybeUninit::uninit();
            (self.next)(self.cleanup.state(), out.as_mut_ptr());
            // SAFETY: the engine contract (see `CIterNextFn`) requires `next` to initialize `out`
            // before returning.
            match unsafe { out.assume_init() } {
                OptionalValue::None => {
                    self.terminated = true;
                    return None;
                }
                OptionalValue::Some(EngineExecResult::Failure(err)) => {
                    return Some(Err(err.into()))
                }
                OptionalValue::Some(EngineExecResult::Success(array)) => {
                    match Self::arrow_array_to_file_metas(array) {
                        Ok(batch) => self.buffer = batch.into_iter(),
                        Err(e) => {
                            // Decoder errors (including empty batches) are fatal
                            self.terminated = true;
                            return Some(Err(e));
                        }
                    }
                }
            }
        }
    }
}

// # Safety
// The engine must ensure that all raw pointers sent are Send-safe (see module level docs)
unsafe impl Send for FfiFileMetaIter {}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::ffi::c_void;
    use std::ptr::NonNull;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    use delta_kernel::arrow::array::{
        ffi as arrow_ffi, ArrayRef, BinaryArray, Int32Array, Int64Array, RecordBatch, StringArray,
        StructArray, UInt64Array,
    };
    use delta_kernel::arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    };
    use delta_kernel::engine::arrow_data::ArrowEngineData;

    use super::*;
    use crate::error::{EngineExecError, KernelError};
    use crate::ExclusiveRustString;

    // === Shared Test Helpers ===

    struct MockIter<T> {
        items: Mutex<VecDeque<OptionalValue<EngineExecResult<T>>>>,
        cleanup_called: Arc<AtomicBool>,
    }

    impl<T> Drop for MockIter<T> {
        fn drop(&mut self) {
            let previously_set = self.cleanup_called.swap(true, Ordering::SeqCst);
            assert!(!previously_set, "MockIter dropped more than once");
        }
    }

    // Makes a mock iterator with the given items and returns the iterator + a cleanup flag for
    // verifying that the iterator was dropped properly.
    //
    // Can be dropped using `free_mock_iter`.
    fn make_mock_iter<T>(
        items: Vec<OptionalValue<EngineExecResult<T>>>,
    ) -> (Box<MockIter<T>>, Arc<AtomicBool>) {
        let cleanup_called = Arc::new(AtomicBool::new(false));
        let iter = Box::new(MockIter {
            items: Mutex::new(VecDeque::from(items)),
            cleanup_called: Arc::clone(&cleanup_called),
        });
        (iter, cleanup_called)
    }

    extern "C" fn free_mock_iter<T>(state: NullableCvoid) {
        let _ = unsafe { Box::from_raw(state.unwrap().as_ptr() as *mut MockIter<T>) };
    }

    // Builds an engine execution error whose message is a kernel-allocated `ExclusiveRustString`
    // handle (mirroring the engine downcalling `allocate_kernel_string`).
    fn make_exec_error(etype: KernelError, message: &str) -> EngineExecError {
        let message: Handle<ExclusiveRustString> = Box::new(message.to_string()).into();
        EngineExecError { etype, message }
    }

    // === FfiEngineDataIter Test ===

    /// Helper for building each yielded EngineData batch
    fn make_data_handle(rows: usize) -> Handle<ExclusiveEngineData> {
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "x",
            ArrowDataType::Int32,
            true,
        )]));
        let values: Vec<i32> = (0..rows as i32).collect();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values))]).unwrap();
        let data: Box<dyn EngineData> = Box::new(ArrowEngineData::new(batch));
        data.into()
    }

    #[test]
    fn ffi_engine_data_iter_drains_and_runs_cleanup() {
        let (mock_iter, cleanup_called) = make_mock_iter::<Handle<ExclusiveEngineData>>(vec![
            OptionalValue::Some(EngineExecResult::Success(make_data_handle(3))),
            OptionalValue::Some(EngineExecResult::Failure(make_exec_error(
                KernelError::GenericError,
                "boom",
            ))),
            OptionalValue::Some(EngineExecResult::Success(make_data_handle(5))),
        ]);
        let state = NonNull::new(Box::into_raw(mock_iter) as *mut c_void);

        extern "C" fn next(
            state: NullableCvoid,
            out: *mut OptionalValue<EngineExecResult<Handle<ExclusiveEngineData>>>,
        ) {
            let mock_iter = unsafe {
                &*(state.unwrap().as_ptr() as *const MockIter<Handle<ExclusiveEngineData>>)
            };
            let item = mock_iter
                .items
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(OptionalValue::None);
            unsafe { out.write(item) };
        }

        let mut iter = FfiEngineDataIter::new(CEngineDataIterator {
            state,
            next,
            free: free_mock_iter::<Handle<ExclusiveEngineData>>,
        });

        let first = iter.next().expect("first item").expect("first ok");
        assert_eq!(first.len(), 3);

        let Some(Err(err)) = iter.next() else {
            panic!("second item should be Err");
        };
        assert!(
            err.to_string().contains("boom"),
            "engine error message must propagate, got {err}"
        );

        let third = iter.next().expect("third item").expect("third ok");
        assert_eq!(third.len(), 5);

        assert!(iter.next().is_none(), "iterator should be exhausted");
        assert!(
            !cleanup_called.load(Ordering::SeqCst),
            "cleanup must not run before the iterator is dropped"
        );

        drop(iter);
        assert!(
            cleanup_called.load(Ordering::SeqCst),
            "cleanup must run when the iterator is dropped"
        );
    }

    // === FfiBytesIter Test ===

    // Helper for building each yielded Arrow bytes array
    fn make_binary_ffi_array(payload: &[u8]) -> FFI_ArrowArray {
        let array = BinaryArray::from(vec![payload]);
        let (ffi_array, _ffi_schema) = arrow_ffi::to_ffi(&array.into_data()).unwrap();
        ffi_array
    }

    #[test]
    fn ffi_bytes_iter_drains_and_runs_cleanup() {
        let (mock_iter, cleanup_called) = make_mock_iter::<FFI_ArrowArray>(vec![
            OptionalValue::Some(EngineExecResult::Success(make_binary_ffi_array(b"hello"))),
            OptionalValue::Some(EngineExecResult::Failure(make_exec_error(
                KernelError::GenericError,
                "boom",
            ))),
            OptionalValue::Some(EngineExecResult::Success(make_binary_ffi_array(b"world!"))),
        ]);
        let state = NonNull::new(Box::into_raw(mock_iter) as *mut c_void);

        extern "C" fn next(
            state: NullableCvoid,
            out: *mut OptionalValue<EngineExecResult<FFI_ArrowArray>>,
        ) {
            let mock_iter =
                unsafe { &*(state.unwrap().as_ptr() as *const MockIter<FFI_ArrowArray>) };
            let item = mock_iter
                .items
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(OptionalValue::None);
            unsafe { out.write(item) };
        }

        let mut iter = FfiBytesIter::new(CBytesIterator {
            state,
            next,
            free: free_mock_iter::<FFI_ArrowArray>,
        });

        let first = iter.next().expect("first item").expect("first ok");
        assert_eq!(first.as_ref(), b"hello");

        let Some(Err(err)) = iter.next() else {
            panic!("second item should be Err");
        };
        assert!(
            err.to_string().contains("boom"),
            "engine error message must propagate, got {err}"
        );

        let third = iter.next().expect("third item").expect("third ok");
        assert_eq!(third.as_ref(), b"world!");

        assert!(iter.next().is_none(), "iterator should be exhausted");
        assert!(!cleanup_called.load(Ordering::SeqCst));

        drop(iter);
        assert!(cleanup_called.load(Ordering::SeqCst));
    }

    // === FfiFileMetaIter Test ===

    /// Helper for building a yielded FileMeta batch. `rows` may be empty to construct a malformed
    /// batch for negative tests.
    fn make_file_meta_ffi_array(rows: &[(&str, i64, u64)]) -> FFI_ArrowArray {
        let locations: Vec<&str> = rows.iter().map(|(l, _, _)| *l).collect();
        let last_modifieds: Vec<i64> = rows.iter().map(|(_, t, _)| *t).collect();
        let sizes: Vec<u64> = rows.iter().map(|(_, _, s)| *s).collect();
        let struct_array = StructArray::from(vec![
            (
                Arc::new(ArrowField::new("location", ArrowDataType::Utf8, false)),
                Arc::new(StringArray::from(locations)) as ArrayRef,
            ),
            (
                Arc::new(ArrowField::new(
                    "last_modified",
                    ArrowDataType::Int64,
                    false,
                )),
                Arc::new(Int64Array::from(last_modifieds)) as ArrayRef,
            ),
            (
                Arc::new(ArrowField::new("size", ArrowDataType::UInt64, false)),
                Arc::new(UInt64Array::from(sizes)) as ArrayRef,
            ),
        ]);
        let (ffi_array, _ffi_schema) = arrow_ffi::to_ffi(&struct_array.into_data()).unwrap();
        ffi_array
    }

    extern "C" fn file_meta_next(
        state: NullableCvoid,
        out: *mut OptionalValue<EngineExecResult<FFI_ArrowArray>>,
    ) {
        let mock_iter = unsafe { &*(state.unwrap().as_ptr() as *const MockIter<FFI_ArrowArray>) };
        let item = mock_iter
            .items
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(OptionalValue::None);
        unsafe { out.write(item) };
    }

    /// Exercises batched yields with a multi-row batch, a transient engine error (which does not
    /// terminate iteration), and a single-row batch. Verifies the Rust-side adapter buffers each
    /// batch and surfaces one row per `Iterator::next` call.
    #[test]
    fn ffi_file_meta_iter_drains_and_runs_cleanup() {
        let (mock_iter, cleanup_called) = make_mock_iter::<FFI_ArrowArray>(vec![
            OptionalValue::Some(EngineExecResult::Success(make_file_meta_ffi_array(&[
                ("file:///a.parquet", 1, 100),
                ("file:///b.parquet", 2, 200),
            ]))),
            OptionalValue::Some(EngineExecResult::Failure(make_exec_error(
                KernelError::GenericError,
                "boom",
            ))),
            OptionalValue::Some(EngineExecResult::Success(make_file_meta_ffi_array(&[(
                "file:///c.parquet",
                3,
                300,
            )]))),
        ]);
        let state = NonNull::new(Box::into_raw(mock_iter) as *mut c_void);

        let mut iter = FfiFileMetaIter::new(CFileMetaIterator {
            state,
            next: file_meta_next,
            free: free_mock_iter::<FFI_ArrowArray>,
        });

        // First batch contributes two rows.
        let first = iter.next().expect("first item").expect("first ok");
        assert_eq!(first.location.as_str(), "file:///a.parquet");
        assert_eq!(first.last_modified, 1);
        assert_eq!(first.size, 100);

        let second = iter.next().expect("second item").expect("second ok");
        assert_eq!(second.location.as_str(), "file:///b.parquet");
        assert_eq!(second.last_modified, 2);
        assert_eq!(second.size, 200);

        // After draining the first batch the adapter pulls the transient engine error.
        let Some(Err(err)) = iter.next() else {
            panic!("third item should be Err");
        };
        assert!(
            err.to_string().contains("boom"),
            "engine error message must propagate, got {err}"
        );

        // Transient engine errors do not terminate iteration; the fourth batch is still returned.
        let fourth = iter.next().expect("fourth item").expect("fourth ok");
        assert_eq!(fourth.location.as_str(), "file:///c.parquet");
        assert_eq!(fourth.last_modified, 3);
        assert_eq!(fourth.size, 300);

        assert!(iter.next().is_none(), "iterator should be exhausted");
        assert!(!cleanup_called.load(Ordering::SeqCst));

        drop(iter);
        assert!(cleanup_called.load(Ordering::SeqCst));
    }

    /// Verifies that an empty batch surfaces as an error and permanently terminates iteration
    #[test]
    fn ffi_file_meta_iter_rejects_empty_batch_and_terminates() {
        let (mock_iter, cleanup_called) = make_mock_iter::<FFI_ArrowArray>(vec![
            OptionalValue::Some(EngineExecResult::Success(make_file_meta_ffi_array(&[]))),
            OptionalValue::Some(EngineExecResult::Success(make_file_meta_ffi_array(&[(
                "file:///trap.parquet",
                99,
                999,
            )]))),
        ]);
        let state = NonNull::new(Box::into_raw(mock_iter) as *mut c_void);

        // Helper fn for checking the queue length from `state` pointer
        let queue_len = || {
            let mock_iter =
                unsafe { &*(state.unwrap().as_ptr() as *const MockIter<FFI_ArrowArray>) };
            mock_iter.items.lock().unwrap().len()
        };

        let mut iter = FfiFileMetaIter::new(CFileMetaIterator {
            state,
            next: file_meta_next,
            free: free_mock_iter::<FFI_ArrowArray>,
        });

        let first = iter.next().expect("first item");
        assert!(first.is_err(), "empty batch must surface as Err");

        // The final entry is still queued.
        assert_eq!(queue_len(), 1, "final entry must remain unconsumed");

        // Further polls return `None` without re-invoking the engine.
        assert!(iter.next().is_none(), "iterator should be terminated");
        assert!(iter.next().is_none(), "iterator should stay terminated");
        assert_eq!(
            queue_len(),
            1,
            "final entry must still be unconsumed after extra polls"
        );

        assert!(!cleanup_called.load(Ordering::SeqCst));
        drop(iter);
        assert!(cleanup_called.load(Ordering::SeqCst));
    }
}
