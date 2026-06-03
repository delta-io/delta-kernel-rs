//! This module contains types needed to implement Engine-owned iterators returned from plan
//! execution.
//!
//! Each `C*Iterator` type contains an opaque engine `state` pointer with a `next` function
//! pointer that yields one iterator item per call.
//!
//! //! # `next()` protocol
//!
//! Each `next` callback returns `OptionalValue<ExternResult<T>>`, mirroring Rust's
//! `Option<Result<T>>`. This allows us to implement `DeltaResultIterator<T>`:
//!     - The outer Option represents whether iteration is complete
//!     - The inner Result represents the DeltaResult yielded by the iterator
//!
//! # Iterator Memory Management
//! There are 3 aspects to iterator memory management:
//!
//! 1. Iterator State - owned by the engine and must be freed by the kernel when it is done with it.
//!    The kernel performs this cleanup by using the `free` function pointer associated with the
//!    containing PlanResult (see [`super::result::CPlanResultWrapper`]). Each `Ffi*Iter` adapter
//!    takes care of this by embedding `PlanResultCleanup`, which will invoke the `free` function
//!    pointer when the adapter is dropped.
//!
//! 2. Iterator Items - each data item (e.g EngineData, Bytes, FileMeta) yielded by the iterator has
//!    its own separate lifetime and may have its own memory management rules. However we generally
//!    use FFI_ArrowArray as a carrier for the item (which internally manages its own release
//!    callback).
//!
//! 3. Item Wrappers (i.e OptionalValue, ExternResult, EngineError) - each yielded item is wrapped
//!    in an OptionalValue + ExternResult which must also be freed. We expect these to be released
//!    when the entire iterator is dropped (through the same `PlanResultCleanup` guard that frees
//!    the iterator state).
//!
//! # Safety
//! The engine is responsible for ensuring that all `state`, `next`, and `free` pointers
//! are safe to send between threads.

use bytes::Bytes;
use delta_kernel::arrow::array::ffi::{self as arrow_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use delta_kernel::arrow::array::{
    self as arrow_array, Array, BinaryArray, Int64Array, StringArray, StructArray, UInt64Array,
};
use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Fields};
use delta_kernel::{DeltaResult, EngineData, Error};
use url::Url;

use super::result::PlanResultCleanup;
use crate::error::ExternResult;
use crate::handle::Handle;
use crate::plans::result::engine_error_to_kernel;
use crate::{ExclusiveEngineData, NullableCvoid, OptionalValue};

// ============================================================================
// repr(C) iterator types
// ============================================================================

/// An engine-owned iterator of [`EngineData`] batches.
///
/// See module level docs for memory management and safety requirements.
#[repr(C)]
pub struct CEngineDataIterator {
    pub state: NullableCvoid,
    pub next: extern "C" fn(
        state: NullableCvoid,
    ) -> OptionalValue<ExternResult<Handle<ExclusiveEngineData>>>,
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
    pub next: extern "C" fn(state: NullableCvoid) -> OptionalValue<ExternResult<FFI_ArrowArray>>,
}

/// An engine-owned iterator of [`FileMeta`](delta_kernel::FileMeta) entries.
///
/// See module level docs for memory management and safety requirements.
///
/// Similar to `CBytesIterator`, CFileMetaIterator uses `FFI_ArrowArray`` as a convenient continer
/// for passing FileMeta entries across the FFI boundary. Each FileMeta is passed as an
/// a `StructArray` of length 1 whose fields are `{location: Utf8, last_modified: Int64, size:
/// UInt64}`, all non-null. Kernel assumes this fixed schema when converting into FileMeta (the
/// schema is NOT passed along through FFI).
#[repr(C)]
pub struct CFileMetaIterator {
    pub state: NullableCvoid,
    pub next: extern "C" fn(state: NullableCvoid) -> OptionalValue<ExternResult<FFI_ArrowArray>>,
}

// ============================================================================
// Rust Iterator adapters
// ============================================================================
//
// Each adapter wraps a `C*Iterator`, implementing the Rust `Iterator` trait, and embeds a
// `PlanResultCleanup` guard for freeing all associated engine resources when the adapter is
// dropped.

/// Provides the Rust `Iterator` trait for [`EngineData`] batches on top of a
/// [`CEngineDataIterator`].
pub(crate) struct FfiEngineDataIter {
    iter: CEngineDataIterator,
    _cleanup: PlanResultCleanup,
}

impl FfiEngineDataIter {
    pub(crate) fn new(iter: CEngineDataIterator, cleanup: PlanResultCleanup) -> Self {
        Self {
            iter,
            _cleanup: cleanup,
        }
    }
}

impl Iterator for FfiEngineDataIter {
    type Item = DeltaResult<Box<dyn EngineData>>;

    fn next(&mut self) -> Option<Self::Item> {
        match (self.iter.next)(self.iter.state) {
            OptionalValue::None => None,
            OptionalValue::Some(ExternResult::Err(err)) => Some(Err(engine_error_to_kernel(err))),
            OptionalValue::Some(ExternResult::Ok(handle)) => {
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
    iter: CBytesIterator,
    _cleanup: PlanResultCleanup,
}

impl FfiBytesIter {
    pub(crate) fn new(iter: CBytesIterator, cleanup: PlanResultCleanup) -> Self {
        Self {
            iter,
            _cleanup: cleanup,
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
        match (self.iter.next)(self.iter.state) {
            OptionalValue::None => None,
            OptionalValue::Some(ExternResult::Err(err)) => Some(Err(engine_error_to_kernel(err))),
            OptionalValue::Some(ExternResult::Ok(array)) => Some(Self::arrow_array_to_bytes(array)),
        }
    }
}

// # Safety
// The engine must ensure that all raw pointers sent are Send-safe (see module level docs)
unsafe impl Send for FfiBytesIter {}

/// Provides the Rust `Iterator` trait for [`FileMeta`] entries on top of a
/// [`CFileMetaIterator`].
pub(crate) struct FfiFileMetaIter {
    iter: CFileMetaIterator,
    _cleanup: PlanResultCleanup,
}

impl FfiFileMetaIter {
    pub(crate) fn new(iter: CFileMetaIterator, cleanup: PlanResultCleanup) -> Self {
        Self {
            iter,
            _cleanup: cleanup,
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

    fn arrow_array_to_file_meta(array: FFI_ArrowArray) -> DeltaResult<delta_kernel::FileMeta> {
        let schema = Self::arrow_schema()?;
        let array_data = unsafe { arrow_ffi::from_ffi(array, &schema) }?;
        let array = arrow_array::make_array(array_data);

        let Some(struct_array) = array.as_any().downcast_ref::<StructArray>() else {
            return Err(Error::generic(format!(
                "CFileMetaIterator must yield StructArray, got {:?}",
                array.data_type()
            )));
        };
        if struct_array.len() != 1 {
            return Err(Error::generic(format!(
                "CFileMetaIterator array must contain exactly one row, got {}",
                struct_array.len()
            )));
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

        if location_col.is_null(0) || last_modified_col.is_null(0) || size_col.is_null(0) {
            return Err(Error::generic(
                "CFileMetaIterator row must not contain null fields",
            ));
        }

        Ok(delta_kernel::FileMeta {
            location: Url::parse(location_col.value(0))?,
            last_modified: last_modified_col.value(0),
            size: size_col.value(0),
        })
    }
}

impl Iterator for FfiFileMetaIter {
    type Item = DeltaResult<delta_kernel::FileMeta>;

    fn next(&mut self) -> Option<Self::Item> {
        match (self.iter.next)(self.iter.state) {
            OptionalValue::None => None,
            OptionalValue::Some(ExternResult::Err(err)) => Some(Err(engine_error_to_kernel(err))),
            OptionalValue::Some(ExternResult::Ok(array)) => {
                Some(Self::arrow_array_to_file_meta(array))
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
    use crate::error::{EngineError, KernelError};
    use crate::plans::result::PlanResultCleanup;

    // === Shared Test Helpers ===

    struct MockIter<T> {
        items: Mutex<VecDeque<OptionalValue<ExternResult<T>>>>,
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
        items: Vec<OptionalValue<ExternResult<T>>>,
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

    fn err_ptr(err: &EngineError) -> *mut EngineError {
        err as *const EngineError as *mut EngineError
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
        let err = EngineError {
            etype: KernelError::GenericError,
        };
        let (mock_iter, cleanup_called) = make_mock_iter::<Handle<ExclusiveEngineData>>(vec![
            OptionalValue::Some(ExternResult::Ok(make_data_handle(3))),
            OptionalValue::Some(ExternResult::Err(err_ptr(&err))),
            OptionalValue::Some(ExternResult::Ok(make_data_handle(5))),
        ]);
        let state = NonNull::new(Box::into_raw(mock_iter) as *mut c_void);

        extern "C" fn next(
            state: NullableCvoid,
        ) -> OptionalValue<ExternResult<Handle<ExclusiveEngineData>>> {
            let mock_iter = unsafe {
                &*(state.unwrap().as_ptr() as *const MockIter<Handle<ExclusiveEngineData>>)
            };
            mock_iter
                .items
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(OptionalValue::None)
        }

        let mut iter = FfiEngineDataIter::new(
            CEngineDataIterator { state, next },
            PlanResultCleanup::new(state, free_mock_iter::<Handle<ExclusiveEngineData>>),
        );

        let first = iter.next().expect("first item").expect("first ok");
        assert_eq!(first.len(), 3);

        let second = iter.next().expect("second item");
        assert!(second.is_err(), "second item should be Err");

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
        let err = EngineError {
            etype: KernelError::GenericError,
        };
        let (mock_iter, cleanup_called) = make_mock_iter::<FFI_ArrowArray>(vec![
            OptionalValue::Some(ExternResult::Ok(make_binary_ffi_array(b"hello"))),
            OptionalValue::Some(ExternResult::Err(err_ptr(&err))),
            OptionalValue::Some(ExternResult::Ok(make_binary_ffi_array(b"world!"))),
        ]);
        let state = NonNull::new(Box::into_raw(mock_iter) as *mut c_void);

        extern "C" fn next(state: NullableCvoid) -> OptionalValue<ExternResult<FFI_ArrowArray>> {
            let mock_iter =
                unsafe { &*(state.unwrap().as_ptr() as *const MockIter<FFI_ArrowArray>) };
            mock_iter
                .items
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(OptionalValue::None)
        }

        let mut iter = FfiBytesIter::new(
            CBytesIterator { state, next },
            PlanResultCleanup::new(state, free_mock_iter::<FFI_ArrowArray>),
        );

        let first = iter.next().expect("first item").expect("first ok");
        assert_eq!(first.as_ref(), b"hello");

        let second = iter.next().expect("second item");
        assert!(second.is_err(), "second item should be Err");

        let third = iter.next().expect("third item").expect("third ok");
        assert_eq!(third.as_ref(), b"world!");

        assert!(iter.next().is_none(), "iterator should be exhausted");
        assert!(!cleanup_called.load(Ordering::SeqCst));

        drop(iter);
        assert!(cleanup_called.load(Ordering::SeqCst));
    }

    // === FfiFileMetaIter Test ===

    /// Helper for building each yielded FileMeta entry. Constructs a single-row `StructArray`
    /// matching the fixed schema kernel synthesizes in `FfiFileMetaIter::arrow_data_type`.
    fn make_file_meta_ffi_array(location: &str, last_modified: i64, size: u64) -> FFI_ArrowArray {
        let struct_array = StructArray::from(vec![
            (
                Arc::new(ArrowField::new("location", ArrowDataType::Utf8, false)),
                Arc::new(StringArray::from(vec![location])) as ArrayRef,
            ),
            (
                Arc::new(ArrowField::new(
                    "last_modified",
                    ArrowDataType::Int64,
                    false,
                )),
                Arc::new(Int64Array::from(vec![last_modified])) as ArrayRef,
            ),
            (
                Arc::new(ArrowField::new("size", ArrowDataType::UInt64, false)),
                Arc::new(UInt64Array::from(vec![size])) as ArrayRef,
            ),
        ]);
        let (ffi_array, _ffi_schema) = arrow_ffi::to_ffi(&struct_array.into_data()).unwrap();
        ffi_array
    }

    #[test]
    fn ffi_file_meta_iter_drains_and_runs_cleanup() {
        let err = EngineError {
            etype: KernelError::GenericError,
        };
        let (mock_iter, cleanup_called) = make_mock_iter::<FFI_ArrowArray>(vec![
            OptionalValue::Some(ExternResult::Ok(make_file_meta_ffi_array(
                "file:///a.parquet",
                1,
                100,
            ))),
            OptionalValue::Some(ExternResult::Err(err_ptr(&err))),
            OptionalValue::Some(ExternResult::Ok(make_file_meta_ffi_array(
                "file:///b.parquet",
                2,
                200,
            ))),
        ]);
        let state = NonNull::new(Box::into_raw(mock_iter) as *mut c_void);

        extern "C" fn next(state: NullableCvoid) -> OptionalValue<ExternResult<FFI_ArrowArray>> {
            let mock_iter =
                unsafe { &*(state.unwrap().as_ptr() as *const MockIter<FFI_ArrowArray>) };
            mock_iter
                .items
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(OptionalValue::None)
        }

        let mut iter = FfiFileMetaIter::new(
            CFileMetaIterator { state, next },
            PlanResultCleanup::new(state, free_mock_iter::<FFI_ArrowArray>),
        );

        let first = iter.next().expect("first item").expect("first ok");
        assert_eq!(first.location.as_str(), "file:///a.parquet");
        assert_eq!(first.last_modified, 1);
        assert_eq!(first.size, 100);

        let second = iter.next().expect("second item");
        assert!(second.is_err(), "second item should be Err");

        let third = iter.next().expect("third item").expect("third ok");
        assert_eq!(third.location.as_str(), "file:///b.parquet");
        assert_eq!(third.last_modified, 2);
        assert_eq!(third.size, 200);

        assert!(iter.next().is_none(), "iterator should be exhausted");
        assert!(!cleanup_called.load(Ordering::SeqCst));

        drop(iter);
        assert!(cleanup_called.load(Ordering::SeqCst));
    }
}
