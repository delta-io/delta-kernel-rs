//! This module contains types needed to implement Engine-owned iterators returned from plan
//! execution.
//!
//! Each `C*Iterator` type contains an opaque engine `state` pointer with a `next` function
//! pointer that yields one iterator item per call. The state pointer and next function pointer must
//! be threadsafe.
//!
//! //! # `next()` protocol
//!
//! Each `next` callback returns `OptionalValue<ExternResult<T>>`, mirroring Rust's
//! `Option<Result<T>>`. This allows us to implement `DeltaResultIterator<T>`:
//!     - The outer Option represents whether iteration is complete
//!     - The inner Result represents the DeltaResult yielded by the iterator
//!
//! # Iterator State Cleanup
//!
//! The iterator state is owned by the engine and must be freed by the kernel when it is done with
//! it. However the `C*Iterator` types do NOT contain their own release/free functions; instead,
//! the Engine provides a single `free` function for the entire PlanResult (see
//! [`super::result::CPlanResultWrapper`]). This is handled on the Rust side by
//! embedding `PlanResultCleanup` inside of each `Ffi*Iter` adapter so that the free function is
//! called when the adapter is dropped.

use bytes::Bytes;
use delta_kernel::arrow::array::ffi::{self as arrow_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use delta_kernel::arrow::array::{self as arrow_array, Array, BinaryArray};
use delta_kernel::arrow::datatypes::DataType as ArrowDataType;
use delta_kernel::{DeltaResult, EngineData, Error};
use url::Url;

use super::result::PlanResultCleanup;
use crate::engine_funcs::FileMeta as CFileMeta;
use crate::error::ExternResult;
use crate::handle::Handle;
use crate::plans::result::engine_error_to_kernel;
use crate::{ExclusiveEngineData, NullableCvoid, OptionalValue, TryFromStringSlice};

// ============================================================================
// repr(C) iterator types
// ============================================================================

/// An engine-owned iterator of [`EngineData`] batches.
///
/// Each EngineData batch has its own separate lifetime and must be freed. Once the iterator yields
/// a batch, the ownership of it is transferred to the Kernel and Kernel is now responsible
/// for freeing it (this is handled transparently by the `FfiEngineDataIter` adapter).
#[repr(C)]
pub struct CEngineDataIterator {
    pub state: NullableCvoid,
    pub next: extern "C" fn(
        state: NullableCvoid,
    ) -> OptionalValue<ExternResult<Handle<ExclusiveEngineData>>>,
}

/// An engine-owned iterator of byte buffers.
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
/// Unlike `CEngineDataIterator` and `CBytesIterator`, CFileMeta does not maintain it's own `free`
/// callback. Instead, we expect the engine to free the entire iterator of FileMeta entries when the
/// iterator itself is dropped (through the `PlanResultCleanup` guard).
#[repr(C)]
pub struct CFileMetaIterator {
    pub state: NullableCvoid,
    pub next: extern "C" fn(state: NullableCvoid) -> OptionalValue<ExternResult<CFileMeta>>,
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

// SAFETY: the engine is expected to provide thread-safe state + free function pointers.
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

// SAFETY: the engine is expected to provide thread-safe state + free function pointers.
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

    fn convert_file_meta(raw: CFileMeta) -> DeltaResult<delta_kernel::FileMeta> {
        let path: &str = unsafe { TryFromStringSlice::try_from_slice(&raw.path) }?;
        let location = Url::parse(path)?;
        let size: delta_kernel::FileSize = raw.size.try_into().map_err(|_| {
            Error::generic("unable to convert CFileMeta.size (usize) to FileMeta.size (u64)")
        })?;
        Ok(delta_kernel::FileMeta {
            location,
            last_modified: raw.last_modified,
            size,
        })
    }
}

impl Iterator for FfiFileMetaIter {
    type Item = DeltaResult<delta_kernel::FileMeta>;

    fn next(&mut self) -> Option<Self::Item> {
        match (self.iter.next)(self.iter.state) {
            OptionalValue::None => None,
            OptionalValue::Some(ExternResult::Err(err)) => Some(Err(engine_error_to_kernel(err))),
            OptionalValue::Some(ExternResult::Ok(raw)) => Some(Self::convert_file_meta(raw)),
        }
    }
}

// SAFETY: the engine is expected to provide thread-safe state + free function pointers.
unsafe impl Send for FfiFileMetaIter {}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::ffi::c_void;
    use std::ptr::NonNull;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    use delta_kernel::arrow::array::{ffi as arrow_ffi, BinaryArray, Int32Array, RecordBatch};
    use delta_kernel::arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    };
    use delta_kernel::engine::arrow_data::ArrowEngineData;

    use super::*;
    use crate::error::{EngineError, KernelError};
    use crate::plans::result::PlanResultCleanup;

    // === Shared Test Helpers ===

    type MockIter<T> = Mutex<VecDeque<OptionalValue<ExternResult<T>>>>;

    fn make_iter<T>(items: Vec<OptionalValue<ExternResult<T>>>) -> MockIter<T> {
        Mutex::new(VecDeque::from(items))
    }

    fn iter_state<T>(iter: &MockIter<T>) -> NullableCvoid {
        NonNull::new(iter as *const MockIter<T> as *mut c_void)
    }

    // A mock cleanup function which sets a flag on an AtomicBool so tests can verify whether drop
    // was called. Panics if invoked more than once, enforcing the contract that
    // `PlanResultCleanup` fires `free` exactly once.
    extern "C" fn set_cleanup_flag(state: NullableCvoid) {
        let flag = unsafe { &*(state.unwrap().as_ptr() as *const AtomicBool) };
        let previously_set = flag.swap(true, Ordering::SeqCst);
        assert!(!previously_set, "set_cleanup_flag invoked more than once");
    }

    fn make_cleanup(flag: &AtomicBool) -> PlanResultCleanup {
        let cleanup_flag_state = NonNull::new(flag as *const AtomicBool as *mut c_void);
        PlanResultCleanup::new(cleanup_flag_state, set_cleanup_flag)
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
        let cleanup_called = AtomicBool::new(false);
        let err = EngineError {
            etype: KernelError::GenericError,
        };
        let mock_iter = make_iter::<Handle<ExclusiveEngineData>>(vec![
            OptionalValue::Some(ExternResult::Ok(make_data_handle(3))),
            OptionalValue::Some(ExternResult::Err(err_ptr(&err))),
            OptionalValue::Some(ExternResult::Ok(make_data_handle(5))),
        ]);

        extern "C" fn next(
            state: NullableCvoid,
        ) -> OptionalValue<ExternResult<Handle<ExclusiveEngineData>>> {
            // SAFETY: state aliases a `MockIter<Handle<ExclusiveEngineData>>` borrowed for the
            // lifetime of the enclosing test.
            let mock_iter = unsafe {
                &*(state.unwrap().as_ptr() as *const MockIter<Handle<ExclusiveEngineData>>)
            };
            mock_iter
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(OptionalValue::None)
        }

        let mut iter = FfiEngineDataIter::new(
            CEngineDataIterator {
                state: iter_state(&mock_iter),
                next,
            },
            make_cleanup(&cleanup_called),
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
        let cleanup_called = AtomicBool::new(false);
        let err = EngineError {
            etype: KernelError::GenericError,
        };
        let mock_iter = make_iter::<FFI_ArrowArray>(vec![
            OptionalValue::Some(ExternResult::Ok(make_binary_ffi_array(b"hello"))),
            OptionalValue::Some(ExternResult::Err(err_ptr(&err))),
            OptionalValue::Some(ExternResult::Ok(make_binary_ffi_array(b"world!"))),
        ]);

        extern "C" fn next(state: NullableCvoid) -> OptionalValue<ExternResult<FFI_ArrowArray>> {
            // SAFETY: state aliases a `MockIter<FFI_ArrowArray>` borrowed for the lifetime of the
            // enclosing test.
            let mock_iter =
                unsafe { &*(state.unwrap().as_ptr() as *const MockIter<FFI_ArrowArray>) };
            mock_iter
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(OptionalValue::None)
        }

        let mut iter = FfiBytesIter::new(
            CBytesIterator {
                state: iter_state(&mock_iter),
                next,
            },
            make_cleanup(&cleanup_called),
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

    /// Helper for building each yielded FileMeta entry
    fn make_file_meta(path: &'static str, size: usize, last_modified: i64) -> CFileMeta {
        let path_slice = unsafe { crate::KernelStringSlice::new_unsafe(path) };
        CFileMeta {
            path: path_slice,
            last_modified,
            size,
        }
    }

    #[test]
    fn ffi_file_meta_iter_drains_and_runs_cleanup() {
        let cleanup_called = AtomicBool::new(false);
        let err = EngineError {
            etype: KernelError::GenericError,
        };
        let mock_iter = make_iter::<CFileMeta>(vec![
            OptionalValue::Some(ExternResult::Ok(make_file_meta(
                "file:///a.parquet",
                100,
                1,
            ))),
            OptionalValue::Some(ExternResult::Err(err_ptr(&err))),
            OptionalValue::Some(ExternResult::Ok(make_file_meta(
                "file:///b.parquet",
                200,
                2,
            ))),
        ]);

        extern "C" fn next(state: NullableCvoid) -> OptionalValue<ExternResult<CFileMeta>> {
            // SAFETY: state aliases a `MockIter<CFileMeta>` borrowed for the lifetime of the
            // enclosing test.
            let mock_iter = unsafe { &*(state.unwrap().as_ptr() as *const MockIter<CFileMeta>) };
            mock_iter
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(OptionalValue::None)
        }

        let mut iter = FfiFileMetaIter::new(
            CFileMetaIterator {
                state: iter_state(&mock_iter),
                next,
            },
            make_cleanup(&cleanup_called),
        );

        let first = iter.next().expect("first item").expect("first ok");
        assert_eq!(first.location.as_str(), "file:///a.parquet");
        assert_eq!(first.size, 100);
        assert_eq!(first.last_modified, 1);

        let second = iter.next().expect("second item");
        assert!(second.is_err(), "second item should be Err");

        let third = iter.next().expect("third item").expect("third ok");
        assert_eq!(third.location.as_str(), "file:///b.parquet");
        assert_eq!(third.size, 200);
        assert_eq!(third.last_modified, 2);

        assert!(iter.next().is_none(), "iterator should be exhausted");
        assert!(!cleanup_called.load(Ordering::SeqCst));

        drop(iter);
        assert!(cleanup_called.load(Ordering::SeqCst));
    }
}
