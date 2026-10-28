//! FFI lifecycle for Kernel's deletion-vector serializer and caller-owned output.

use std::io::{self, Write};

use delta_kernel::actions::deletion_vector_writer::{
    DeletionVectorWriteResult, KernelDeletionVector, StreamingDeletionVectorWriter,
};
use delta_kernel::{KernelError, KernelResult};
use delta_kernel_ffi_macros::handle_descriptor;

use crate::error::{AllocateErrorFn, EngineExecResult, ExternResult, IntoExternResult};
use crate::handle::Handle;
use crate::{FfiSlice, KernelBytesSlice, NullableCvoid};

/// Writes some or all of `bytes` to the caller's output.
///
/// The byte slice is borrowed for this callback only. Set `out` to `Success(n)` with
/// `0 <= n <= bytes.len`, or `Failure` with a Rust-owned error message. Partial writes are retried.
/// Kernel initializes `out` to `Uninit`; leaving it unchanged is an error. A null callback is
/// rejected by `new_dv_writer`.
pub type DvWriteFn = Option<
    unsafe extern "C" fn(
        context: NullableCvoid,
        bytes: KernelBytesSlice,
        out: *mut EngineExecResult<usize>,
    ),
>;

/// Flushes the caller's output, setting `out` to `Success(true)` or `Failure`.
///
/// Kernel initializes `out` to `Uninit`. `Success(false)` or an unchanged result is an error.
/// A null callback is rejected by `new_dv_writer`.
pub type DvFlushFn =
    Option<unsafe extern "C" fn(context: NullableCvoid, out: *mut EngineExecResult<bool>)>;

/// Caller-owned output borrowed by a DV writer until it is finalized or freed.
///
/// The record must stay at the same address and must not be accessed, modified, or passed to
/// another writer during that borrow. Callbacks run synchronously on the thread making the FFI
/// call. Their context may be null for stateless callbacks. Kernel never frees the record or
/// context. The callbacks and context must support sequential calls from any thread to which the
/// writer is moved, and must not unwind or re-enter the same writer.
#[repr(C)]
pub struct FfiDvWriterOutput {
    /// Opaque caller-owned callback state, borrowed for the writer's lifetime.
    pub context: NullableCvoid,
    /// Required output write callback.
    pub write: DvWriteFn,
    /// Required output flush callback.
    pub flush: DvFlushFn,
}

// The private adapter is layout-identical to the caller's record. Only new_dv_writer constructs its
// borrow, after imposing the callback/lifetime contract; the public record exposes no safe I/O API.
#[repr(transparent)]
struct BorrowedDvOutput(FfiDvWriterOutput);

// SAFETY: new_dv_writer requires callbacks and context to support sequential calls across threads.
unsafe impl Send for BorrowedDvOutput {}

/// Owns the serializer state while exclusively borrowing its caller-owned output.
pub struct DvWriter {
    writer: StreamingDeletionVectorWriter<'static, BorrowedDvOutput>,
    failed: bool,
}

/// Exclusive DV writer handle. Finalize it, or abandon it with `free_dv_writer`.
#[handle_descriptor(target=DvWriter, mutable=true, sized=true)]
pub struct ExclusiveDvWriter;

/// The offset, serialized size, and cardinality returned for one complete deletion vector.
///
/// Combine these fields with the caller-selected output path to construct a DV descriptor.
#[repr(C)]
#[derive(Debug, PartialEq, Eq)]
pub struct FfiDvWriteResult {
    /// Offset of this DV's size prefix within its output file.
    pub offset: i32,
    /// Serialized DV size, excluding the size prefix and checksum.
    pub size_in_bytes: i32,
    /// Number of distinct deleted physical row indexes.
    pub cardinality: i64,
}

/// Creates a serializer borrowing `output`; does not write or upload anything.
///
/// Create one writer per desired output file. Write one or more complete vectors with
/// `dv_writer_write`, then consume the handle with `dv_writer_finalize`. To abandon an output,
/// consume it with `free_dv_writer` instead. Neither operation frees the caller's output.
/// `allocate_error` allocates errors for this standalone serializer call; no Engine is required.
///
/// # Errors
/// Returns an error if `output` is null or either callback is missing.
///
/// # Safety
/// A non-null `output` must be aligned, initialized, and exclusively available. On success, its
/// address, callbacks, and context must remain valid until the returned handle is finalized or
/// freed, following `FfiDvWriterOutput`'s lifetime and threading contract. On error no borrow is
/// retained. The error allocator must be valid for this call.
#[no_mangle]
pub unsafe extern "C" fn new_dv_writer(
    output: *mut FfiDvWriterOutput,
    allocate_error: AllocateErrorFn,
) -> ExternResult<Handle<ExclusiveDvWriter>> {
    let result: KernelResult<_> = (|| {
        // SAFETY: the transparent adapter has the same layout as the caller-owned output record.
        let output = unsafe { output.cast::<BorrowedDvOutput>().as_mut() }
            .ok_or_else(|| KernelError::generic("DV writer output must not be null"))?;
        if output.0.write.is_none() || output.0.flush.is_none() {
            return Err(KernelError::generic(
                "DV writer requires write and flush callbacks",
            ));
        }
        // SAFETY: the FFI caller keeps this exclusive borrow alive until finalize/free consumes it.
        let writer = StreamingDeletionVectorWriter::new(output);
        Ok(Box::new(DvWriter {
            writer,
            failed: false,
        })
        .into())
    })();
    result.into_extern_result(&allocate_error)
}

/// Serializes one complete vector, returning its offset, size, and cardinality.
///
/// `deleted_rows` contains all deleted physical row indexes for that vector, including previous
/// deletions. The serializer neither reads nor merges an existing vector. Input order is
/// unrestricted; duplicate indexes are deduplicated and an empty slice represents an empty DV.
/// Each call appends an independent vector to this writer's output.
///
/// # Errors
/// Returns an error for a null writer pointer, malformed input, serialization/output failure, or
/// a writer already failed by such an error. Malformed input does not change the writer. After a
/// serialization/output failure, abandon the output and free the handle; it cannot be resumed.
///
/// # Safety
/// A non-null `writer` must point to a valid, exclusively borrowed handle. For nonzero length,
/// `deleted_rows` must address that many aligned, initialized u64 values, readable for this call.
/// A zero-length slice may have a null pointer. The output's lifetime/threading contract continues
/// to apply. Callbacks and the error allocator must be valid; no input rows are retained.
#[no_mangle]
pub unsafe extern "C" fn dv_writer_write(
    writer: *mut Handle<ExclusiveDvWriter>,
    deleted_rows: FfiSlice<u64>,
    allocate_error: AllocateErrorFn,
) -> ExternResult<FfiDvWriteResult> {
    let result: KernelResult<_> = (|| {
        let handle = unsafe { writer.as_mut() }
            .ok_or_else(|| KernelError::generic("DV writer pointer must not be null"))?;
        let writer = unsafe { handle.as_mut() };
        if writer.failed {
            return Err(KernelError::invalid_transaction_state(
                "DV writer has failed",
            ));
        }
        let rows = unsafe { deleted_rows.try_as_slice() }?;
        let mut dv = KernelDeletionVector::new();
        dv.add_deleted_row_indexes(rows);
        let result = writer.writer.write_deletion_vector(dv);
        writer.failed = result.is_err();
        result.map(FfiDvWriteResult::from)
    })();
    result.into_extern_result(&allocate_error)
}

/// Finalizes and consumes the writer, flushing its output.
///
/// The handle is consumed on both success and error. The caller may release its output record
/// and context after this call returns. No upload or transaction commit is performed.
///
/// # Errors
/// Returns an error if a prior serialization/output operation failed or flushing fails.
///
/// # Safety
/// `writer` must be a valid owned handle, and its output must still satisfy its lifetime/threading
/// contract. Do not use or free the handle again. The error allocator must be valid for this call.
#[no_mangle]
pub unsafe extern "C" fn dv_writer_finalize(
    writer: Handle<ExclusiveDvWriter>,
    allocate_error: AllocateErrorFn,
) -> ExternResult<bool> {
    let writer = unsafe { writer.into_inner() };
    let result = if writer.failed {
        Err(KernelError::invalid_transaction_state(
            "DV writer has failed",
        ))
    } else {
        writer.writer.finalize()
    };
    result.map(|()| true).into_extern_result(&allocate_error)
}

/// Abandons and consumes a writer without flushing or freeing its caller-owned output.
///
/// Any bytes already written are incomplete/unpublished output; cleanup is the caller's
/// responsibility. After this call the caller may release the output record and context.
///
/// # Safety
/// `writer` must be a valid owned handle and the borrowed output must still be valid. Do not use
/// or free the handle again.
#[no_mangle]
pub unsafe extern "C" fn free_dv_writer(writer: Handle<ExclusiveDvWriter>) {
    writer.drop_handle();
}

impl From<DeletionVectorWriteResult> for FfiDvWriteResult {
    fn from(result: DeletionVectorWriteResult) -> Self {
        Self {
            offset: result.offset,
            size_in_bytes: result.size_in_bytes,
            cardinality: result.cardinality,
        }
    }
}

impl Write for BorrowedDvOutput {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let callback = self
            .0
            .write
            .ok_or_else(|| io::Error::other("missing DV write callback"))?;
        let mut result = EngineExecResult::Uninit;
        unsafe {
            callback(
                self.0.context,
                KernelBytesSlice::new_unsafe(bytes),
                &mut result,
            );
        }
        match result {
            EngineExecResult::Success(count) if count <= bytes.len() => Ok(count),
            EngineExecResult::Success(_) => Err(io::Error::other("DV write count exceeds input")),
            EngineExecResult::Failure(error) => Err(io::Error::other(KernelError::from(error))),
            EngineExecResult::Uninit => {
                Err(io::Error::other("DV write callback left result unset"))
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        let callback = self
            .0
            .flush
            .ok_or_else(|| io::Error::other("missing DV flush callback"))?;
        let mut result = EngineExecResult::Uninit;
        unsafe { callback(self.0.context, &mut result) };
        match result {
            EngineExecResult::Success(true) => Ok(()),
            EngineExecResult::Success(false) => {
                Err(io::Error::other("DV flush callback returned false"))
            }
            EngineExecResult::Failure(error) => Err(io::Error::other(KernelError::from(error))),
            EngineExecResult::Uninit => {
                Err(io::Error::other("DV flush callback left result unset"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ptr::{self, NonNull};

    #[cfg(feature = "default-engine-base")]
    use delta_kernel::actions::deletion_vector::{
        DeletionVectorDescriptor, DeletionVectorStorageType,
    };
    #[cfg(feature = "default-engine-base")]
    use delta_kernel::object_store::{path::Path, ObjectStoreExt};
    #[cfg(feature = "default-engine-base")]
    use delta_kernel::Engine;
    use rstest::rstest;

    use super::*;
    use crate::error::{EngineExecError, FFIKernelError};
    use crate::ffi_test_utils::{allocate_err, ok_or_panic, recover_error};

    #[derive(Default)]
    struct Output {
        bytes: Vec<u8>,
        flushes: usize,
        max_chunk: Option<usize>,
        mode: Option<FailureMode>,
    }

    #[derive(Clone, Copy)]
    enum FailureMode {
        Write,
        WriteAfterPrefix,
        ZeroWrite,
        InvalidWriteCount,
        UnsetWriteResult,
        Flush,
        FalseFlush,
        UnsetFlushResult,
    }

    impl Output {
        fn callbacks(&mut self) -> FfiDvWriterOutput {
            FfiDvWriterOutput {
                context: Some(NonNull::from(self).cast()),
                write: Some(write_output),
                flush: Some(flush_output),
            }
        }
    }

    fn output_error(message: &str) -> EngineExecError {
        EngineExecError {
            etype: FFIKernelError::IOErrorError,
            message: Box::new(message.to_owned()).into(),
        }
    }

    unsafe extern "C" fn write_output(
        context: NullableCvoid,
        bytes: KernelBytesSlice,
        out: *mut EngineExecResult<usize>,
    ) {
        let output = unsafe { &mut *context.unwrap().as_ptr().cast::<Output>() };
        let bytes = unsafe { bytes.try_as_slice() }.unwrap();
        let result = match output.mode {
            Some(FailureMode::Write) => {
                EngineExecResult::Failure(output_error("test write failure"))
            }
            Some(FailureMode::WriteAfterPrefix) if !output.bytes.is_empty() => {
                EngineExecResult::Failure(output_error("test write failure"))
            }
            Some(FailureMode::ZeroWrite) => EngineExecResult::Success(0),
            Some(FailureMode::InvalidWriteCount) => EngineExecResult::Success(bytes.len() + 1),
            Some(FailureMode::UnsetWriteResult) => return,
            _ => {
                let count = output.max_chunk.unwrap_or(bytes.len()).min(bytes.len());
                output.bytes.extend_from_slice(&bytes[..count]);
                EngineExecResult::Success(count)
            }
        };
        unsafe { out.write(result) };
    }

    unsafe extern "C" fn flush_output(context: NullableCvoid, out: *mut EngineExecResult<bool>) {
        let output = unsafe { &mut *context.unwrap().as_ptr().cast::<Output>() };
        output.flushes += 1;
        let result = match output.mode {
            Some(FailureMode::Flush) => {
                EngineExecResult::Failure(output_error("test flush failure"))
            }
            Some(FailureMode::FalseFlush) => EngineExecResult::Success(false),
            Some(FailureMode::UnsetFlushResult) => return,
            _ => EngineExecResult::Success(true),
        };
        unsafe { out.write(result) };
    }

    fn rows(indexes: &[u64]) -> FfiSlice<u64> {
        unsafe { FfiSlice::new_unsafe(indexes) }
    }

    fn expect_error<T>(result: ExternResult<T>, expected: &str) {
        match result {
            ExternResult::Err(error) => {
                let error = unsafe { recover_error(error) };
                assert!(error.message.contains(expected), "{}", error.message);
            }
            ExternResult::Ok(_) => panic!("expected error containing {expected}"),
        }
    }

    fn serialize(vectors: &[&[u64]]) -> (Output, Vec<FfiDvWriteResult>) {
        let mut output = Output::default();
        let mut callbacks = output.callbacks();
        let mut writer = ok_or_panic(unsafe { new_dv_writer(&mut callbacks, allocate_err) });
        let results = vectors
            .iter()
            .map(|indexes| {
                ok_or_panic(unsafe { dv_writer_write(&mut writer, rows(indexes), allocate_err) })
            })
            .collect();
        assert!(ok_or_panic(unsafe {
            dv_writer_finalize(writer, allocate_err)
        }));
        (output, results)
    }

    #[test]
    fn several_complete_vectors_share_one_output_and_match_the_rust_writer() {
        let vectors: &[&[u64]] = &[&[5, 1, 5, 0], &[2, u32::MAX as u64 + 1, u64::MAX], &[]];
        let (output, results) = serialize(vectors);
        let mut expected = Vec::new();
        let mut writer = StreamingDeletionVectorWriter::new(&mut expected);
        let expected_results: Vec<_> = vectors
            .iter()
            .map(|indexes| {
                let mut dv = KernelDeletionVector::new();
                dv.add_deleted_row_indexes(*indexes);
                FfiDvWriteResult::from(writer.write_deletion_vector(dv).unwrap())
            })
            .collect();
        writer.finalize().unwrap();
        assert_eq!(output.bytes, expected);
        assert_eq!(results, expected_results);
        assert_eq!(results[0].offset, 1);
        assert_eq!(results[0].cardinality, 3);
        assert!(results[1].offset > results[0].offset);
        assert_eq!(results[2].cardinality, 0);
        assert_eq!(output.flushes, 1);
    }

    #[test]
    fn callers_can_choose_separate_outputs_and_offsets_restart() {
        let (first, a) = serialize(&[&[1, 3, 7]]);
        let (second, b) = serialize(&[&[2]]);
        assert_eq!(a[0].offset, 1);
        assert_eq!(b[0].offset, 1);
        assert_eq!(a[0].cardinality, 3);
        assert_eq!(b[0].cardinality, 1);
        assert_ne!(first.bytes, second.bytes);
    }

    #[test]
    fn empty_writer_finalizes_without_creating_a_vector() {
        let (output, results) = serialize(&[]);
        assert!(output.bytes.is_empty());
        assert!(results.is_empty());
        assert_eq!(output.flushes, 1);
    }

    #[test]
    fn null_empty_rows_are_a_complete_empty_vector() {
        let mut output = Output::default();
        let mut callbacks = output.callbacks();
        let mut writer = ok_or_panic(unsafe { new_dv_writer(&mut callbacks, allocate_err) });
        let result =
            ok_or_panic(unsafe { dv_writer_write(&mut writer, FfiSlice::empty(), allocate_err) });
        assert_eq!(result.cardinality, 0);
        assert!(ok_or_panic(unsafe {
            dv_writer_finalize(writer, allocate_err)
        }));
        assert!(!output.bytes.is_empty());
    }

    #[test]
    fn invalid_row_slice_does_not_change_or_poison_the_writer() {
        let mut output = Output::default();
        let mut callbacks = output.callbacks();
        let mut writer = ok_or_panic(unsafe { new_dv_writer(&mut callbacks, allocate_err) });
        expect_error(
            unsafe {
                dv_writer_write(
                    &mut writer,
                    FfiSlice {
                        ptr: ptr::null(),
                        len: 1,
                    },
                    allocate_err,
                )
            },
            "null",
        );
        assert!(output.bytes.is_empty());
        let result = ok_or_panic(unsafe { dv_writer_write(&mut writer, rows(&[4]), allocate_err) });
        assert_eq!(result.offset, 1);
        assert_eq!(result.cardinality, 1);
        ok_or_panic(unsafe { dv_writer_finalize(writer, allocate_err) });
    }

    #[test]
    fn null_output_and_writer_pointers_return_errors() {
        expect_error(
            unsafe { new_dv_writer(ptr::null_mut(), allocate_err) },
            "null",
        );
        expect_error(
            unsafe { dv_writer_write(ptr::null_mut(), FfiSlice::empty(), allocate_err) },
            "null",
        );
    }

    #[rstest]
    #[case::write(true)]
    #[case::flush(false)]
    fn missing_output_callbacks_are_rejected(#[case] missing_write: bool) {
        let mut output = Output::default();
        let mut callbacks = output.callbacks();
        if missing_write {
            callbacks.write = None;
        } else {
            callbacks.flush = None;
        }
        expect_error(
            unsafe { new_dv_writer(&mut callbacks, allocate_err) },
            "callbacks",
        );
        assert!(output.bytes.is_empty());
        assert_eq!(output.flushes, 0);
    }

    #[test]
    fn short_writes_are_completed_by_the_rust_serializer() {
        let mut output = Output {
            max_chunk: Some(2),
            ..Default::default()
        };
        let mut callbacks = output.callbacks();
        let mut writer = ok_or_panic(unsafe { new_dv_writer(&mut callbacks, allocate_err) });
        ok_or_panic(unsafe { dv_writer_write(&mut writer, rows(&[0, 7]), allocate_err) });
        ok_or_panic(unsafe { dv_writer_finalize(writer, allocate_err) });
        assert_eq!(output.bytes, serialize(&[&[0, 7]]).0.bytes);
    }

    #[rstest]
    #[case::write(FailureMode::Write, "test write failure")]
    #[case::partial_output(FailureMode::WriteAfterPrefix, "test write failure")]
    #[case::zero(FailureMode::ZeroWrite, "Failed to write")]
    #[case::invalid_count(FailureMode::InvalidWriteCount, "exceeds input")]
    #[case::unset(FailureMode::UnsetWriteResult, "left result unset")]
    fn failed_writes_cannot_be_resumed(#[case] mode: FailureMode, #[case] message: &str) {
        let mut output = Output {
            mode: Some(mode),
            ..Default::default()
        };
        let mut callbacks = output.callbacks();
        let mut writer = ok_or_panic(unsafe { new_dv_writer(&mut callbacks, allocate_err) });
        expect_error(
            unsafe { dv_writer_write(&mut writer, rows(&[1]), allocate_err) },
            message,
        );
        if matches!(mode, FailureMode::WriteAfterPrefix) {
            assert!(!output.bytes.is_empty());
        }
        output.mode = None;
        expect_error(
            unsafe { dv_writer_write(&mut writer, rows(&[1]), allocate_err) },
            "has failed",
        );
        expect_error(
            unsafe { dv_writer_finalize(writer, allocate_err) },
            "has failed",
        );
        assert_eq!(output.flushes, 0);
    }

    #[rstest]
    #[case::flush(FailureMode::Flush, "test flush failure")]
    #[case::false_result(FailureMode::FalseFlush, "returned false")]
    #[case::unset(FailureMode::UnsetFlushResult, "left result unset")]
    fn finalization_propagates_flush_errors(#[case] mode: FailureMode, #[case] message: &str) {
        let mut output = Output {
            mode: Some(mode),
            ..Default::default()
        };
        let mut callbacks = output.callbacks();
        let mut writer = ok_or_panic(unsafe { new_dv_writer(&mut callbacks, allocate_err) });
        ok_or_panic(unsafe { dv_writer_write(&mut writer, rows(&[1]), allocate_err) });
        expect_error(unsafe { dv_writer_finalize(writer, allocate_err) }, message);
        assert_eq!(output.flushes, 1);
    }

    #[test]
    fn free_abandons_output_without_flushing() {
        let mut output = Output::default();
        let mut callbacks = output.callbacks();
        let mut writer = ok_or_panic(unsafe { new_dv_writer(&mut callbacks, allocate_err) });
        ok_or_panic(unsafe { dv_writer_write(&mut writer, rows(&[1]), allocate_err) });
        unsafe { free_dv_writer(writer) };
        assert!(!output.bytes.is_empty());
        assert_eq!(output.flushes, 0);
    }

    #[test]
    fn writer_can_move_threads_while_its_output_remains_exclusive() {
        let mut output = Output::default();
        let mut callbacks = output.callbacks();
        let writer = ok_or_panic(unsafe { new_dv_writer(&mut callbacks, allocate_err) });
        let result = std::thread::spawn(move || {
            let mut writer = writer;
            let result =
                ok_or_panic(unsafe { dv_writer_write(&mut writer, rows(&[1, 3]), allocate_err) });
            ok_or_panic(unsafe { dv_writer_finalize(writer, allocate_err) });
            result
        })
        .join()
        .unwrap();
        assert_eq!(result.cardinality, 2);
        assert_eq!(output.flushes, 1);
        assert_eq!(output.bytes, serialize(&[&[1, 3]]).0.bytes);
    }

    #[cfg(feature = "default-engine-base")]
    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn kernel_reader_decodes_independent_vectors_from_shared_and_separate_outputs() {
        let vectors: &[&[u64]] = &[&[1, 3, 7], &[2, 8], &[]];
        let (store, engine, root) = test_utils::engine_store_setup("ffi_dv_serializer", None);
        for shared in [true, false] {
            let groups: Vec<&[&[u64]]> = if shared {
                vec![vectors]
            } else {
                vectors.chunks(1).collect()
            };
            for (file, group) in groups.iter().enumerate() {
                let (output, results) = serialize(group);
                let path = root.join(&format!("output-{shared}-{file}.bin")).unwrap();
                store
                    .put(
                        &Path::from_url_path(path.path()).unwrap(),
                        output.bytes.into(),
                    )
                    .await
                    .unwrap();
                for (result, expected) in results.into_iter().zip(*group) {
                    let descriptor = DeletionVectorDescriptor::try_new(
                        DeletionVectorStorageType::PersistedAbsolute,
                        path.as_str(),
                        Some(result.offset),
                        result.size_in_bytes,
                        result.cardinality,
                    )
                    .unwrap();
                    assert_eq!(
                        descriptor
                            .row_indexes(engine.storage_handler(), &root)
                            .unwrap(),
                        *expected
                    );
                }
            }
        }
    }
}
