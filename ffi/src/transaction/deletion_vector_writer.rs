//! FFI lifecycle for Kernel's deletion-vector serializer and caller-owned output.

use std::io::{self, Write};
use std::ptr::NonNull;

use delta_kernel::actions::deletion_vector_writer::{
    DeletionVectorWriteResult, KernelDeletionVector, StreamingDeletionVectorWriter,
};
use delta_kernel::{KernelError, KernelResult};
use delta_kernel_ffi_macros::handle_descriptor;

use crate::error::{EngineExecResult, ErrorAllocators, ExternResult, IntoExternResult};
use crate::handle::{BoxExt, Handle};
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

/// Caller-owned callback record copied when a DV writer is created.
///
/// The record only needs to remain valid during `new_dv_writer`. The callbacks and pointed-to
/// context must remain valid until the writer is finalized or freed. Calls are synchronous;
/// context may be null for stateless callbacks and is never freed by Kernel. Callback state must
/// support sequential calls from any thread to which the writer is moved. Callbacks must not
/// unwind or re-enter the same writer.
#[repr(C)]
pub struct FfiDvWriterOutput {
    /// Opaque caller-owned callback state, borrowed for the writer's lifetime.
    pub context: NullableCvoid,
    /// Required output write callback.
    pub write: DvWriteFn,
    /// Required output flush callback.
    pub flush: DvFlushFn,
}

// The private allocation pins the copied callbacks independently of the caller's record.
struct OwnedDvOutput(FfiDvWriterOutput);

/// Owns the serializer and its copied callback record; the callback context remains caller-owned.
pub struct DvWriter {
    writer: Option<StreamingDeletionVectorWriter<'static, OwnedDvOutput>>,
    output: NonNull<OwnedDvOutput>,
    failed: bool,
}

// SAFETY: output uniquely owns a stable Box, and new_dv_writer requires its callbacks/context to
// support sequential calls across threads. No references to the allocation escape the handle.
unsafe impl Send for DvWriter {}

/// Exclusive DV writer handle. Finalize it, or abandon it with `free_dv_writer`.
#[handle_descriptor(target=DvWriter, mutable=true, sized=true)]
pub struct ExclusiveDvWriter;

/// Exclusive compressed deletion vector. Add index chunks, then write or free it.
#[handle_descriptor(target=KernelDeletionVector, mutable=true, sized=true)]
pub struct ExclusiveDeletionVector;

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

/// Creates an empty compressed deletion vector without writing anything.
///
/// Populate it with `deletion_vector_add_indexes`, then transfer it to `dv_writer_write`.
/// Use `free_deletion_vector` to abandon it. The vector must contain all prior and new deletions
/// before serialization; Kernel does not look up or merge a previously published DV.
#[unsafe(no_mangle)]
pub extern "C" fn new_deletion_vector() -> Handle<ExclusiveDeletionVector> {
    Box::new(KernelDeletionVector::new()).into()
}

/// Adds a borrowed chunk of physical row indexes to the compressed vector.
///
/// Unsorted indexes and duplicates are accepted. An empty chunk, including `{NULL, 0}`, is a no-op.
/// Indexes above `i64::MAX` or malformed chunks are rejected without changing the vector.
/// The caller must additionally check indexes against the target file's physical row count.
///
/// # Safety
/// `vector` must reference a live, exclusively borrowed handle. For nonzero length,
/// `indexes` must address that many aligned, initialized u64 values readable for this call.
/// `error_allocators` must point to a valid table for this call, with callbacks and context
/// satisfying `ErrorAllocators`'s contract. No input memory is retained after the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn deletion_vector_add_indexes(
    vector: &mut Handle<ExclusiveDeletionVector>,
    indexes: FfiSlice<u64>,
    error_allocators: *const ErrorAllocators,
) -> ExternResult<bool> {
    let result: KernelResult<_> = (|| {
        // SAFETY: a nonempty input is aligned, initialized, and readable for this call.
        let rows = unsafe { indexes.try_as_slice() }?;
        if rows.iter().any(|index| *index > i64::MAX as u64) {
            return Err(KernelError::deletion_vector(
                "DV row indexes must not exceed i64::MAX",
            ));
        }
        // SAFETY: the caller prevents concurrent access; validation completed before mutation.
        unsafe { vector.as_mut() }.add_deleted_row_indexes(rows);
        Ok(true)
    })();
    // SAFETY: the allocator table, context, and callbacks are valid for this call.
    unsafe { result.into_extern_result(&*error_allocators) }
}

/// Consumes an abandoned deletion vector without serializing it.
///
/// # Safety
/// `vector` must be a valid owned handle. Do not use or free it after this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn free_deletion_vector(vector: Handle<ExclusiveDeletionVector>) {
    // SAFETY: the caller transfers a live handle exactly once.
    unsafe { vector.drop_handle() };
}

/// Creates a serializer copying `output`; does not write or upload anything.
///
/// Create one writer per desired output file. Write one or more complete vectors with
/// `dv_writer_write`, then consume the handle with `dv_writer_finalize`. To abandon an output,
/// consume it with `free_dv_writer` instead. Neither operation frees the caller's output.
/// `error_allocators` allocates errors for this standalone serializer call; no Engine is required.
///
/// # Errors
/// Returns an error if either callback is missing.
///
/// # Safety
/// `output` must reference an initialized record readable for this call. Its callbacks and
/// context must satisfy `FfiDvWriterOutput`'s lifetime and threading contract for any returned
/// handle. No caller record is retained, including when it was stack-allocated in a helper.
/// `error_allocators` must point to a valid table for this call, with callbacks and context
/// satisfying `ErrorAllocators`'s contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn new_dv_writer(
    output: &FfiDvWriterOutput,
    error_allocators: *const ErrorAllocators,
) -> ExternResult<Handle<ExclusiveDvWriter>> {
    let result: KernelResult<_> = (|| {
        if output.write.is_none() || output.flush.is_none() {
            return Err(KernelError::generic(
                "DV writer requires write and flush callbacks",
            ));
        }
        let mut output = BoxExt::into_non_null(Box::new(OwnedDvOutput(FfiDvWriterOutput {
            context: output.context,
            write: output.write,
            flush: output.flush,
        })));
        // SAFETY: DvWriter owns this stable allocation and drops the serializer's borrow before
        // freeing it. The private pointer is never exposed or used while the borrow is active.
        let writer = StreamingDeletionVectorWriter::new(unsafe { output.as_mut() });
        Ok(Box::new(DvWriter {
            writer: Some(writer),
            output,
            failed: false,
        })
        .into())
    })();
    // SAFETY: the allocator table, context, and callbacks are valid for this call.
    unsafe { result.into_extern_result(&*error_allocators) }
}

/// Serializes one complete vector, returning its offset, size, and cardinality.
///
/// `vector` must contain all deleted physical row indexes, including previous deletions.
/// The serializer neither reads nor merges an existing vector. Each call appends an independent
/// vector to the writer's output and consumes its vector handle on both success and error.
///
/// # Errors
/// An already failed writer still consumes the vector. After a
/// serialization/output failure, abandon the output and free the writer; it cannot be resumed.
///
/// # Safety
/// `writer` must reference a valid, exclusively borrowed writer handle. `vector` must be
/// a valid owned handle created with `new_deletion_vector`; do not use or free it after this call.
/// The output's lifetime/threading contract applies. `error_allocators` must point to a valid
/// table for this call, with callbacks and context satisfying `ErrorAllocators`'s contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dv_writer_write(
    writer: &mut Handle<ExclusiveDvWriter>,
    vector: Handle<ExclusiveDeletionVector>,
    error_allocators: *const ErrorAllocators,
) -> ExternResult<FfiDvWriteResult> {
    // SAFETY: the caller transfers the live vector exactly once. Consume before any fallible check
    // so it is dropped on failed-writer, serialization, and output errors alike.
    let vector = unsafe { vector.into_inner() };
    let result: KernelResult<_> = (|| {
        // SAFETY: the caller supplies a valid handle and prevents concurrent/re-entrant access.
        let writer = unsafe { writer.as_mut() };
        if writer.failed {
            return Err(KernelError::invalid_transaction_state(
                "DV writer has failed",
            ));
        }
        let serializer = writer.writer.as_mut().ok_or_else(|| {
            KernelError::invalid_transaction_state("DV writer has been finalized")
        })?;
        let result = serializer.write_deletion_vector(*vector);
        writer.failed = result.is_err();
        result.map(FfiDvWriteResult::from)
    })();
    // SAFETY: the allocator table, context, and callbacks are valid for this call.
    unsafe { result.into_extern_result(&*error_allocators) }
}

/// Finalizes and consumes the writer, flushing its output.
///
/// The handle is consumed on both success and error. The caller may release its callback context
/// after this call returns. No upload or transaction commit is performed.
///
/// # Errors
/// Returns an error if a prior serialization/output operation failed or flushing fails.
///
/// # Safety
/// `writer` must be a valid owned handle, and its output must still satisfy its lifetime/threading
/// contract. Do not use or free the handle again. `error_allocators` must point to a valid table
/// for this call, with callbacks and context satisfying `ErrorAllocators`'s contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dv_writer_finalize(
    writer: Handle<ExclusiveDvWriter>,
    error_allocators: *const ErrorAllocators,
) -> ExternResult<bool> {
    // SAFETY: the caller transfers a live handle exactly once, including on failure.
    let mut writer = unsafe { writer.into_inner() };
    let result = if writer.failed {
        Err(KernelError::invalid_transaction_state(
            "DV writer has failed",
        ))
    } else {
        writer
            .writer
            .take()
            .ok_or_else(|| KernelError::invalid_transaction_state("DV writer has been finalized"))
            .and_then(StreamingDeletionVectorWriter::finalize)
    };
    // SAFETY: the allocator table, context, and callbacks are valid for this call.
    unsafe { result.map(|()| true).into_extern_result(&*error_allocators) }
}

/// Abandons and consumes a writer without flushing or freeing its caller-owned output.
///
/// Any bytes already written are incomplete/unpublished output; cleanup is the caller's
/// responsibility. After this call the caller may release the callback context.
///
/// # Safety
/// `writer` must be a valid owned handle and the borrowed output must still be valid. Do not use
/// or free the handle again.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn free_dv_writer(writer: Handle<ExclusiveDvWriter>) {
    // SAFETY: the caller transfers a live handle exactly once.
    unsafe { writer.drop_handle() };
}

impl Drop for DvWriter {
    fn drop(&mut self) {
        self.writer = None;
        // SAFETY: the serializer's exclusive borrow has ended. output came from Box::into_raw,
        // remains uniquely owned by this handle, and is reclaimed exactly once.
        unsafe { drop(Box::from_raw(self.output.as_ptr())) };
    }
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

impl Write for OwnedDvOutput {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let callback = self
            .0
            .write
            .ok_or_else(|| io::Error::other("missing DV write callback"))?;
        let mut result = EngineExecResult::Uninit;
        // SAFETY: the constructor requires a valid callback/context until finalize/free, and the
        // byte slice and initialized result slot are exclusively available for this callback.
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
        // SAFETY: the callback/context remain valid, and result is an exclusive initialized slot.
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
    use std::ffi::c_void;
    use std::ptr::{self, NonNull};
    #[cfg(feature = "default-engine-base")]
    use std::sync::Arc;

    #[cfg(feature = "default-engine-base")]
    use delta_kernel::Engine;
    #[cfg(feature = "default-engine-base")]
    use delta_kernel::actions::deletion_vector::{
        DeletionVectorDescriptor, DeletionVectorStorageType,
    };
    #[cfg(feature = "default-engine-base")]
    use delta_kernel::object_store::{ObjectStoreExt, path::Path};
    #[cfg(feature = "default-engine-base")]
    use delta_kernel::{
        arrow::{
            array::{ArrayRef, Int32Array},
            record_batch::RecordBatch,
        },
        committer::FileSystemCommitter,
        schema::schema_ref,
        transaction::UpdateTableOperation,
    };
    use rstest::rstest;

    use super::*;
    use crate::error::allocation::KernelErrorInfo;
    use crate::error::{EngineExecError, FFIKernelError};
    use crate::ffi_test_utils::{
        TEST_ERROR_ALLOCATORS, assert_extern_result_error_contains,
        assert_extern_result_error_with_message, ok_or_panic,
    };
    #[cfg(feature = "default-engine-base")]
    use crate::{
        SharedSnapshot,
        ffi_test_utils::engine_handle_for_store,
        free_engine, free_snapshot, kernel_string_slice,
        scan::{free_scan, scan, scan_metadata_iter_init},
        transaction::{
            KernelDvStorageType, KernelUpdateTableOperation, committed_transaction_version,
            dv_descriptor_map_insert, dv_descriptor_map_new, dv_descriptor_new,
            free_committed_transaction, new_update_table_txn_builder, update_table_txn_add_files,
            update_table_txn_builder_build, update_table_txn_commit,
            update_table_txn_update_deletion_vectors,
        },
    };

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

    fn vector(indexes: &[u64]) -> Handle<ExclusiveDeletionVector> {
        let mut vector = new_deletion_vector();
        ok_or_panic(unsafe {
            deletion_vector_add_indexes(&mut vector, rows(indexes), &TEST_ERROR_ALLOCATORS)
        });
        vector
    }

    fn serialize(vectors: &[&[u64]]) -> (Output, Vec<FfiDvWriteResult>) {
        let mut output = Output::default();
        let callbacks = output.callbacks();
        let mut writer = ok_or_panic(unsafe { new_dv_writer(&callbacks, &TEST_ERROR_ALLOCATORS) });
        let results = vectors
            .iter()
            .map(|indexes| {
                ok_or_panic(unsafe {
                    dv_writer_write(&mut writer, vector(indexes), &TEST_ERROR_ALLOCATORS)
                })
            })
            .collect();
        assert!(ok_or_panic(unsafe {
            dv_writer_finalize(writer, &TEST_ERROR_ALLOCATORS)
        }));
        (output, results)
    }

    #[test]
    fn several_complete_vectors_share_one_output_and_match_the_rust_writer() {
        let vectors: &[&[u64]] = &[
            &[5, 1, 5, 0],
            &[2, u32::MAX as u64 + 1, i64::MAX as u64],
            &[],
        ];
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
    fn vectors_accumulate_copied_chunks_and_deduplicate_across_additions() {
        let mut dv = new_deletion_vector();
        let mut first_chunk = vec![3, 1, 3];
        ok_or_panic(unsafe {
            deletion_vector_add_indexes(&mut dv, rows(&first_chunk), &TEST_ERROR_ALLOCATORS)
        });
        first_chunk.fill(99);
        drop(first_chunk);
        ok_or_panic(unsafe {
            deletion_vector_add_indexes(&mut dv, rows(&[9, 7, 1]), &TEST_ERROR_ALLOCATORS)
        });
        ok_or_panic(unsafe {
            deletion_vector_add_indexes(&mut dv, FfiSlice::empty(), &TEST_ERROR_ALLOCATORS)
        });
        assert_eq!(unsafe { dv.as_ref() }.cardinality(), 4);

        let mut output = Output::default();
        let callbacks = output.callbacks();
        let mut writer = ok_or_panic(unsafe { new_dv_writer(&callbacks, &TEST_ERROR_ALLOCATORS) });
        let result =
            ok_or_panic(unsafe { dv_writer_write(&mut writer, dv, &TEST_ERROR_ALLOCATORS) });
        assert_eq!(result.cardinality, 4);
        ok_or_panic(unsafe { dv_writer_finalize(writer, &TEST_ERROR_ALLOCATORS) });
        assert_eq!(output.bytes, serialize(&[&[1, 3, 7, 9]]).0.bytes);
    }

    #[rstest]
    fn abandoned_vectors_are_freed_without_output(#[values(false, true)] populated: bool) {
        let mut dv = new_deletion_vector();
        if populated {
            ok_or_panic(unsafe {
                deletion_vector_add_indexes(&mut dv, rows(&[1, 7]), &TEST_ERROR_ALLOCATORS)
            });
        }
        unsafe { free_deletion_vector(dv) };
    }

    #[test]
    fn empty_writer_finalizes_without_creating_a_vector() {
        let (output, results) = serialize(&[]);
        assert!(output.bytes.is_empty());
        assert!(results.is_empty());
        assert_eq!(output.flushes, 1);
    }

    #[test]
    fn empty_index_chunks_preserve_a_complete_empty_vector() {
        let mut output = Output::default();
        let callbacks = output.callbacks();
        let mut writer = ok_or_panic(unsafe { new_dv_writer(&callbacks, &TEST_ERROR_ALLOCATORS) });
        let mut vector = new_deletion_vector();
        ok_or_panic(unsafe {
            deletion_vector_add_indexes(&mut vector, FfiSlice::empty(), &TEST_ERROR_ALLOCATORS)
        });
        let result =
            ok_or_panic(unsafe { dv_writer_write(&mut writer, vector, &TEST_ERROR_ALLOCATORS) });
        assert_eq!(result.cardinality, 0);
        assert!(ok_or_panic(unsafe {
            dv_writer_finalize(writer, &TEST_ERROR_ALLOCATORS)
        }));
        assert!(!output.bytes.is_empty());
    }

    #[test]
    fn invalid_index_chunk_leaves_the_existing_vector_usable() {
        let mut output = Output::default();
        let callbacks = output.callbacks();
        let mut writer = ok_or_panic(unsafe { new_dv_writer(&callbacks, &TEST_ERROR_ALLOCATORS) });
        let mut vector = vector(&[1]);
        assert_extern_result_error_with_message(
            unsafe {
                deletion_vector_add_indexes(
                    &mut vector,
                    FfiSlice {
                        ptr: ptr::null(),
                        len: 1,
                    },
                    &TEST_ERROR_ALLOCATORS,
                )
            },
            FFIKernelError::GenericError,
            None,
        );
        assert!(output.bytes.is_empty());
        ok_or_panic(unsafe {
            deletion_vector_add_indexes(&mut vector, rows(&[4]), &TEST_ERROR_ALLOCATORS)
        });
        let result =
            ok_or_panic(unsafe { dv_writer_write(&mut writer, vector, &TEST_ERROR_ALLOCATORS) });
        assert_eq!(result.offset, 1);
        assert_eq!(result.cardinality, 2);
        ok_or_panic(unsafe { dv_writer_finalize(writer, &TEST_ERROR_ALLOCATORS) });
        assert_eq!(output.bytes, serialize(&[&[1, 4]]).0.bytes);
    }

    #[rstest]
    #[case::signed_max(i64::MAX as u64, true)]
    #[case::above_signed_max(i64::MAX as u64 + 1, false)]
    #[case::unsigned_max(u64::MAX, false)]
    fn protocol_index_bounds_are_checked_before_mutating_a_chunk(
        #[case] index: u64,
        #[case] accepted: bool,
    ) {
        let mut output = Output::default();
        let callbacks = output.callbacks();
        let mut writer = ok_or_panic(unsafe { new_dv_writer(&callbacks, &TEST_ERROR_ALLOCATORS) });
        let mut dv = vector(&[1]);
        let result = unsafe {
            deletion_vector_add_indexes(&mut dv, rows(&[0, index]), &TEST_ERROR_ALLOCATORS)
        };
        if accepted {
            assert!(ok_or_panic(result));
            let result =
                ok_or_panic(unsafe { dv_writer_write(&mut writer, dv, &TEST_ERROR_ALLOCATORS) });
            assert_eq!(result.cardinality, 3);
            assert_eq!(output.bytes, serialize(&[&[0, 1, index]]).0.bytes);
        } else {
            assert_extern_result_error_with_message(
                result,
                FFIKernelError::DeletionVectorError,
                None,
            );
            // The valid prefix of the rejected chunk must not be added either.
            assert_eq!(unsafe { dv.as_ref() }.cardinality(), 1);
            ok_or_panic(unsafe {
                deletion_vector_add_indexes(&mut dv, rows(&[7]), &TEST_ERROR_ALLOCATORS)
            });
            ok_or_panic(unsafe { dv_writer_write(&mut writer, dv, &TEST_ERROR_ALLOCATORS) });
            assert_eq!(output.bytes, serialize(&[&[1, 7]]).0.bytes);
        }
        ok_or_panic(unsafe { dv_writer_finalize(writer, &TEST_ERROR_ALLOCATORS) });
    }

    fn writer_from_stack_record(output: &mut Output) -> Handle<ExclusiveDvWriter> {
        let callbacks = output.callbacks();
        ok_or_panic(unsafe { new_dv_writer(&callbacks, &TEST_ERROR_ALLOCATORS) })
    }

    #[test]
    fn copied_output_record_can_be_dropped_or_modified_after_creation() {
        let mut output = Output::default();
        let mut writer = writer_from_stack_record(&mut output);
        ok_or_panic(unsafe {
            dv_writer_write(&mut writer, vector(&[1, 3]), &TEST_ERROR_ALLOCATORS)
        });
        ok_or_panic(unsafe { dv_writer_finalize(writer, &TEST_ERROR_ALLOCATORS) });
        assert_eq!(output.bytes, serialize(&[&[1, 3]]).0.bytes);

        let mut output = Output::default();
        let mut callbacks = output.callbacks();
        let mut writer = ok_or_panic(unsafe { new_dv_writer(&callbacks, &TEST_ERROR_ALLOCATORS) });
        callbacks.context = None;
        callbacks.write = None;
        callbacks.flush = None;
        assert!(callbacks.write.is_none());
        ok_or_panic(unsafe { dv_writer_write(&mut writer, vector(&[7]), &TEST_ERROR_ALLOCATORS) });
        ok_or_panic(unsafe { dv_writer_finalize(writer, &TEST_ERROR_ALLOCATORS) });
        assert_eq!(output.bytes, serialize(&[&[7]]).0.bytes);
        assert_eq!(output.flushes, 1);
    }

    unsafe extern "C" fn stateless_write(
        context: NullableCvoid,
        bytes: KernelBytesSlice,
        out: *mut EngineExecResult<usize>,
    ) {
        assert!(context.is_none());
        unsafe { out.write(EngineExecResult::Success(bytes.len)) };
    }

    unsafe extern "C" fn stateless_flush(context: NullableCvoid, out: *mut EngineExecResult<bool>) {
        assert!(context.is_none());
        unsafe { out.write(EngineExecResult::Success(true)) };
    }

    #[test]
    fn stateless_callbacks_accept_a_null_context() {
        let output = FfiDvWriterOutput {
            context: None,
            write: Some(stateless_write),
            flush: Some(stateless_flush),
        };
        let mut writer = ok_or_panic(unsafe { new_dv_writer(&output, &TEST_ERROR_ALLOCATORS) });
        let result = ok_or_panic(unsafe {
            dv_writer_write(&mut writer, vector(&[1, 3]), &TEST_ERROR_ALLOCATORS)
        });
        assert_eq!(result, serialize(&[&[1, 3]]).1[0]);
        ok_or_panic(unsafe { dv_writer_finalize(writer, &TEST_ERROR_ALLOCATORS) });
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
        assert_extern_result_error_with_message(
            unsafe { new_dv_writer(&callbacks, &TEST_ERROR_ALLOCATORS) },
            FFIKernelError::GenericError,
            None,
        );
        assert!(output.bytes.is_empty());
        assert_eq!(output.flushes, 0);
    }

    unsafe extern "C" fn count_kernel_error_allocations(
        context: *mut c_void,
        info: *const KernelErrorInfo,
    ) -> *mut c_void {
        unsafe {
            *context.cast::<usize>() += 1;
            (TEST_ERROR_ALLOCATORS.allocate_kernel_error)(context, info)
        }
    }

    #[test]
    fn standalone_calls_use_the_supplied_error_allocator_context() {
        let mut allocation_calls = 0usize;
        let error_allocators = ErrorAllocators {
            context: (&raw mut allocation_calls).cast(),
            allocate_kernel_error: count_kernel_error_allocations,
            ..TEST_ERROR_ALLOCATORS
        };
        let mut output = Output {
            mode: Some(FailureMode::Write),
            ..Default::default()
        };
        let mut callbacks = output.callbacks();
        callbacks.write = None;
        assert_extern_result_error_with_message(
            unsafe { new_dv_writer(&callbacks, &error_allocators) },
            FFIKernelError::GenericError,
            None,
        );

        let mut dv = new_deletion_vector();
        assert_extern_result_error_with_message(
            unsafe {
                deletion_vector_add_indexes(
                    &mut dv,
                    rows(&[i64::MAX as u64 + 1]),
                    &error_allocators,
                )
            },
            FFIKernelError::DeletionVectorError,
            None,
        );
        unsafe { free_deletion_vector(dv) };

        callbacks.write = Some(write_output);
        let mut writer = ok_or_panic(unsafe { new_dv_writer(&callbacks, &error_allocators) });
        assert_extern_result_error_with_message(
            unsafe { dv_writer_write(&mut writer, vector(&[1]), &error_allocators) },
            FFIKernelError::GenericError,
            None,
        );
        assert_extern_result_error_with_message(
            unsafe { dv_writer_finalize(writer, &error_allocators) },
            FFIKernelError::InvalidTransactionStateError,
            None,
        );
        assert_eq!(allocation_calls, 4);
        assert_eq!(output.flushes, 0);
    }

    #[test]
    fn short_writes_are_completed_by_the_rust_serializer() {
        let mut output = Output {
            max_chunk: Some(2),
            ..Default::default()
        };
        let callbacks = output.callbacks();
        let mut writer = ok_or_panic(unsafe { new_dv_writer(&callbacks, &TEST_ERROR_ALLOCATORS) });
        ok_or_panic(unsafe {
            dv_writer_write(&mut writer, vector(&[0, 7]), &TEST_ERROR_ALLOCATORS)
        });
        ok_or_panic(unsafe { dv_writer_finalize(writer, &TEST_ERROR_ALLOCATORS) });
        assert_eq!(output.bytes, serialize(&[&[0, 7]]).0.bytes);
    }

    #[rstest]
    #[case::write(FailureMode::Write, "test write failure")]
    #[case::partial_output(FailureMode::WriteAfterPrefix, "test write failure")]
    #[case::zero(FailureMode::ZeroWrite, "Failed to write")]
    #[case::invalid_count(FailureMode::InvalidWriteCount, "exceeds input")]
    #[case::unset(FailureMode::UnsetWriteResult, "left result unset")]
    fn failed_writes_cannot_be_resumed(
        #[case] mode: FailureMode,
        #[case] message: &str,
        #[values(false, true)] free_after_failure: bool,
    ) {
        let mut output = Output {
            mode: Some(mode),
            ..Default::default()
        };
        let callbacks = output.callbacks();
        let mut writer = ok_or_panic(unsafe { new_dv_writer(&callbacks, &TEST_ERROR_ALLOCATORS) });
        assert_extern_result_error_contains(
            unsafe { dv_writer_write(&mut writer, vector(&[1]), &TEST_ERROR_ALLOCATORS) },
            FFIKernelError::GenericError,
            message,
        );
        if matches!(mode, FailureMode::WriteAfterPrefix) {
            assert!(!output.bytes.is_empty());
        }
        // SAFETY: this test owns the context; no callback is running and no reference is retained.
        unsafe { (*callbacks.context.unwrap().as_ptr().cast::<Output>()).mode = None };
        assert_extern_result_error_with_message(
            unsafe { dv_writer_write(&mut writer, vector(&[1]), &TEST_ERROR_ALLOCATORS) },
            FFIKernelError::InvalidTransactionStateError,
            None,
        );
        if free_after_failure {
            unsafe { free_dv_writer(writer) };
        } else {
            assert_extern_result_error_with_message(
                unsafe { dv_writer_finalize(writer, &TEST_ERROR_ALLOCATORS) },
                FFIKernelError::InvalidTransactionStateError,
                None,
            );
        }
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
        let callbacks = output.callbacks();
        let mut writer = ok_or_panic(unsafe { new_dv_writer(&callbacks, &TEST_ERROR_ALLOCATORS) });
        ok_or_panic(unsafe { dv_writer_write(&mut writer, vector(&[1]), &TEST_ERROR_ALLOCATORS) });
        assert_extern_result_error_contains(
            unsafe { dv_writer_finalize(writer, &TEST_ERROR_ALLOCATORS) },
            FFIKernelError::GenericError,
            message,
        );
        assert_eq!(output.flushes, 1);
    }

    #[test]
    fn free_abandons_output_without_flushing() {
        let mut output = Output::default();
        let callbacks = output.callbacks();
        let mut writer = ok_or_panic(unsafe { new_dv_writer(&callbacks, &TEST_ERROR_ALLOCATORS) });
        ok_or_panic(unsafe { dv_writer_write(&mut writer, vector(&[1]), &TEST_ERROR_ALLOCATORS) });
        unsafe { free_dv_writer(writer) };
        assert!(!output.bytes.is_empty());
        assert_eq!(output.flushes, 0);
    }

    #[test]
    fn writer_can_move_threads_while_its_output_remains_exclusive() {
        let mut output = Output::default();
        let callbacks = output.callbacks();
        let writer = ok_or_panic(unsafe { new_dv_writer(&callbacks, &TEST_ERROR_ALLOCATORS) });
        let result = std::thread::spawn(move || {
            let mut writer = writer;
            let result = ok_or_panic(unsafe {
                dv_writer_write(&mut writer, vector(&[1, 3]), &TEST_ERROR_ALLOCATORS)
            });
            ok_or_panic(unsafe { dv_writer_finalize(writer, &TEST_ERROR_ALLOCATORS) });
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
    // Exercises safe Engine/object-store setup, not the FFI lifetime boundary; runs natively.
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

    #[cfg(feature = "default-engine-base")]
    #[rstest]
    #[case::shared_output(true)]
    #[case::separate_outputs(false)]
    #[tokio::test]
    // Parquet/Engine transaction integration is expensive under Miri and tested natively.
    #[cfg_attr(miri, ignore)]
    async fn serialized_results_stage_with_new_files_and_commit_through_ffi(
        #[case] shared: bool,
    ) -> KernelResult<()> {
        let (store, engine, root) = test_utils::engine_store_setup("ffi_dv_commit", None);
        let engine = Arc::new(engine);
        let snapshot = test_utils::create_table_and_load_snapshot(
            root.as_str(),
            schema_ref! { nullable "id": INTEGER },
            engine.as_ref(),
            &[("delta.enableDeletionVectors", "true")],
        )?;
        let mut initial = snapshot
            .update_table(UpdateTableOperation::Write, "dv-test", true)
            .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
        let mut original_files = Vec::new();
        let mut adds = Vec::new();
        for (path, ids) in [
            ("first.parquet", vec![10, 20, 30, 40]),
            ("second.parquet", vec![50, 60]),
        ] {
            let batch =
                RecordBatch::try_from_iter([("id", Arc::new(Int32Array::from(ids)) as ArrayRef)])?;
            let bytes = test_utils::record_batch_to_bytes(&batch);
            let object_path = Path::from_url_path(root.join(path)?.path())?;
            store.put(&object_path, bytes.clone().into()).await?;
            adds.push((
                path,
                bytes.len() as i64,
                1_000_000,
                Some(batch.num_rows() as i64),
            ));
            original_files.push((object_path, bytes));
        }
        initial.add_files(test_utils::create_add_files_metadata(
            initial.add_files_schema(),
            adds,
        )?);
        let snapshot = initial
            .commit(engine.as_ref())?
            .unwrap_post_commit_snapshot();
        let original_version = snapshot.version();
        let handle: Handle<SharedSnapshot> = snapshot.clone().into();
        let ffi_engine = engine_handle_for_store(store.clone());
        let engine_info = "dv-test";
        let builder = ok_or_panic(unsafe {
            new_update_table_txn_builder(
                handle.shallow_copy(),
                KernelUpdateTableOperation::Merge,
                kernel_string_slice!(engine_info),
                true,
                ffi_engine.shallow_copy(),
            )
        });
        let txn = ok_or_panic(unsafe {
            update_table_txn_builder_build(builder, ffi_engine.shallow_copy())
        });
        let map = dv_descriptor_map_new();
        let vectors: &[&[u64]] = &[&[1, 2], &[0]];
        let paths = ["first.parquet", "second.parquet"];
        let groups: Vec<&[&[u64]]> = if shared {
            vec![vectors]
        } else {
            vectors.chunks(1).collect()
        };
        let mut data_index = 0;
        for (output_index, group) in groups.iter().enumerate() {
            let (output, results) = serialize(group);
            let output_path = root.join(&format!("serialized-{output_index}.bin"))?;
            store
                .put(
                    &Path::from_url_path(output_path.path())?,
                    output.bytes.into(),
                )
                .await?;
            let output_url = output_path.as_str();
            for result in results {
                let descriptor = ok_or_panic(unsafe {
                    dv_descriptor_new(
                        KernelDvStorageType::PersistedAbsolute as i32,
                        kernel_string_slice!(output_url),
                        true,
                        result.offset,
                        result.size_in_bytes,
                        result.cardinality,
                        ffi_engine.shallow_copy(),
                    )
                });
                let data_path = paths[data_index];
                ok_or_panic(unsafe {
                    dv_descriptor_map_insert(
                        map.shallow_copy(),
                        kernel_string_slice!(data_path),
                        descriptor,
                        ffi_engine.shallow_copy(),
                    )
                });
                data_index += 1;
            }
        }
        let replacement = RecordBatch::try_from_iter([(
            "id",
            Arc::new(Int32Array::from(vec![200, 300, 500])) as ArrayRef,
        )])?;
        let bytes = test_utils::record_batch_to_bytes(&replacement);
        let replacement_path = "replacement.parquet";
        store
            .put(
                &Path::from_url_path(root.join(replacement_path)?.path())?,
                bytes.clone().into(),
            )
            .await?;
        let adds = test_utils::create_add_files_metadata(
            unsafe { txn.as_ref() }.add_files_schema(),
            vec![(replacement_path, bytes.len() as i64, 1_000_000, Some(3))],
        )?;
        unsafe { update_table_txn_add_files(txn.shallow_copy(), adds.into()) };
        let scan_handle = ok_or_panic(unsafe {
            scan(handle.shallow_copy(), ffi_engine.shallow_copy(), None, None)
        });
        let metadata = ok_or_panic(unsafe {
            scan_metadata_iter_init(ffi_engine.shallow_copy(), scan_handle.shallow_copy())
        });
        ok_or_panic(unsafe {
            update_table_txn_update_deletion_vectors(
                txn.shallow_copy(),
                map,
                metadata,
                ffi_engine.shallow_copy(),
            )
        });
        assert_eq!(
            snapshot
                .clone()
                .scan_builder()
                .build()?
                .execute(engine.clone())?
                .map(|batch| batch.map(|data| data.len()))
                .collect::<KernelResult<Vec<_>>>()?
                .into_iter()
                .sum::<usize>(),
            6
        );
        let committed =
            ok_or_panic(unsafe { update_table_txn_commit(txn, ffi_engine.shallow_copy()) });
        assert_eq!(
            unsafe { committed_transaction_version(&committed) },
            original_version + 1
        );
        let current = delta_kernel::Snapshot::builder_for(&root).build(engine.as_ref())?;
        let mut ids = Vec::new();
        for batch in current.scan_builder().build()?.execute(engine.clone())? {
            let batch = test_utils::into_record_batch(batch?);
            ids.extend(
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied(),
            );
        }
        ids.sort_unstable();
        assert_eq!(ids, vec![10, 40, 60, 200, 300, 500]);
        for (path, bytes) in original_files {
            assert_eq!(store.get(&path).await?.bytes().await?.as_ref(), bytes);
        }
        unsafe {
            free_committed_transaction(committed);
            free_scan(scan_handle);
            free_snapshot(handle);
            free_engine(ffi_engine);
        }
        Ok(())
    }
}
