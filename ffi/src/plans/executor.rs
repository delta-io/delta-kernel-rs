//! This module provides an FFI-backed implementation of the [`PlanExecutor`] trait (allowing plan
//! execution to happen outside of Rust).
use std::sync::Arc;

use delta_kernel::plans::proto::schema as proto_schema;
use delta_kernel::schema::StructType;
use delta_kernel::{DeltaResult, Error, Operation, ParquetFooter, PlanExecutor, PlanResult};
use delta_kernel_ffi_macros::handle_descriptor;
use prost::Message as _;

use crate::error::EngineExecResult;
use crate::plans::iter::{FfiBytesIter, FfiEngineDataIter, FfiFileMetaIter};
use crate::plans::result::{CParquetFooter, CPlanResult};
use crate::{kernel_bytes_slice, KernelBytesSlice, NullableCvoid};

/// A shared (`Arc`-like) handle to an [`PlanExecutor`].
#[handle_descriptor(target=dyn PlanExecutor, mutable=false)]
pub struct SharedPlanExecutor;

/// C callback, provided by the Engine, for executing an [`Operation`].
///
/// `context` - an opaque pointer, originally passed to
/// [`get_plan_executor`](super::get_plan_executor).
/// `plan_proto` - a byte slice containing the proto-serialized representation of an [`Operation`]
/// `out` - an out pointer into which the engine writes the result.
///
/// Since the out result is written to caller (Kernel) provided memory, the kernel will also be
/// responsible for freeing it. Kernel will pre-initialize the out pointer to
/// [`EngineExecResult::Uninit`] before handing it to the engine upcall.
pub type CExecuteOpFn = extern "C" fn(
    context: NullableCvoid,
    plan_proto: KernelBytesSlice,
    out: *mut EngineExecResult<CPlanResult>,
);

/// A [`PlanExecutor`] implementation that forwards each [`Operation`] to a C callback.
///
/// Constructed and managed via [`get_plan_executor`](super::get_plan_executor) and
/// [`free_plan_executor`](super::free_plan_executor).
pub struct FfiPlanExecutor {
    context: NullableCvoid,
    callback: CExecuteOpFn,
}

impl FfiPlanExecutor {
    /// Construct an [`FfiPlanExecutor`] from a context pointer and C callback.
    ///
    /// The `context` pointer and `callback` must satisfy the thread-safety and lifetime
    /// requirements documented on [`get_plan_executor`](super::get_plan_executor).
    pub(crate) fn new(context: NullableCvoid, callback: CExecuteOpFn) -> Self {
        Self { context, callback }
    }
}

// SAFETY: the engine is expected to provide thread-safe context + callback function pointers.
unsafe impl Send for FfiPlanExecutor {}
unsafe impl Sync for FfiPlanExecutor {}

impl PlanExecutor for FfiPlanExecutor {
    fn execute_op(&self, op: Operation) -> DeltaResult<PlanResult> {
        let plan_proto_bytes = op.to_proto_bytes();
        let plan_proto_slice = kernel_bytes_slice!(plan_proto_bytes);

        let mut out = EngineExecResult::Uninit;
        (self.callback)(self.context, plan_proto_slice, &mut out);
        let plan_result =
            match out {
                EngineExecResult::Success(plan) => plan,
                EngineExecResult::Failure(err) => return Err(err.into()),
                EngineExecResult::Uninit => return Err(Error::internal_error(
                    "FFI engine returned from execute_op upcall without writing the plan result",
                )),
            };
        match plan_result {
            CPlanResult::Unit => Ok(PlanResult::Unit),
            CPlanResult::Data(it) => Ok(PlanResult::Data(Box::new(FfiEngineDataIter::new(it)))),
            CPlanResult::FileMeta(it) => {
                Ok(PlanResult::FileMeta(Box::new(FfiFileMetaIter::new(it))))
            }
            CPlanResult::Bytes(it) => Ok(PlanResult::Bytes(Box::new(FfiBytesIter::new(it)))),
            CPlanResult::ParquetFooter(footer) => {
                Ok(PlanResult::ParquetFooter(decode_parquet_footer(footer)?))
            }
        }
    }
}

/// Convert a [`CParquetFooter`] into a kernel [`ParquetFooter`].
///
/// Consumes the embedded [`ExclusiveRustBytes`](crate::ExclusiveRustBytes) handle carrying
/// the proto-serialized schema, returning an error if the bytes are not a valid schema proto
/// message.
fn decode_parquet_footer(footer: CParquetFooter) -> DeltaResult<ParquetFooter> {
    let CParquetFooter { schema_proto } = footer;
    // SAFETY: ExclusiveRustBytes should only have a single owner, so consuming here is safe.
    let bytes = *unsafe { schema_proto.into_inner() };
    let proto = proto_schema::StructType::decode(bytes.as_slice()).map_err(Error::generic_err)?;
    let schema = Arc::new(StructType::try_from(proto)?);
    Ok(ParquetFooter { schema })
}

#[cfg(test)]
mod tests {
    use std::ffi::c_void;
    use std::ptr::NonNull;
    use std::sync::{Arc, Mutex};

    use delta_kernel::arrow::array::ffi::FFI_ArrowArray;
    use delta_kernel::plans::proto::{operation as proto, schema as proto_schema};
    use delta_kernel::schema::{DataType as KernelDataType, StructField, StructType};
    use delta_kernel::Error;
    use prost::Message;
    use url::Url;

    use super::*;
    use crate::error::{EngineExecError, KernelError};
    use crate::handle::Handle;
    use crate::plans::get_plan_executor;
    use crate::plans::iter::{CBytesIterator, CEngineDataIterator, CFileMetaIterator};
    use crate::plans::result::CParquetFooter;
    use crate::{
        allocate_kernel_bytes, kernel_bytes_slice, ExclusiveEngineData, ExclusiveRustString,
        OptionalValue,
    };

    extern "C" fn noop_free(_state: NullableCvoid) {}

    /// Mock callback that pulls a pre-configured `CPlanResult` out of a `Mutex<Option<_>>`
    /// stashed in `context` and writes it into the out pointer as an `EngineExecResult::Success`.
    extern "C" fn mock_execute_op(
        context: NullableCvoid,
        _plan_proto: KernelBytesSlice,
        out: *mut EngineExecResult<CPlanResult>,
    ) {
        let cell = unsafe { &*(context.unwrap().as_ptr() as *const Mutex<Option<CPlanResult>>) };
        let plan = cell
            .lock()
            .unwrap()
            .take()
            .expect("mock_execute_op invoked more than once");
        unsafe { out.write(EngineExecResult::Success(plan)) };
    }

    /// Executes a dummy plan operation against a `PlanExecutor` whose callback returns the given
    /// `expected_plan_result`. Returns the resulting `PlanResult` for furhter validation.
    fn execute_dummy_op(expected_plan_result: CPlanResult) -> PlanResult {
        let cell: Mutex<Option<CPlanResult>> = Mutex::new(Some(expected_plan_result));
        let context = NonNull::new(&cell as *const Mutex<Option<CPlanResult>> as *mut c_void);
        let executor = unsafe { get_plan_executor(context, mock_execute_op) };
        let plan_executor: Arc<dyn PlanExecutor> = unsafe { executor.into_inner() };

        // Construct a dummy op because we don't actually care about the plan bytes here.
        let url = Url::parse("memory:///table/").unwrap();
        let op = Operation::IoOperation(delta_kernel::IoOperation::file_listing(url));

        plan_executor.execute_op(op).expect("execute_op succeeds")
    }

    #[test]
    fn execute_op_unit_variant() {
        execute_dummy_op(CPlanResult::Unit)
            .into_unit()
            .expect("Unit variant");
    }

    /// A callback that writes an `EngineExecResult::Failure` must surface as the matching kernel
    /// error, preserving the engine-supplied message.
    #[test]
    fn execute_op_surfaces_engine_failure() {
        extern "C" fn fail_execute_op(
            _context: NullableCvoid,
            _plan_proto: KernelBytesSlice,
            out: *mut EngineExecResult<CPlanResult>,
        ) {
            // Mirror the engine downcalling `allocate_kernel_string` to build the message handle.
            let message: Handle<ExclusiveRustString> = Box::new("kaboom".to_string()).into();
            let err = EngineExecError {
                etype: KernelError::UnsupportedError,
                message,
            };
            unsafe { out.write(EngineExecResult::Failure(err)) };
        }

        let executor = unsafe { get_plan_executor(None, fail_execute_op) };
        let plan_executor: Arc<dyn PlanExecutor> = unsafe { executor.into_inner() };

        let url = Url::parse("memory:///table/").unwrap();
        let op = Operation::IoOperation(delta_kernel::IoOperation::file_listing(url));

        let Err(err) = plan_executor.execute_op(op) else {
            panic!("execute_op should surface the engine failure");
        };
        assert!(
            matches!(err, Error::Unsupported(ref msg) if msg == "kaboom"),
            "expected Error::Unsupported(\"kaboom\"), got {err:?}"
        );
    }

    /// A callback that returns without writing the out pointer must surface as an internal error
    #[test]
    fn execute_op_surfaces_uninitialized_out() {
        extern "C" fn noop_execute_op(
            _context: NullableCvoid,
            _plan_proto: KernelBytesSlice,
            _out: *mut EngineExecResult<CPlanResult>,
        ) {
        }

        let executor = unsafe { get_plan_executor(None, noop_execute_op) };
        let plan_executor: Arc<dyn PlanExecutor> = unsafe { executor.into_inner() };

        let url = Url::parse("memory:///table/").unwrap();
        let op = Operation::IoOperation(delta_kernel::IoOperation::file_listing(url));

        let Err(err) = plan_executor.execute_op(op) else {
            panic!("execute_op should surface an error when the engine does not write the result");
        };
        assert!(
            err.to_string().contains(
                "FFI engine returned from execute_op upcall without writing the plan result"
            ),
            "expected the engine-did-not-write message, got {err}"
        );
    }

    #[test]
    fn execute_op_data_variant() {
        extern "C" fn empty_data_next(
            _state: NullableCvoid,
            out: *mut OptionalValue<EngineExecResult<Handle<ExclusiveEngineData>>>,
        ) {
            unsafe { out.write(OptionalValue::None) };
        }

        let iter = CEngineDataIterator {
            state: None,
            next: empty_data_next,
            free: noop_free,
        };
        let mut data_iter = execute_dummy_op(CPlanResult::Data(iter))
            .into_data()
            .expect("Data variant");
        assert!(data_iter.next().is_none(), "empty data iterator");
    }

    #[test]
    fn execute_op_file_meta_variant() {
        extern "C" fn empty_file_meta_next(
            _state: NullableCvoid,
            out: *mut OptionalValue<EngineExecResult<FFI_ArrowArray>>,
        ) {
            unsafe { out.write(OptionalValue::None) };
        }

        let iter = CFileMetaIterator {
            state: None,
            next: empty_file_meta_next,
            free: noop_free,
        };
        let mut file_meta_iter = execute_dummy_op(CPlanResult::FileMeta(iter))
            .into_file_meta()
            .expect("FileMeta variant");
        assert!(file_meta_iter.next().is_none(), "empty file meta iterator");
    }

    #[test]
    fn execute_op_bytes_variant() {
        extern "C" fn empty_bytes_next(
            _state: NullableCvoid,
            out: *mut OptionalValue<EngineExecResult<FFI_ArrowArray>>,
        ) {
            unsafe { out.write(OptionalValue::None) };
        }

        let iter = CBytesIterator {
            state: None,
            next: empty_bytes_next,
            free: noop_free,
        };
        let mut bytes_iter = execute_dummy_op(CPlanResult::Bytes(iter))
            .into_bytes()
            .expect("Bytes variant");
        assert!(bytes_iter.next().is_none(), "empty bytes iterator");
    }

    #[test]
    fn execute_op_parquet_footer_variant() {
        let schema =
            StructType::try_new(vec![StructField::nullable("id", KernelDataType::INTEGER)])
                .unwrap();
        let bytes = proto_schema::StructType::from(&schema).encode_to_vec();
        let schema_proto = unsafe { allocate_kernel_bytes(kernel_bytes_slice!(bytes)) };
        let footer = CParquetFooter { schema_proto };
        let footer = execute_dummy_op(CPlanResult::ParquetFooter(footer))
            .into_parquet_footer()
            .expect("ParquetFooter variant");
        assert_eq!(footer.schema.fields().count(), 1);
        let id_field = footer.schema.field("id").expect("id field");
        assert_eq!(id_field.data_type(), &KernelDataType::INTEGER);
        assert!(id_field.is_nullable());
    }

    #[test]
    fn execute_op_passes_serialized_operation_bytes() {
        extern "C" fn validate_op(
            _context: NullableCvoid,
            plan_proto: KernelBytesSlice,
            out: *mut EngineExecResult<CPlanResult>,
        ) {
            let bytes = unsafe { std::slice::from_raw_parts(plan_proto.ptr, plan_proto.len) };
            let op = proto::Operation::decode(bytes).expect("decode Operation proto");
            let Some(proto::operation::Op::Io(io)) = op.op else {
                panic!("expected an IoOperation");
            };
            let Some(proto::io_operation::Op::FileListing(file_listing)) = io.op else {
                panic!("expected a FileListing");
            };
            assert_eq!(file_listing.url, "memory:///table/");
            unsafe { out.write(EngineExecResult::Success(CPlanResult::Unit)) };
        }

        let executor = unsafe { get_plan_executor(None, validate_op) };
        let plan_executor: Arc<dyn PlanExecutor> = unsafe { executor.into_inner() };

        let url = Url::parse("memory:///table/").unwrap();
        let op = Operation::IoOperation(delta_kernel::IoOperation::file_listing(url));

        plan_executor
            .execute_op(op)
            .expect("execute_op succeeds")
            .into_unit()
            .expect("Unit variant");
    }
}
