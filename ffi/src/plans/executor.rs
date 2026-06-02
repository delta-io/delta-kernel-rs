//! This module provides an FFI-backed implementation of the [`PlanExecutor`] trait (allowing plan
//! execution to happen outside of Rust).
use std::sync::Arc;

use delta_kernel::{DeltaResult, Operation, ParquetFooter, PlanExecutor, PlanResult};
use delta_kernel_ffi_macros::handle_descriptor;

use crate::error::ExternResult;
use crate::plans::iter::{FfiBytesIter, FfiEngineDataIter, FfiFileMetaIter};
use crate::plans::result::{
    engine_error_to_kernel, CParquetFooter, CPlanResult, CPlanResultWrapper, PlanResultCleanup,
};
use crate::schema_visitor::{extract_kernel_schema, KernelSchemaVisitorState};
use crate::{KernelBytesSlice, NullableCvoid};

/// A shared (`Arc`-like) handle to an [`PlanExecutor`].
#[handle_descriptor(target=dyn PlanExecutor, mutable=false)]
pub struct SharedPlanExecutor;

/// C callback, provided by the Engine, for executing an [`Operation`].
///
/// `context` - an opaque pointer, originally passed to
/// [`get_plan_executor`](super::get_plan_executor).
/// `plan_proto` - a byte slice containing the proto-serialized representation of an [`Operation`]
///
/// The returned [`CPlanResultWrapper`] is consumed by kernel and should be freed according to
/// the rules documented on [`CPlanResult`] and [`CPlanResultWrapper`].
pub type CExecuteOpFn =
    extern "C" fn(context: NullableCvoid, plan_proto: KernelBytesSlice) -> CPlanResultWrapper;

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
    fn execute_op(&self, _op: Operation) -> DeltaResult<PlanResult> {
        // TODO: serialize `_op` to bytes once proto schema is checked in.
        let plan_proto_bytes: &[u8] = &[];
        let plan_proto_slice = unsafe { KernelBytesSlice::new_unsafe(plan_proto_bytes) };

        let result_wrapper = (self.callback)(self.context, plan_proto_slice);

        // Generate the cleanup guard immediately to guarantee that plan result resources are
        // freed when either:
        // - this fn returns (e.g. on the `Err` branch, or on the non-iterator `Ok` variants), or
        // - the iterator adapters are dropped (by forwarding this guard to them)
        let cleanup = PlanResultCleanup::new(result_wrapper.state, result_wrapper.free);
        let plan_result = match result_wrapper.result {
            ExternResult::Ok(plan) => plan,
            ExternResult::Err(err) => return Err(engine_error_to_kernel(err)),
        };
        match plan_result {
            CPlanResult::Unit => Ok(PlanResult::Unit),
            CPlanResult::Data(it) => Ok(PlanResult::Data(Box::new(FfiEngineDataIter::new(
                it, cleanup,
            )))),
            CPlanResult::FileMeta(it) => Ok(PlanResult::FileMeta(Box::new(FfiFileMetaIter::new(
                it, cleanup,
            )))),
            CPlanResult::Bytes(it) => {
                Ok(PlanResult::Bytes(Box::new(FfiBytesIter::new(it, cleanup))))
            }
            CPlanResult::ParquetFooter(footer) => {
                Ok(PlanResult::ParquetFooter(decode_parquet_footer(footer)?))
            }
        }
    }
}

/// Convert a [`CParquetFooter`] into a kernel [`ParquetFooter`]
///
/// Returns an error if schema visiting produces an invalid schema.
fn decode_parquet_footer(footer: CParquetFooter) -> DeltaResult<ParquetFooter> {
    let CParquetFooter { schema } = footer;
    let mut visitor_state = KernelSchemaVisitorState::default();
    let schema_id = (schema.visitor)(schema.schema, &mut visitor_state);
    let schema = extract_kernel_schema(&mut visitor_state, schema_id)?;
    Ok(ParquetFooter {
        schema: Arc::new(schema),
    })
}

#[cfg(test)]
mod tests {
    use std::ffi::c_void;
    use std::ptr::NonNull;
    use std::sync::Mutex;

    use delta_kernel::arrow::array::ffi::FFI_ArrowArray;
    use delta_kernel::schema::DataType as KernelDataType;
    use url::Url;

    use super::*;
    use crate::ffi_test_utils::{allocate_err, ok_or_panic};
    use crate::handle::Handle;
    use crate::plans::get_plan_executor;
    use crate::plans::iter::{CBytesIterator, CEngineDataIterator, CFileMetaIterator};
    use crate::plans::result::CParquetFooter;
    use crate::scan::EngineSchema;
    use crate::schema_visitor::{visit_field_integer, visit_field_struct};
    use crate::{kernel_string_slice, ExclusiveEngineData, OptionalValue};

    extern "C" fn noop_free(_state: NullableCvoid) {}

    /// Mock callback that pulls a pre-configured `CPlanResult` out of a `Mutex<Option<_>>`
    /// stashed in `context`. Hands back a no-op `free` since the tests aren't intended to validate
    /// cleanup behavior.
    extern "C" fn mock_execute_op(
        context: NullableCvoid,
        _plan_proto: KernelBytesSlice,
    ) -> CPlanResultWrapper {
        let cell = unsafe { &*(context.unwrap().as_ptr() as *const Mutex<Option<CPlanResult>>) };
        let plan = cell
            .lock()
            .unwrap()
            .take()
            .expect("mock_execute_op invoked more than once");
        CPlanResultWrapper {
            result: ExternResult::Ok(plan),
            state: None,
            free: noop_free,
        }
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

    #[test]
    fn execute_op_data_variant() {
        extern "C" fn empty_data_next(
            _state: NullableCvoid,
        ) -> OptionalValue<ExternResult<Handle<ExclusiveEngineData>>> {
            OptionalValue::None
        }

        let iter = CEngineDataIterator {
            state: None,
            next: empty_data_next,
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
        ) -> OptionalValue<ExternResult<FFI_ArrowArray>> {
            OptionalValue::None
        }

        let iter = CFileMetaIterator {
            state: None,
            next: empty_file_meta_next,
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
        ) -> OptionalValue<ExternResult<FFI_ArrowArray>> {
            OptionalValue::None
        }

        let iter = CBytesIterator {
            state: None,
            next: empty_bytes_next,
        };
        let mut bytes_iter = execute_dummy_op(CPlanResult::Bytes(iter))
            .into_bytes()
            .expect("Bytes variant");
        assert!(bytes_iter.next().is_none(), "empty bytes iterator");
    }

    /// Schema visitor that produces `{id: integer (nullable)}`.
    extern "C" fn visit_id_only_schema(
        _schema_ptr: *mut c_void,
        state: &mut KernelSchemaVisitorState,
    ) -> usize {
        let id = "id";
        let id_field_id = unsafe {
            ok_or_panic(visit_field_integer(
                state,
                kernel_string_slice!(id),
                true,
                allocate_err,
            ))
        };
        let field_ids = [id_field_id];
        let schema = "schema";
        unsafe {
            ok_or_panic(visit_field_struct(
                state,
                kernel_string_slice!(schema),
                field_ids.as_ptr(),
                1,
                false,
                allocate_err,
            ))
        }
    }

    #[test]
    fn execute_op_parquet_footer_variant() {
        let footer = CParquetFooter {
            schema: EngineSchema {
                schema: std::ptr::null_mut(),
                visitor: visit_id_only_schema,
            },
        };
        let footer = execute_dummy_op(CPlanResult::ParquetFooter(footer))
            .into_parquet_footer()
            .expect("ParquetFooter variant");
        assert_eq!(footer.schema.fields().count(), 1);
        let id_field = footer.schema.field("id").expect("id field");
        assert_eq!(id_field.data_type(), &KernelDataType::INTEGER);
        assert!(id_field.is_nullable());
    }
}
