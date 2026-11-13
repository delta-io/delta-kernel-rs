//! Retained relations and scoped executor callbacks.

use std::sync::Arc;

use delta_kernel::plans::proto::schema as proto_schema;
use delta_kernel::plans::{Plan, RelationRef, ScopedPlanExecutor};
use delta_kernel::schema::StructType;
use delta_kernel::{KernelError, KernelResult, Operation, PlanExecutor, PlanResult};
use delta_kernel_ffi_macros::handle_descriptor;
use prost::Message;

use super::executor::{CExecuteOpFn, FfiPlanExecutor};
use crate::error::{EngineExecResult, ExternResult, IntoExternResult};
use crate::handle::Handle;
use crate::{
    ErrorAllocators, KernelBytesSlice, KernelOwnedBytes, KernelStringSlice, NullableCvoid,
    OptionalValue, SharedExternEngine, TryFromStringSlice, kernel_bytes_slice, kernel_string_slice,
};

/// Shared handle to an identifier and schema. Freeing it does not release the engine's rows.
#[handle_descriptor(target=RelationRef, mutable=false, sized=true)]
pub struct SharedRelationRef;

/// Shared handle to an executor scope that owns retained rows.
#[handle_descriptor(target=dyn ScopedPlanExecutor, mutable=false)]
pub struct SharedScopedPlanExecutor;

/// Executes a query encoded as an Operation protobuf and retains its rows in this scope.
///
/// All inputs are borrowed for the callback. Write an owned relation handle to `out` on success;
/// Kernel consumes it. The returned schema must equal the query's output schema. Initialize the
/// result even on failure and never unwind across the C boundary.
pub type CExecuteAndRetainFn = extern "C" fn(
    context: NullableCvoid,
    name: KernelStringSlice,
    operation: KernelBytesSlice,
    out: *mut EngineExecResult<Handle<SharedRelationRef>>,
);

struct FfiScopedPlanExecutor {
    executor: FfiPlanExecutor,
    context: NullableCvoid,
    retain: CExecuteAndRetainFn,
    free: extern "C" fn(NullableCvoid),
}

// SAFETY: construction requires thread-safe callbacks and context valid until the free callback.
unsafe impl Send for FfiScopedPlanExecutor {}
unsafe impl Sync for FfiScopedPlanExecutor {}

impl PlanExecutor for FfiScopedPlanExecutor {
    fn execute_op(&self, op: Operation) -> KernelResult<PlanResult> {
        self.executor.execute_op(op)
    }
}

impl ScopedPlanExecutor for FfiScopedPlanExecutor {
    fn execute_and_retain(&self, name: &str, plan: Plan) -> KernelResult<RelationRef> {
        let expected_schema = plan.schema.clone();
        let bytes = Operation::QueryPlan(plan).to_proto_bytes();
        let mut out = EngineExecResult::Uninit;
        (self.retain)(
            self.context,
            kernel_string_slice!(name),
            kernel_bytes_slice!(bytes),
            &mut out,
        );
        let relation = match out {
            EngineExecResult::Success(handle) => unsafe { handle.into_inner() },
            EngineExecResult::Failure(error) => return Err(error.into()),
            EngineExecResult::Uninit => {
                return Err(KernelError::internal_error(
                    "FFI engine did not write the retained relation result",
                ));
            }
        };
        if relation.schema() != &expected_schema {
            return Err(KernelError::internal_error(
                "Retained relation schema differs from query output",
            ));
        }
        Ok(relation.as_ref().clone())
    }
}

impl Drop for FfiScopedPlanExecutor {
    fn drop(&mut self) {
        (self.free)(self.context);
    }
}

/// Creates a scoped executor. Its final release invokes `free(context)` to release retained rows.
///
/// # Safety
///
/// Callbacks and context must be thread-safe and valid until `free` runs. Callbacks must not
/// unwind. The scope must remain alive while any of its RelationRefs are used; engines must reject
/// foreign or expired identifiers. Kernel does not transfer ownership of the context before this
/// call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn get_scoped_plan_executor(
    context: NullableCvoid,
    execute: CExecuteOpFn,
    retain: CExecuteAndRetainFn,
    free: extern "C" fn(NullableCvoid),
) -> Handle<SharedScopedPlanExecutor> {
    let executor: Arc<dyn ScopedPlanExecutor> = Arc::new(FfiScopedPlanExecutor {
        executor: FfiPlanExecutor::new(context, execute),
        context,
        retain,
        free,
    });
    executor.into()
}

/// Releases a scope handle. The final release calls the engine's scope cleanup callback.
///
/// # Safety
/// `executor` must be valid and is consumed. Do not use it afterward.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn free_scoped_plan_executor(executor: Handle<SharedScopedPlanExecutor>) {
    unsafe { executor.drop_handle() }
}

/// Creates a plan-based engine sharing the executor scope, with an optional fallback engine.
///
/// All input handles are borrowed. The returned engine retains the scope independently, so final
/// scope cleanup waits for the engine and any prepared/result owners to be released.
///
/// # Safety
/// Handles must be valid. `error_allocators` must point to initialized allocators whose callbacks
/// and context satisfy their lifetime and concurrency requirements for every use of the engine.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn get_plan_based_engine_from_scope(
    executor: Handle<SharedScopedPlanExecutor>,
    fallback: OptionalValue<Handle<SharedExternEngine>>,
    error_allocators: *const ErrorAllocators,
) -> Handle<SharedExternEngine> {
    let executor: Arc<dyn PlanExecutor> = unsafe { executor.clone_as_arc() };
    unsafe { super::get_plan_based_engine(executor.into(), fallback, error_allocators) }
}

/// Constructs a relation identifier from UTF-8 `id` and a StructType protobuf `schema`.
///
/// This copies the identifier and schema, without reading or retaining rows. The engine must
/// already own the corresponding rows within its scope. Returns decoding errors through `engine`.
///
/// # Safety
/// `engine` and both slices must be valid and are borrowed for the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn new_relation_ref(
    id: KernelStringSlice,
    schema: KernelBytesSlice,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<SharedRelationRef>> {
    let engine = unsafe { engine.as_ref() };
    let result: KernelResult<Handle<SharedRelationRef>> = (|| {
        let id: String = unsafe { String::try_from_slice(&id) }?;
        let proto = proto_schema::StructType::decode(unsafe { schema.try_as_slice() }?)
            .map_err(KernelError::generic_err)?;
        let schema = Arc::new(StructType::try_from(proto)?);
        Ok(Arc::new(RelationRef::new(id, schema)).into())
    })();
    unsafe { result.into_extern_result(&engine) }
}

/// Borrows the relation identifier. The slice remains valid until this handle is released.
///
/// # Safety
/// `relation` must be valid and remain alive while the returned slice is used.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn relation_ref_id(relation: Handle<SharedRelationRef>) -> KernelStringSlice {
    let relation = unsafe { relation.as_ref() };
    let id = relation.id();
    kernel_string_slice!(id)
}

/// Returns an owned StructType protobuf. Release it with `free_kernel_bytes`.
///
/// # Safety
/// `relation` must be valid and is borrowed for the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn relation_ref_schema(
    relation: Handle<SharedRelationRef>,
) -> KernelOwnedBytes {
    let schema = unsafe { relation.as_ref() }.schema();
    proto_schema::StructType::from(schema.as_ref())
        .encode_to_vec()
        .into()
}

/// Releases an identifier handle; actual rows live until the executor scope is released.
///
/// # Safety
/// `relation` must be valid and is consumed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn free_relation_ref(relation: Handle<SharedRelationRef>) {
    unsafe { relation.drop_handle() }
}

#[cfg(test)]
mod tests {
    use std::ptr::NonNull;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use delta_kernel::plans::PlanBuilder;
    use delta_kernel::plans::proto::operation;
    use delta_kernel::schema::schema_ref;
    use rstest::rstest;

    use super::*;
    use crate::error::{EngineExecError, FFIKernelError};
    use crate::ffi_test_utils::{error_only_engine_handle, ok_or_panic};
    use crate::plans::result::CPlanResult;

    struct Context {
        mode: u8,
        freed: Arc<AtomicUsize>,
    }

    extern "C" fn execute(
        _: NullableCvoid,
        _: KernelBytesSlice,
        out: *mut EngineExecResult<CPlanResult>,
    ) {
        unsafe { out.write(EngineExecResult::Success(CPlanResult::Unit)) }
    }

    extern "C" fn retain(
        context: NullableCvoid,
        name: KernelStringSlice,
        op: KernelBytesSlice,
        out: *mut EngineExecResult<Handle<SharedRelationRef>>,
    ) {
        let context = unsafe { &*context.unwrap().as_ptr().cast::<Context>() };
        assert_eq!(unsafe { name.try_to_string() }.unwrap(), "test");
        let op = operation::Operation::decode(unsafe { op.try_as_slice() }.unwrap()).unwrap();
        assert!(matches!(
            op.op,
            Some(operation::operation::Op::QueryPlan(_))
        ));
        let result = match context.mode {
            0 | 1 => {
                let schema = if context.mode == 0 {
                    schema_ref! { nullable "id": INTEGER }
                } else {
                    schema_ref! { nullable "wrong": STRING }
                };
                EngineExecResult::Success(Arc::new(RelationRef::new("retained", schema)).into())
            }
            2 => EngineExecResult::Failure(EngineExecError {
                etype: FFIKernelError::IOErrorError,
                message: Box::new("retention failed".to_string()).into(),
            }),
            _ => return,
        };
        unsafe { out.write(result) }
    }

    extern "C" fn free(context: NullableCvoid) {
        let context = unsafe { Box::from_raw(context.unwrap().as_ptr().cast::<Context>()) };
        context.freed.fetch_add(1, Ordering::Relaxed);
    }

    #[rstest]
    fn scoped_callbacks_validate_results_and_release_context(#[values(0, 1, 2, 3)] mode: u8) {
        let freed = Arc::new(AtomicUsize::new(0));
        let context = NonNull::new(
            Box::into_raw(Box::new(Context {
                mode,
                freed: freed.clone(),
            }))
            .cast(),
        );
        let handle = unsafe { get_scoped_plan_executor(context, execute, retain, free) };
        let executor = unsafe { handle.clone_as_arc() };
        unsafe { free_scoped_plan_executor(handle) };
        assert_eq!(freed.load(Ordering::Relaxed), 0);
        let plan = PlanBuilder::values(schema_ref! { nullable "id": INTEGER }, vec![])
            .unwrap()
            .build()
            .unwrap();
        let result = executor.execute_and_retain("test", plan);
        if mode == 0 {
            assert_eq!(result.unwrap().id(), "retained");
        } else {
            assert!(result.is_err());
        }
        drop(executor);
        assert_eq!(freed.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn plan_engine_keeps_the_scope_alive_after_its_original_handle_is_freed() {
        let freed = Arc::new(AtomicUsize::new(0));
        let context = NonNull::new(
            Box::into_raw(Box::new(Context {
                mode: 0,
                freed: freed.clone(),
            }))
            .cast(),
        );
        let scope = unsafe { get_scoped_plan_executor(context, execute, retain, free) };
        let engine = unsafe {
            get_plan_based_engine_from_scope(
                scope.shallow_copy(),
                OptionalValue::None,
                &crate::ffi_test_utils::TEST_ERROR_ALLOCATORS,
            )
        };
        unsafe { free_scoped_plan_executor(scope) };
        assert_eq!(freed.load(Ordering::Relaxed), 0);
        unsafe { crate::free_engine(engine) };
        assert_eq!(freed.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn relation_ref_round_trips_identifier_and_schema_with_named_cleanup() {
        let engine = error_only_engine_handle();
        let schema = schema_ref! { nullable "id": INTEGER };
        let bytes = proto_schema::StructType::from(schema.as_ref()).encode_to_vec();
        let id = "files-1";
        let handle = unsafe {
            ok_or_panic(new_relation_ref(
                kernel_string_slice!(id),
                kernel_bytes_slice!(bytes),
                engine.shallow_copy(),
            ))
        };
        assert_eq!(
            unsafe { relation_ref_id(handle.shallow_copy()).try_to_string() }.unwrap(),
            id
        );
        let encoded = unsafe { relation_ref_schema(handle.shallow_copy()) };
        assert_eq!(unsafe { encoded.into_vec() }, bytes);
        let encoded = unsafe { relation_ref_schema(handle.shallow_copy()) };
        unsafe {
            crate::free_kernel_bytes(encoded);
            free_relation_ref(handle);
            crate::free_engine(engine);
        }
    }
}
