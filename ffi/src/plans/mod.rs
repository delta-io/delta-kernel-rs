//! This module contains all types and functions needed to support declarative plan execution
//! over the FFI boundary.

use std::sync::Arc;

use delta_kernel::engine::plans::PlanBasedEngine;
use delta_kernel::{Engine, PlanExecutor};

use crate::handle::Handle;
use crate::plans::executor::{CExecuteOpFn, FfiPlanExecutor, SharedPlanExecutor};
use crate::{ErrorAllocators, NullableCvoid, OptionalValue, SharedExternEngine, engine_to_handle};

pub mod executor;
pub mod iter;
pub mod result;

/// Build a [`PlanExecutor`] backed by an engine-provided C callback.
///
/// Pass `true` for `support_query_plans` if the callback supports query plans beyond file scans.
/// Pass `false` for an executor limited to I/O operations and file scans; Kernel then replays
/// protocol and metadata through engine handlers. This choice is fixed for the executor's lifetime.
/// Handler I/O and explicitly requested plans still use the executor. See
/// [`PlanExecutor::support_query_plans`] for the support contract.
///
/// The returned handle can be consumed by [`get_plan_based_engine`] or released with
/// [`free_plan_executor`].
///
/// # Safety
/// The `context` pointer MUST be thread-safe (Send + Sync) and MUST remain valid for as long as the
/// executor is used. It is valid to pass NULL as the context. The callback MUST remain callable
/// for the executor's lifetime and support concurrent calls.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn get_plan_executor(
    context: NullableCvoid,
    callback: CExecuteOpFn,
    support_query_plans: bool,
) -> Handle<SharedPlanExecutor> {
    let executor: Arc<dyn PlanExecutor> =
        Arc::new(FfiPlanExecutor::new(context, callback, support_query_plans));
    executor.into()
}

/// Free an executor obtained from [`get_plan_executor`].
///
/// Normally the handle is consumed by [`get_plan_based_engine`] and need not be explicitly freed by
/// the caller. Use this only when discarding the executor without wrapping it in PlanBasedEngine.
///
/// # Safety
///
/// Caller must pass a valid handle obtained from [`get_plan_executor`] and must not use it
/// again afterwards.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn free_plan_executor(executor: Handle<SharedPlanExecutor>) {
    unsafe { executor.drop_handle() }
}

/// Construct a [`PlanBasedEngine`] backed by `plan_executor`, optionally delegating operations not
/// yet implemented on the plan-execution path to `fallback_engine`.
///
/// This method consumes the [`SharedPlanExecutor`] handle. It does NOT consume `fallback_engine`:
/// the caller retains ownership of the fallback engine handle and must free it separately via
/// `free_engine`. The returned plan-based engine clones the handlers it needs from the fallback,
/// so it remains valid independently of when the caller frees its fallback handle. When
/// `fallback_engine` is [`OptionalValue::None`], unimplemented operations return an unsupported
/// error.
///
/// # Safety
///
/// `error_allocators` must point to an initialized [`ErrorAllocators`] for this call. Its context
/// and callbacks must remain valid for every use and support concurrent calls if shared across
/// threads.
///
/// Caller must pass a valid [`SharedPlanExecutor`] handle obtained from [`get_plan_executor`]
/// and a valid [`ErrorAllocators`]. When `fallback_engine` is [`OptionalValue::Some`], it must
/// contain a valid [`SharedExternEngine`] handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn get_plan_based_engine(
    plan_executor: Handle<SharedPlanExecutor>,
    fallback_engine: OptionalValue<Handle<SharedExternEngine>>,
    error_allocators: *const ErrorAllocators,
) -> Handle<SharedExternEngine> {
    let error_allocators = unsafe { *error_allocators };
    let executor: Arc<dyn PlanExecutor> = unsafe { plan_executor.into_inner() };
    let fallback = match fallback_engine {
        OptionalValue::Some(engine) => Some(unsafe { engine.as_ref() }.engine()),
        OptionalValue::None => None,
    };
    let engine: Arc<dyn Engine> = Arc::new(PlanBasedEngine::new(fallback, executor));
    engine_to_handle(engine, error_allocators)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use delta_kernel::arrow::array::{Array, RecordBatch, StringArray};
    use delta_kernel::engine::arrow_data::ArrowEngineData;
    use delta_kernel::engine_data::FilteredEngineData;
    use delta_kernel::object_store::memory::InMemory;
    use delta_kernel_default_engine::DefaultEngineBuilder;
    use rstest::rstest;
    use url::Url;

    use super::*;
    use crate::error::EngineExecResult;
    use crate::ffi_test_utils::TEST_ERROR_ALLOCATORS;
    use crate::free_engine;
    use crate::plans::result::CPlanResult;

    extern "C" fn unreachable_callback(
        _context: NullableCvoid,
        _plan_proto: crate::KernelBytesSlice,
        _out: *mut EngineExecResult<CPlanResult>,
    ) {
        unreachable!("callback should not run -- this test only constructs the engine");
    }

    extern "C" fn unit_callback(
        _context: NullableCvoid,
        _plan_proto: crate::KernelBytesSlice,
        out: *mut EngineExecResult<CPlanResult>,
    ) {
        unsafe { out.write(EngineExecResult::Success(CPlanResult::Unit)) };
    }

    #[rstest]
    #[case::enabled(true)]
    #[case::disabled(false)]
    fn plan_executor_query_plan_support_survives_engine_construction(#[case] support: bool) {
        let executor = unsafe { get_plan_executor(None, unit_callback, support) };
        assert_eq!(unsafe { executor.as_ref() }.support_query_plans(), support);

        let engine_handle =
            unsafe { get_plan_based_engine(executor, OptionalValue::None, &TEST_ERROR_ALLOCATORS) };
        let engine = unsafe { engine_handle.as_ref() }.engine();
        assert_eq!(
            engine
                .require_plan_executor()
                .unwrap()
                .support_query_plans(),
            support
        );
        engine
            .storage_handler()
            .put(
                &Url::parse("memory:///table/file").unwrap(),
                Bytes::from_static(b"test"),
                false,
            )
            .unwrap();

        unsafe { free_engine(engine_handle) };
    }

    /// Assert that the given engine handle wraps a `PlanBasedEngine` (not e.g. a DefaultEngine).
    fn assert_is_plan_based_engine(engine_handle: &Handle<SharedExternEngine>) {
        let extern_engine = unsafe { engine_handle.as_ref() };
        let engine = extern_engine.engine();
        assert!(
            engine.any_ref().downcast_ref::<PlanBasedEngine>().is_some(),
            "engine handle must wrap a PlanBasedEngine",
        );
    }

    #[test]
    fn plan_based_engine_works_after_fallback_handle_is_freed() {
        let executor = unsafe { get_plan_executor(None, unreachable_callback, true) };

        // The caller retains ownership of the fallback engine handle. `shallow_copy` mimics C
        // passing the pointer by value, so we can still free our own handle afterward.
        let fallback = DefaultEngineBuilder::new(Arc::new(InMemory::new())).build();
        let fallback_handle = engine_to_handle(Arc::new(fallback), TEST_ERROR_ALLOCATORS);

        let engine_handle = unsafe {
            get_plan_based_engine(
                executor,
                OptionalValue::Some(fallback_handle.shallow_copy()),
                &TEST_ERROR_ALLOCATORS,
            )
        };

        assert_is_plan_based_engine(&engine_handle);

        unsafe { free_engine(fallback_handle) };

        let batch = RecordBatch::try_from_iter(vec![(
            "value",
            Arc::new(StringArray::from(vec!["one"])) as Arc<dyn Array>,
        )])
        .unwrap();
        let data = Box::new(ArrowEngineData::new(batch));
        let location = Url::parse("memory:///table/_delta_log/00000000000000000001.json").unwrap();
        unsafe { engine_handle.as_ref() }
            .engine()
            .json_handler()
            .write_json_file(
                &location,
                Box::new(std::iter::once(Ok(
                    FilteredEngineData::with_all_rows_selected(data),
                ))),
                false,
            )
            .expect("plan-based engine retains its fallback json handler");

        unsafe { free_engine(engine_handle) };
    }

    #[test]
    fn get_plan_based_engine_without_fallback_returns_plan_based_engine() {
        let executor = unsafe { get_plan_executor(None, unreachable_callback, true) };

        let engine_handle =
            unsafe { get_plan_based_engine(executor, OptionalValue::None, &TEST_ERROR_ALLOCATORS) };

        assert_is_plan_based_engine(&engine_handle);

        unsafe { free_engine(engine_handle) };
    }

    /// Without a fallback, an operation the plan-execution path does not implement reports that
    /// rather than delegating.
    #[test]
    fn without_fallback_unimplemented_operations_are_unsupported() {
        let executor = unsafe { get_plan_executor(None, unreachable_callback, true) };
        let engine_handle =
            unsafe { get_plan_based_engine(executor, OptionalValue::None, &TEST_ERROR_ALLOCATORS) };

        let engine = unsafe { engine_handle.as_ref() }.engine();
        let location = Url::parse("memory:///table/_delta_log/00000000000000000001.json").unwrap();
        let err = engine
            .json_handler()
            .write_json_file(&location, Box::new(std::iter::empty()), false)
            .expect_err("write_json_file has no plan-execution path and no fallback");
        assert!(
            matches!(err, delta_kernel::KernelError::Unsupported(_)),
            "expected an unsupported error, got: {err:?}",
        );

        unsafe { free_engine(engine_handle) };
    }
}
