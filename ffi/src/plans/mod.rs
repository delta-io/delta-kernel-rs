//! This module contains all types and functions needed to support declarative plan execution
//! over the FFI boundary.

use std::sync::Arc;

use delta_kernel::engine::arrow_expression::ArrowEvaluationHandler;
use delta_kernel::engine::plans::PlanBasedEngine;
use delta_kernel::{Engine, EvaluationHandler, PlanExecutor};

use crate::handle::Handle;
use crate::plans::executor::{CExecuteOpFn, FfiPlanExecutor, SharedPlanExecutor};
use crate::{engine_to_handle, AllocateErrorFn, NullableCvoid, SharedExternEngine};

pub mod executor;
pub mod iter;
pub mod result;

/// Build a [`PlanExecutor`] backed by an engine-provided C callback.
///
/// # Safety
/// The `context` pointer MUST be thread-safe (Send + Sync) and MUST remain valid for as long as the
/// executor is used. It is valid to pass NULL as the context.
#[no_mangle]
pub unsafe extern "C" fn get_plan_executor(
    context: NullableCvoid,
    callback: CExecuteOpFn,
) -> Handle<SharedPlanExecutor> {
    let executor: Arc<dyn PlanExecutor> = Arc::new(FfiPlanExecutor::new(context, callback));
    executor.into()
}

/// Free a plan executor obtained from [`get_plan_executor`].
///
/// Normally the handle is consumed by [`get_plan_based_engine`] and need not be explicitly freed by
/// the caller. Use this only when discarding the executor without wrapping it in PlanBasedEngine.
///
/// # Safety
///
/// Caller must pass a valid handle previously obtained from [`get_plan_executor`] and must not use
/// it again afterwards.
#[no_mangle]
pub unsafe extern "C" fn free_plan_executor(executor: Handle<SharedPlanExecutor>) {
    executor.drop_handle();
}

/// Construct a [`PlanBasedEngine`] from the given [`SharedPlanExecutor`], with no fallback engine.
///
/// Operations not yet implemented on the plan-execution path return an unsupported error. Use
/// [`get_plan_based_engine_with_fallback`] to delegate those operations to another engine instead.
///
/// This method consumes the [`SharedPlanExecutor`] handle, which should be freed when the engine
/// is dropped via `free_engine` (caller responsibility).
///
/// # Safety
///
/// Caller must pass a valid [`SharedPlanExecutor`] handle obtained from [`get_plan_executor`] and
/// a valid [`AllocateErrorFn`].
#[no_mangle]
pub unsafe extern "C" fn get_plan_based_engine(
    plan_executor: Handle<SharedPlanExecutor>,
    allocate_error: AllocateErrorFn,
) -> Handle<SharedExternEngine> {
    let executor: Arc<dyn PlanExecutor> = unsafe { plan_executor.into_inner() };
    let evaluation_handler: Arc<dyn EvaluationHandler> = Arc::new(ArrowEvaluationHandler);
    let engine: Arc<dyn Engine> = Arc::new(PlanBasedEngine::new(evaluation_handler, executor));
    engine_to_handle(engine, allocate_error)
}

/// Construct a [`PlanBasedEngine`] backed by `plan_executor`, delegating operations not yet
/// implemented on the plan-execution path to `fallback_engine`.
///
/// This method consumes BOTH the [`SharedPlanExecutor`] and [`SharedExternEngine`] handles.
/// Ownership of the fallback engine transfers into the returned engine: dropping the returned
/// engine (via `free_engine`) also drops the embedded fallback engine.
///
/// # Safety
///
/// Caller must pass a valid [`SharedPlanExecutor`] handle obtained from [`get_plan_executor`], a
/// valid [`SharedExternEngine`] handle, and a valid [`AllocateErrorFn`].
#[no_mangle]
pub unsafe extern "C" fn get_plan_based_engine_with_fallback(
    plan_executor: Handle<SharedPlanExecutor>,
    fallback_engine: Handle<SharedExternEngine>,
    allocate_error: AllocateErrorFn,
) -> Handle<SharedExternEngine> {
    let executor: Arc<dyn PlanExecutor> = unsafe { plan_executor.into_inner() };
    let fallback: Arc<dyn Engine> = unsafe { fallback_engine.into_inner() }.engine();
    let engine: Arc<dyn Engine> = Arc::new(PlanBasedEngine::with_fallback(fallback, executor));
    engine_to_handle(engine, allocate_error)
}

#[cfg(test)]
mod tests {
    use delta_kernel::object_store::memory::InMemory;
    use delta_kernel_default_engine::DefaultEngineBuilder;

    use super::*;
    use crate::error::EngineExecResult;
    use crate::ffi_test_utils::allocate_err;
    use crate::free_engine;
    use crate::plans::result::CPlanResult;

    extern "C" fn unreachable_callback(
        _context: NullableCvoid,
        _plan_proto: crate::KernelBytesSlice,
        _out: *mut EngineExecResult<CPlanResult>,
    ) {
        unreachable!("callback should not run -- this test only constructs the engine");
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
    fn get_plan_based_engine_returns_plan_based_engine() {
        let executor = unsafe { get_plan_executor(None, unreachable_callback) };
        let engine_handle = unsafe { get_plan_based_engine(executor, allocate_err) };

        assert_is_plan_based_engine(&engine_handle);

        unsafe { free_engine(engine_handle) };
    }

    #[test]
    fn get_plan_based_engine_with_fallback_returns_plan_based_engine() {
        let executor = unsafe { get_plan_executor(None, unreachable_callback) };

        // Build a fallback engine handle whose ownership will transfer into the plan-based engine.
        let fallback = DefaultEngineBuilder::new(Arc::new(InMemory::new())).build();
        let fallback_handle = engine_to_handle(Arc::new(fallback), allocate_err);

        let engine_handle =
            unsafe { get_plan_based_engine_with_fallback(executor, fallback_handle, allocate_err) };

        assert_is_plan_based_engine(&engine_handle);

        // Freeing the plan-based engine also frees the embedded fallback
        unsafe { free_engine(engine_handle) };
    }
}
