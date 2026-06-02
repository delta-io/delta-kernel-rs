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
/// The `context` pointer MUST be thread-safe and MUST remain valid for as long as the
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

/// Construct a [`PlanBasedEngine`] from the given [`SharedPlanExecutor`].
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ffi_test_utils::allocate_err;
    use crate::free_engine;
    use crate::plans::result::CPlanResultWrapper;

    #[test]
    fn get_plan_based_engine_returns_plan_based_engine() {
        extern "C" fn unreachable_callback(
            _context: NullableCvoid,
            _plan_proto: crate::KernelBytesSlice,
        ) -> CPlanResultWrapper {
            unreachable!("callback should not run -- this test only constructs the engine");
        }

        let executor = unsafe { get_plan_executor(None, unreachable_callback) };
        let engine_handle = unsafe { get_plan_based_engine(executor, allocate_err) };

        // Confirm we got back a real `PlanBasedEngine`, not e.g. an accidental DefaultEngine.
        let extern_engine = unsafe { engine_handle.as_ref() };
        let engine = extern_engine.engine();
        assert!(
            engine.any_ref().downcast_ref::<PlanBasedEngine>().is_some(),
            "engine handle must wrap a PlanBasedEngine",
        );

        unsafe { free_engine(engine_handle) };
    }
}
