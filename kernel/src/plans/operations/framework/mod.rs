//! State-machine framework: the shared infrastructure every kernel SM uses.
//!
//! - [`state_machine`] — the [`StateMachine`](state_machine::StateMachine) trait +
//!   [`NextStep`](state_machine::NextStep) the executor drives.
//! - [`step`] — the typed "unit of work" ([`Step`](step::Step)) handed from SM to executor each
//!   step.
//! - [`step_result`] — thread-safe success-payload container the executor fills
//!   ([`KernelConsumer`](crate::plans::kernel_consumers::KernelConsumer) finished handles +
//!   optional schema-query result) and that the SM consumes on `submit`.
//! - [`engine_error`] — [`EngineError`](engine_error::EngineError), the typed failure the engine
//!   surfaces to the SM (distinct from [`DeltaError`](crate::plans::errors::DeltaError), which is
//!   the kernel-to-caller error).
//! - [`coroutine`] — [`Coroutine`](coroutine::driver::Coroutine), the async-fn-backed
//!   `StateMachine` impl. Wraps `genawaiter2::sync::GenBoxed` to translate the typed
//!   [`StepYield`](coroutine::context::StepYield) / [`StepResume`](coroutine::context::StepResume)
//!   protocol into the [`StateMachine`](state_machine::StateMachine) trait the executor drives.
//! - [`plan_context`] — SSA plan-construction [`Context`](plan_context::Context) and
//!   [`Cursor`](plan_context::Cursor). Standalone in PR4; wired into the coroutine step-protocol in
//!   PR5.

pub mod coroutine;
pub mod engine_error;
pub mod plan_context;
pub mod state_machine;
pub mod step;
pub mod step_result;
