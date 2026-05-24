//! State-machine framework: the shared infrastructure every kernel SM uses.
//!
//! - [`state_machine`] -- the [`StateMachine`](state_machine::StateMachine) trait,
//!   [`NextStep`](state_machine::NextStep), and the typed
//!   [`EngineRequest`](state_machine::EngineRequest) /
//!   [`EngineResponse`](state_machine::EngineResponse) protocol the executor and SM exchange each
//!   tick.
//! - [`engine_error`] -- [`EngineError`](engine_error::EngineError), the typed failure the engine
//!   surfaces to the SM (distinct from [`DeltaError`](crate::plans::errors::DeltaError), the
//!   kernel-to-caller error).
//! - [`coroutine`] -- [`CoroutineSM`](coroutine::CoroutineSM), the async-fn-backed `StateMachine`
//!   impl. Wraps `genawaiter2::rc::Gen` to translate the typed `StepYield` / `StepResume` protocol
//!   (both `pub(crate)`) into the `StateMachine` trait the executor drives.
//! - [`plan_context`] -- [`Context`](plan_context::Context) and
//!   [`PlanBuilder`](plan_context::PlanBuilder), the ergonomic surface SM bodies use to build
//!   [`Plan`](crate::plans::ir::plan::Plan) graphs.

pub mod coroutine;
pub mod engine_error;
pub mod plan_context;
pub mod state_machine;
