//! State machines -- kernel-authored coroutine bodies that orchestrate plan execution.
//!
//! - [`framework`] -- the framework SMs are built on. Provides the typed engine-side failure type
//!   ([`framework::engine_error::EngineError`]), the [`framework::state_machine::StateMachine`]
//!   trait that the executor drives, the [`framework::coroutine::CoroutineSM`] driver that compiles
//!   `async fn` SM bodies into that trait, and the [`framework::plan_context::Context`] /
//!   [`framework::plan_context::PlanBuilder`] plan-construction surface.
//! - [`scan`] -- scan-time SMs (file-scan, full-state, scan-metadata) plus the reconciliation
//!   pipeline that drives log replay.

pub mod framework;
pub mod scan;
