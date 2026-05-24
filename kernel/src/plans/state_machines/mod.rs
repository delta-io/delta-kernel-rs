//! State machines -- kernel-authored coroutine bodies that orchestrate plan execution.
//!
//! - [`framework`] -- the framework SMs are built on. Currently provides the typed engine-side
//!   failure type ([`framework::engine_error::EngineError`]), the
//!   [`framework::state_machine::StateMachine`] trait that the executor drives, and the
//!   [`framework::coroutine::CoroutineSM`] driver that compiles `async fn` SM bodies into that
//!   trait. The `Context` plan-construction surface lives in a sibling submodule added under the
//!   same `declarative-plans` feature gate.

pub mod framework;
