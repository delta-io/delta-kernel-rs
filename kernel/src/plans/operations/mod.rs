//! State machines -- kernel-authored coroutine bodies that orchestrate plan execution.
//!
//! - [`framework`] -- the framework SMs are built on: the
//!   [`StateMachine`](framework::state_machine::StateMachine) trait, the
//!   [`Coroutine`](framework::coroutine::driver::Coroutine) driver (a thin shell over
//!   `genawaiter2::sync::GenBoxed`), the typed [`Context`](framework::plan_context::Context) /
//!   [`StepPayload`] surface, and the `Extractor<O>` typed adapter.
//!
//! [`StepPayload`]: framework::step_payload::StepPayload

pub mod framework;
pub mod scan;
