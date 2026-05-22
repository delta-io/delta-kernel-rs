//! CoroutineSM-backed [`StateMachine`](super::state_machine::StateMachine) implementation.
//!
//! - [`context`] -- the [`StepYield`](context::StepYield) / [`StepResume`](context::StepResume)
//!   yield/resume protocol shared by the [`driver`] and the SSA
//!   [`Context`](super::plan_context::Context).
//! - [`driver`] — the [`CoroutineSM<R>`](driver::CoroutineSM) driver that compiles
//!   [`StepYield`](context::StepYield) sequences into the
//!   [`StateMachine`](super::state_machine::StateMachine) contract.
//!
//! The underlying stackless-coroutine machinery comes from the
//! [`genawaiter2`] crate. See [`driver`] for the no-panic policy
//! discussion (genawaiter2 panics on protocol misuse; the SSA
//! [`Context`](super::plan_context::Context) dispatch helpers rule out those paths by
//! construction).

pub mod context;
pub mod driver;
