//! Coroutine-backed [`StateMachine`](super::state_machine::StateMachine) implementation.
//!
//! - [`phase`] — async [`Phase<'a>`](phase::Phase) surface SM authors use
//!   (`phase.execute(operation, name).await`), plus the [`PhaseYield`](phase::PhaseYield) /
//!   [`PhaseResume`](phase::PhaseResume) protocol that flows through the underlying generator.
//! - [`driver`] — the [`CoroutineSM<R>`](driver::CoroutineSM) driver that compiles
//!   [`PhaseYield`](phase::PhaseYield) sequences into the
//!   [`StateMachine`](super::state_machine::StateMachine) contract.
//!
//! The underlying stackless-coroutine machinery comes from the
//! [`genawaiter2`] crate. See [`driver`] for the no-panic policy
//! discussion (genawaiter2 panics on protocol misuse; the [`Phase`](phase::Phase) API rules out
//! those paths by construction).

pub mod driver;
pub mod phase;
