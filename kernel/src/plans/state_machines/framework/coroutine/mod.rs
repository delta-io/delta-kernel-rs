//! Coroutine-backed `StateMachine` implementation.
//!
//! - [`phase`] — async [`Phase<'a>`](phase::Phase) surface SM authors use
//!   (`phase.execute(operation, name).await`), plus the [`PhaseYield`](phase::PhaseYield) /
//!   [`PhaseResume`](phase::PhaseResume) protocol that flows through the [`genawaiter2`] generator.
//! - [`driver`] — the [`CoroutineSM<R>`](driver::CoroutineSM) driver that compiles
//!   [`PhaseYield`](phase::PhaseYield) sequences into the
//!   [`StateMachine`](super::state_machine::StateMachine) contract.

pub mod driver;
pub mod phase;
