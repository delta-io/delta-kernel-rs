//! Coroutine-backed `StateMachine` implementation.
//!
//! - [`generator`] — minimal hand-rolled stackless coroutine shim. Replaces `genawaiter` with a
//!   no-deps `Co<Y, R>` / `Gen<Y, R, O>` pair built on a single rendezvous slot.
//! - [`phase`] — async [`Phase<'a>`](phase::Phase) surface SM authors use
//!   (`phase.execute(operation, name).await`), plus the [`PhaseYield`](phase::PhaseYield) /
//!   [`PhaseResume`](phase::PhaseResume) protocol that flows through the generator.
//! - [`driver`] — the [`CoroutineSM<R>`](driver::CoroutineSM) driver that compiles
//!   [`PhaseYield`](phase::PhaseYield) sequences into the
//!   [`StateMachine`](super::state_machine::StateMachine) contract.

pub mod driver;
pub mod generator;
pub mod phase;
