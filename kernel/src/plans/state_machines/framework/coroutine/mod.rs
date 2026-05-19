//! Coroutine-backed [`StateMachine`](super::state_machine::StateMachine) implementation.
//!
//! - [`context`] — async [`Context<'a>`](context::Context) surface SM authors use
//!   (`ctx.execute(operation, name).await`, plus the
//!   [`RelationRegistry`](crate::plans::ir::RelationRegistry) API reached via `Deref` /
//!   `DerefMut`). Includes the [`PhaseYield`](context::PhaseYield) /
//!   [`PhaseResume`](context::PhaseResume) protocol that flows through the underlying generator.
//! - [`driver`] — the [`CoroutineSM<R>`](driver::CoroutineSM) driver that compiles
//!   [`PhaseYield`](context::PhaseYield) sequences into the
//!   [`StateMachine`](super::state_machine::StateMachine) contract.
//!
//! The underlying stackless-coroutine machinery comes from the
//! [`genawaiter2`] crate. See [`driver`] for the no-panic policy
//! discussion (genawaiter2 panics on protocol misuse; the
//! [`Context`](context::Context) API rules out those paths by construction).

pub mod context;
pub mod driver;
