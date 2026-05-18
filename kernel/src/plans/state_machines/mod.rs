//! State machines — kernel-authored coroutine bodies that orchestrate
//! plan execution.
//!
//! - [`framework`] — the framework SMs are built on: the
//!   [`StateMachine`](framework::state_machine::StateMachine) trait, the
//!   [`CoroutineSM`](framework::coroutine::driver::CoroutineSM) driver (a thin shell over
//!   `genawaiter2::sync::GenBoxed`), the typed
//!   [`Context`](framework::coroutine::context::Context) / [`PhaseState`] surface, and the
//!   `Extractor<O>` typed adapter.
//!
//! Concrete SMs (Snapshot, Scan, DML) land in follow-on PRs. Until then,
//! much of the framework surface has no in-tree caller — `dead_code` is
//! allowed subtree-wide to keep CI green while the remaining PRs land.
//!
//! [`PhaseState`]: framework::phase_state::PhaseState

#![allow(dead_code)]

pub mod framework;
pub mod scan;
