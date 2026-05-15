//! State-machine framework: the shared infrastructure every kernel SM uses.
//!
//! - [`state_machine`] — the [`StateMachine`](state_machine::StateMachine) trait +
//!   [`AdvanceResult`](state_machine::AdvanceResult) the executor drives.
//! - [`phase_operation`] — the typed "unit of work"
//!   ([`PhaseOperation`](phase_operation::PhaseOperation)) handed from SM to executor each step.
//! - [`phase_state`] — thread-safe success-payload container the executor fills (KDF
//!   [`FinishedHandle`](crate::plans::kdf::FinishedHandle)s + optional schema-query result) and
//!   that the SM consumes on `advance`.
//! - [`engine_error`] — [`EngineError`](engine_error::EngineError), the typed failure the engine
//!   surfaces to the SM (distinct from [`DeltaError`](crate::plans::errors::DeltaError), which is
//!   the kernel-to-caller error).
//! - [`coroutine`] — [`CoroutineSM`](coroutine::driver::CoroutineSM), the async-fn-backed
//!   `StateMachine` impl + its hand-rolled `Gen`/`Co` generator shim.

pub mod coroutine;
pub mod engine_error;
pub mod phase_operation;
pub mod phase_state;
pub mod state_machine;
