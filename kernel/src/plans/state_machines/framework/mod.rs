//! State-machine framework: the shared infrastructure every kernel SM uses.
//!
//! - [`engine_error`] -- [`EngineError`](engine_error::EngineError), the typed failure the engine
//!   surfaces to the SM (distinct from [`DeltaError`](crate::plans::errors::DeltaError), the
//!   kernel-to-caller error).
//!
//! Additional modules (`state_machine`, `coroutine`, `plan_context`) land in follow-up PRs.

pub mod engine_error;
