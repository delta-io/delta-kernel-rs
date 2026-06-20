//! State machines -- kernel-authored coroutine bodies that orchestrate plan execution.
//!
//! - [`framework`] -- the framework SMs are built on. Successive PRs flesh out the `StateMachine`
//!   trait, the `CoroutineSM` driver, the typed `Context`/`EngineResponse` surface, and the
//!   `Extractor<O>` typed adapter; this PR seeds it with the typed engine-side failure type
//!   ([`framework::engine_error::EngineError`]).

pub mod framework;
