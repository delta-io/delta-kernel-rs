//! Resumable kernel workflows driven by connector-owned plan executors.
//!
//! [`StateMachine`] defines the request/response protocol exposed to executors, while
//! [`CoroutineSM`] lets kernel workflows express that protocol as ordinary async control flow.

mod coroutine;
mod state_machine;

pub(crate) use coroutine::CoroutineEngine;
pub use coroutine::CoroutineSM;
pub use state_machine::{EngineRequest, EngineResponse, NextStep, StateMachine};
