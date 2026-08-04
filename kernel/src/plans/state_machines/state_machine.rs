//! The executor-facing state-machine protocol.

use crate::plans::{Operation, PlanResult};
use crate::{DeltaResult, Error};

/// Work requested by a kernel state machine.
#[derive(Debug, Clone)]
pub enum EngineRequest {
    /// Execute a declarative operation and return its [`PlanResult`].
    Execute(Operation),
}

/// Successful output returned by an executor for one request.
pub enum EngineResponse {
    /// Result of an [`EngineRequest::Execute`] request.
    Plan(PlanResult),
}

impl std::fmt::Debug for EngineResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Plan(_) => f.write_str("Plan(..)"),
        }
    }
}

/// Result of advancing a state machine with an executor response.
#[derive(Debug)]
pub enum NextStep<R> {
    /// Execute this request and pass its outcome to [`StateMachine::submit`].
    Execute(EngineRequest),
    /// The machine completed with `R`.
    Done(R),
}

/// A resumable kernel workflow driven one engine request at a time.
///
/// Drivers repeatedly call [`Self::get_step`]. For [`NextStep::Execute`], execute the request and
/// pass its result to [`Self::submit`]; [`NextStep::Done`] contains the terminal value.
pub trait StateMachine {
    /// Value produced when the workflow completes.
    type Result;

    /// Return the next executor request or the workflow's terminal result.
    fn get_step(&mut self) -> DeltaResult<NextStep<Self::Result>>;

    /// Resume the workflow with the result of its current request.
    ///
    /// Engine failures are supplied as [`Error`] so the workflow can either handle them or
    /// propagate them as its terminal error.
    ///
    /// Returns an error if no engine request is awaiting a response.
    fn submit(&mut self, result: Result<EngineResponse, Error>) -> DeltaResult<()>;

    /// Stable name of the request currently awaiting execution, or `"done"` after completion.
    fn step_name(&self) -> &'static str;
}
