//! The [`StateMachine`] trait — the contract between kernel SMs and the
//! engine-side executor.
//!
//! Engines drive every SM through the same three methods: ask for the next
//! step's work, execute it, hand back the outcome. The SM decides whether
//! to continue to another step or terminate with a typed result.
//!
//! ```text
//! loop {
//!     let op = sm.get_step()?;
//!     let result = executor.execute(op);
//!     match sm.submit(result)? {
//!         NextStep::Continue => continue,
//!         NextStep::Done(r) => return Ok(r),
//!     }
//! }
//! ```

use super::engine_error::EngineError;
use super::step::EngineRequest;
use super::step_payload::EngineResponse;
use crate::plans::errors::DeltaError;

/// Result of submitting a step result to a state machine.
#[derive(Debug)]
pub enum NextStep<R> {
    /// SM advanced to the next step; the driver should loop back to
    /// [`StateMachine::get_step`].
    Continue,
    /// SM completed with the given result. The driver must stop.
    Done(R),
}

/// The contract kernel state machines implement and engine-side executors
/// drive.
///
/// Each SM-visible "step" is one tick of this loop: the executor asks
/// [`StateMachine::get_step`] for what to run, executes it, then calls
/// [`StateMachine::submit`] with the outcome ([`EngineResponse`] on success, an
/// [`EngineError`] on engine failure).
///
/// # Error layering
///
/// Both `get_step` and `submit` return [`DeltaError`] — the typed,
/// template-parameterized kernel error surface. Engine failures arrive via
/// the `Err(EngineError)` arm of `submit`'s input; the SM chooses whether
/// to lift them into its own terminal result or propagate them as a
/// [`DeltaError`].
pub trait StateMachine {
    /// What the SM returns when it finishes.
    type Result;

    /// Return the next step's work.
    ///
    /// Takes `&mut self` so the SM can lazily construct plans and move
    /// internal state.
    ///
    /// Returns `Err(DeltaError)` for unrecoverable kernel bugs hit while
    /// *building* the next step (plan construction can fail on e.g. a
    /// malformed schema projection). These are typically tagged
    /// [`DeltaErrorCode::DeltaCommandInvariantViolation`](crate::plans::errors::DeltaErrorCode::DeltaCommandInvariantViolation).
    fn get_step(&mut self) -> Result<EngineRequest, DeltaError>;

    /// Receive the step outcome from the driver.
    ///
    /// - `Ok(EngineResponse)` — the executor ran the step and produced its single typed payload (a
    ///   finished consumer handle, a schema, or
    ///   [`EngineResponse::Empty`](super::step_payload::EngineResponse::Empty) for the
    ///   driver-internal priming case). The SM body destructures the variant matching its preceding
    ///   yield; any other variant is an executor bug surfaced as an internal error.
    /// - `Err(EngineError)` — a typed engine-side failure; the SM matches on
    ///   [`EngineError::kind`](super::engine_error::EngineError::kind) and decides how to surface
    ///   it.
    fn submit(
        &mut self,
        result: Result<EngineResponse, EngineError>,
    ) -> Result<NextStep<Self::Result>, DeltaError>;

    /// Static label for logging / diagnostics. Drivers use this in span
    /// names and error contexts; implementations return the currently-active
    /// step name, or `"complete"` once the SM has finished.
    fn step_name(&self) -> &'static str;

    /// Sorted snapshot of logical relation names currently registered with the SM at the boundary
    /// of the most recent yield.
    ///
    /// Surfaces diagnostics / span context, parallel scheduling info, and cross-phase relation
    /// tracking. Returns the empty vector for SMs that do not surface relations, and for
    /// terminal states (after the SM completes).
    fn live_relations(&self) -> Vec<String> {
        Vec::new()
    }
}
