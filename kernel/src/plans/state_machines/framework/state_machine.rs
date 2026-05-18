//! The [`StateMachine`] trait — the contract between kernel SMs and the
//! engine-side executor.
//!
//! Engines drive every SM through the same three methods: ask for the next
//! phase's work, execute it, hand back the outcome. The SM decides whether
//! to advance to another phase or terminate with a typed result.
//!
//! ```text
//! loop {
//!     let op = sm.get_operation()?;
//!     let result = executor.execute(op);
//!     match sm.advance(result)? {
//!         AdvanceResult::Continue => continue,
//!         AdvanceResult::Done(r) => return Ok(r),
//!     }
//! }
//! ```

use super::engine_error::EngineError;
use super::phase_operation::PhaseOperation;
use super::phase_state::PhaseState;
use crate::plans::errors::DeltaError;

/// Result of advancing a state machine one step.
#[derive(Debug)]
pub enum AdvanceResult<R> {
    /// SM advanced to the next phase; the driver should loop back to
    /// [`StateMachine::get_operation`].
    Continue,
    /// SM completed with the given result. The driver must stop.
    Done(R),
}

impl<R> AdvanceResult<R> {
    pub fn is_done(&self) -> bool {
        matches!(self, Self::Done(_))
    }

    /// Extract the terminal result. Panics on `Continue` — test-only.
    #[cfg(test)]
    pub fn unwrap(self) -> R {
        match self {
            Self::Done(r) => r,
            Self::Continue => panic!("called unwrap on AdvanceResult::Continue"),
        }
    }

    /// Convert to `Option<R>` — `Some` iff `Done`, `None` for `Continue`.
    pub fn into_result(self) -> Option<R> {
        match self {
            Self::Done(r) => Some(r),
            Self::Continue => None,
        }
    }
}

/// The contract kernel state machines implement and engine-side executors
/// drive.
///
/// Each SM-visible "phase" is one tick of this loop: the executor asks
/// [`StateMachine::get_operation`] for what to run, executes it, then calls
/// [`StateMachine::advance`] with the outcome ([`PhaseState`] on success, an
/// [`EngineError`] on engine failure).
///
/// # Error layering
///
/// Both `get_operation` and `advance` return [`DeltaError`] — the typed,
/// template-parameterized kernel error surface. Engine failures arrive via
/// the `Err(EngineError)` arm of `advance`'s input; the SM chooses whether
/// to lift them into its own terminal result or propagate them as a
/// [`DeltaError`].
pub trait StateMachine {
    /// What the SM returns when it finishes.
    type Result;

    /// Return the next phase's work.
    ///
    /// Takes `&mut self` so the SM can lazily construct plans and move
    /// internal state.
    ///
    /// Returns `Err(DeltaError)` for unrecoverable kernel bugs hit while
    /// *building* the next phase (plan construction can fail on e.g. a
    /// malformed schema projection). These are typically tagged
    /// [`DeltaErrorCode::DeltaCommandInvariantViolation`](crate::plans::errors::DeltaErrorCode::DeltaCommandInvariantViolation).
    fn get_operation(&mut self) -> Result<PhaseOperation, DeltaError>;

    /// Receive the phase outcome from the driver.
    ///
    /// - `Ok(PhaseState)` — the executor ran every plan in the phase and gathered any KDF state /
    ///   schema-query result; the SM takes ownership of the accumulator.
    /// - `Err(EngineError)` — a typed engine-side failure; the SM matches on
    ///   [`EngineError::kind`](super::engine_error::EngineError::kind) and decides how to surface
    ///   it.
    fn advance(
        &mut self,
        result: Result<PhaseState, EngineError>,
    ) -> Result<AdvanceResult<Self::Result>, DeltaError>;

    /// Static label for logging / diagnostics. Drivers use this in span
    /// names and error contexts; implementations should return the
    /// currently-active phase name (or a fallback like `"complete"` once
    /// the SM has finished).
    fn phase_name(&self) -> &'static str;

    /// Sorted snapshot of logical relation names currently registered with the SM at the boundary
    /// of the most recent yield.
    ///
    /// Used by engine-side executors for diagnostics / span context, parallel scheduling, and
    /// cross-phase relation tracking. Returns the empty vector for SMs that do not surface
    /// relations, and for terminal states (after the SM completes).
    fn live_relations(&self) -> Vec<String> {
        Vec::new()
    }
}
