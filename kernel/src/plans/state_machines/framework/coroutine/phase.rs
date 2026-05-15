//! Phase surface: the async API coroutine bodies use to drive
//! [`super::driver::CoroutineSM`].
//!
//! Three pieces, all kernel-internal:
//!
//! - [`PhaseYield`] / [`PhaseResume`] / [`PhaseCo`] — the typed protocol flowing through the
//!   coroutine. Each yield is a single [`PhaseOperation`] (wrapped in [`Arc`] for cheap clones)
//!   plus a static phase name; each resume is a `Result<PhaseState, EngineError>`.
//! - [`Phase`] — the async surface SM authors use. The single entry point is [`Phase::execute`],
//!   which yields one operation and returns the resulting [`PhaseState`] (or an [`EngineError`]).
//!
//! ## 1:1 protocol
//!
//! Each `Phase::execute(op, name)` corresponds to exactly one operation handed
//! to the engine and exactly one resume. There is no batching, no separate
//! dispatch step, and no in-driver same-batch replay: SM bodies that need to
//! run multiple plans in a single engine call construct a
//! [`PhaseOperation::Plans`] with multiple plans and extract per-plan outputs
//! from the returned [`PhaseState`] using their respective
//! [`Extractor<O>`](crate::plans::ir::Extractor)s.

use std::sync::Arc;

use super::generator::Co;
use crate::plans::state_machines::framework::engine_error::EngineError;
use crate::plans::state_machines::framework::phase_operation::PhaseOperation;
use crate::plans::state_machines::framework::phase_state::PhaseState;

/// Value yielded by a coroutine at each phase boundary. Carries the
/// operation envelope plus a static phase name for diagnostics. The driver
/// presents the operation to the engine and resumes the coroutine with a
/// [`PhaseResume`] containing the engine outcome.
///
/// `Arc<PhaseOperation>` keeps the yield cheap to move through the
/// generator and lets the driver hold a clone for `phase_name` introspection
/// (`StateMachine::phase_name`) without cloning the entire operation.
pub(crate) struct PhaseYield {
    pub operation: Arc<PhaseOperation>,
    pub phase_name: &'static str,
}

/// Value the driver passes back to the coroutine on resume. Wraps the
/// engine outcome for the most recent [`PhaseYield::operation`].
pub(crate) struct PhaseResume(pub Result<PhaseState, EngineError>);

impl std::fmt::Debug for PhaseResume {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            Ok(_) => f.write_str("PhaseResume(Ok(..))"),
            Err(e) => write!(f, "PhaseResume(Err({:?}))", e.kind),
        }
    }
}

/// Coroutine handle used inside phase bodies. Thin alias over the generic
/// [`Co`] from the [`generator`](super::generator) shim.
pub(crate) type PhaseCo = Co<PhaseYield, PhaseResume>;

/// Async surface SM authors call. Holds a mutable borrow of the coroutine
/// handle; lives only for the duration of one phase body.
pub(crate) struct Phase<'a>(pub(crate) &'a mut PhaseCo);

impl<'a> Phase<'a> {
    /// Hand `operation` to the engine and await its outcome.
    ///
    /// Returns the populated [`PhaseState`] on success or the engine's
    /// [`EngineError`] on failure. Errors are surfaced raw so SM bodies
    /// can pattern-match on
    /// [`EngineError::kind`](super::super::engine_error::EngineError::kind);
    /// translation to [`DeltaError`] is the SM's responsibility.
    pub(crate) async fn execute(
        &mut self,
        operation: PhaseOperation,
        name: &'static str,
    ) -> Result<PhaseState, EngineError> {
        let resume = self
            .0
            .yield_(PhaseYield {
                operation: Arc::new(operation),
                phase_name: name,
            })
            .await;
        resume.0
    }
}
