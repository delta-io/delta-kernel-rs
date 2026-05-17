//! Phase surface: the async API coroutine bodies use to drive
//! [`super::driver::CoroutineSM`].
//!
//! Three pieces, all kernel-internal:
//!
//! - [`PhaseYield`] / [`PhaseResume`] / [`PhaseCo`] — the typed protocol flowing through the
//!   coroutine. Each yield is a single [`PhaseOperation`] plus a static phase name; each resume is
//!   a `Result<PhaseState, EngineError>`.
//! - [`Phase`] — the async surface SM authors use. Single entry point [`Phase::execute`] yields one
//!   operation and returns the resulting [`PhaseState`] or the raw [`EngineError`]; SM bodies
//!   choose the appropriate [`DeltaErrorCode`] via [`EngineError::into_delta`] when lifting into a
//!   typed kernel error. Engines emit [`EngineError`]; the kernel translates at SM level per the
//!   layering contract documented in
//!   [`engine_error`](crate::plans::state_machines::framework::engine_error).
//!
//! ## 1:1 protocol
//!
//! Each `Phase::execute(op, name)` corresponds to exactly one operation handed to the engine and
//! exactly one resume. There is no batching, no separate dispatch step, and no in-driver
//! same-batch replay: SM bodies that need to run multiple plans in a single engine call construct
//! a [`PhaseOperation::Plans`] with multiple plans and extract per-plan outputs from the
//! returned [`PhaseState`] using their respective
//! [`Extractor<O>`](crate::plans::ir::Extractor)s.

use crate::plans::state_machines::framework::engine_error::EngineError;
use crate::plans::state_machines::framework::phase_operation::PhaseOperation;
use crate::plans::state_machines::framework::phase_state::PhaseState;

/// Value yielded by a coroutine at each phase boundary. Carries the operation envelope plus a
/// static phase name for diagnostics. The driver presents the operation to the engine and resumes
/// the coroutine with a [`PhaseResume`] containing the engine outcome.
pub(crate) struct PhaseYield {
    pub operation: PhaseOperation,
    pub phase_name: &'static str,
}

/// Value the driver passes back to the coroutine on resume. Wraps the engine outcome for the most
/// recent [`PhaseYield::operation`].
pub(crate) struct PhaseResume(pub Result<PhaseState, EngineError>);

impl std::fmt::Debug for PhaseResume {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            Ok(_) => f.write_str("PhaseResume(Ok(..))"),
            Err(e) => write!(f, "PhaseResume(Err({:?}))", e.kind),
        }
    }
}

/// Coroutine handle used inside phase bodies. Alias over [`genawaiter2::sync::Co`].
pub(crate) type PhaseCo = genawaiter2::sync::Co<PhaseYield, PhaseResume>;

/// Async surface SM authors call. Holds a mutable borrow of the coroutine handle; lives only for
/// the duration of one phase body.
pub(crate) struct Phase<'a>(pub(crate) &'a mut PhaseCo);

impl<'a> Phase<'a> {
    /// Hand `operation` to the engine and await its outcome. Returns the populated [`PhaseState`]
    /// on success or the raw [`EngineError`] on failure; callers match on
    /// [`EngineError::kind`](crate::plans::state_machines::framework::engine_error::EngineError::kind)
    /// and lift into a kernel [`DeltaError`](crate::plans::errors::DeltaError) via
    /// [`EngineError::into_delta`].
    pub(crate) async fn execute(
        &mut self,
        operation: PhaseOperation,
        name: &'static str,
    ) -> Result<PhaseState, EngineError> {
        let PhaseResume(result) = self
            .0
            .yield_(PhaseYield {
                operation,
                phase_name: name,
            })
            .await;
        result
    }
}
