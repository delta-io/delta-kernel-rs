//! Coroutine yield/resume protocol shared by the [`Coroutine`](super::driver::Coroutine)
//! driver and the SSA [`crate::plans::operations::framework::plan_context::Context`].
//!
//! Each yield carries a single [`StepYield`] (operation + static phase name + an optional
//! debug-only live-relation snapshot), and the driver resumes the body with a [`StepResume`]
//! wrapping the engine outcome. SM bodies emit yields through the SSA `Context`'s
//! `execute` / `execute_consume` helpers; this module only defines the protocol types and
//! the underlying [`StepCo`] alias.

use crate::plans::operations::framework::engine_error::EngineError;
use crate::plans::operations::framework::step::Step;
use crate::plans::operations::framework::step_result::StepResult;

/// Value yielded by a coroutine at each phase boundary. Carries the operation envelope and the
/// phase name.
pub(crate) struct StepYield {
    pub operation: Step,
    pub step_name: &'static str,
}

/// Value the driver passes back to the coroutine on resume. Wraps the engine outcome for the most
/// recent [`StepYield::operation`].
pub(crate) struct StepResume(pub Result<StepResult, EngineError>);

impl std::fmt::Debug for StepResume {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            Ok(_) => f.write_str("StepResume(Ok(..))"),
            Err(e) => write!(f, "StepResume(Err({:?}))", e.kind),
        }
    }
}

/// Coroutine handle used inside phase bodies. Alias over [`genawaiter2::rc::Co`].
///
/// `genawaiter2::rc` (not `sync`) is intentional: the [`Coroutine`](super::driver::Coroutine)
/// driver does not need a `Send` future bound (see the driver module docs for why), and
/// `rc::Co` is `!Send` -- the SM body's future inherits that, which is the correct
/// architectural shape for a CPU-only sequencer.
pub(crate) type StepCo = genawaiter2::rc::Co<StepYield, StepResume>;
