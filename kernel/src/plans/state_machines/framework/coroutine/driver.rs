//! The [`CoroutineSM`] driver: compiles [`PhaseYield`] sequences into the
//! [`StateMachine`] contract.
//!
//! The protocol is strictly 1:1: each yield carries exactly one [`PhaseOperation`], the driver
//! hands it to the engine via [`StateMachine::get_operation`], and on [`StateMachine::advance`]
//! the engine outcome is fed back through the coroutine as a [`PhaseResume`].
//!
//! There is no batching, no task IDs, and no same-batch replay — SM bodies that need to run
//! multiple plans in one engine call construct a [`PhaseOperation::Plans`] with multiple plans
//! and recover per-plan outputs from the resulting [`PhaseState`] using their respective
//! extractors.
//!
//! Zero-yield coroutines are allowed: an SM body that has no intermediate engine work to do may
//! simply compute and return its terminal value, in which case [`StateMachine::advance`]
//! immediately reports [`AdvanceResult::Done`].
//!
//! ## genawaiter2 panic policy
//!
//! This driver is built on [`genawaiter2`], which panics on protocol misuse (the body awaits a
//! non-yield future, the body retains a `Co` after returning, etc.). The kernel's no-panic policy
//! is preserved because the only path to `Co::yield_` is through [`Phase::execute`]; bodies
//! awaiting that single API are correct by construction. Bodies are kernel-internal,
//! `pub(crate)`-only, and FFI consumers see the SM only through `Box<dyn StateMachine>`, so the
//! panic paths are unreachable in any externally-exposed code path.
//!
//! ## Errors
//!
//! User-facing failures produced by the engine flow through [`PhaseResume`] as
//! `Result<PhaseState, EngineError>`; SM bodies translate to a typed kernel
//! [`DeltaError`](crate::plans::errors::DeltaError) via
//! [`EngineError::into_delta`](crate::plans::state_machines::framework::engine_error::EngineError::into_delta)
//! at each call site.

use std::future::Future;

use genawaiter2::sync::GenBoxed;
use genawaiter2::GeneratorState;

use super::super::engine_error::EngineError;
use super::super::phase_operation::PhaseOperation;
use super::super::phase_state::PhaseState;
use super::super::state_machine::{AdvanceResult, StateMachine};
use super::phase::{PhaseCo, PhaseResume, PhaseYield};
use crate::bail_delta;
use crate::plans::errors::{DeltaError, DeltaErrorCode};

type InnerGen<R> = GenBoxed<PhaseYield, PhaseResume, Result<R, DeltaError>>;

/// A state machine driven by a stackless coroutine (via [`genawaiter2`]).
///
/// At each phase the body yields a single [`PhaseYield`] (operation + static phase name) and
/// awaits the engine outcome. The driver shuttles operations to [`StateMachine::get_operation`]
/// and resumes the body with the matching [`PhaseResume`] on [`StateMachine::advance`].
pub struct CoroutineSM<R: Send + 'static> {
    gen: InnerGen<R>,
    /// Where the SM is positioned. `Yielded` while engine work is pending; `ResultReady` when the
    /// body completed without ever yielding (the next `advance` hands the stored result back);
    /// `Done` once `advance` has returned the terminal result.
    state: SmPosition<R>,
}

/// Internal positioning of a [`CoroutineSM`]. Drives the [`StateMachine`] contract.
enum SmPosition<R> {
    /// A phase is queued; engine work pending.
    Yielded(PhaseYield),
    /// Body has produced its terminal result; awaiting `advance` to hand it back. Reached only by
    /// zero-yield SMs (multi-yield SMs skip through this state inside `advance` and transition
    /// straight to `Done`).
    ResultReady(Result<R, DeltaError>),
    /// Result has been consumed; terminal sink.
    Done,
}

impl<R: Send + 'static> CoroutineSM<R> {
    /// Construct a coroutine state machine and run the producer to its first yield (or to
    /// completion).
    ///
    /// If the producer yields, the first yield becomes the initial [`PhaseYield`] returned by
    /// [`StateMachine::get_operation`]. If it completes without yielding, the terminal result is
    /// stored and handed back from the first [`StateMachine::advance`] call -- callers don't need
    /// to special-case zero-yield SMs.
    pub(crate) fn new<F, Fut>(producer: F) -> Result<Self, DeltaError>
    where
        F: FnOnce(PhaseCo) -> Fut + Send + 'static,
        Fut: Future<Output = Result<R, DeltaError>> + Send + 'static,
    {
        let mut gen: InnerGen<R> = GenBoxed::new_boxed(producer);
        // genawaiter2's `Gen<Y, R, F>` exposes only `resume_with(R)`; the first resume arg is
        // discarded because no future is yet awaiting it. We pass an inert
        // `Ok(PhaseState::empty())` sentinel that the body would never observe.
        let state = match gen.resume_with(PhaseResume(Ok(PhaseState::empty()))) {
            GeneratorState::Yielded(phase) => SmPosition::Yielded(phase),
            GeneratorState::Complete(result) => SmPosition::ResultReady(result),
        };
        Ok(Self { gen, state })
    }

    /// `true` once the coroutine has terminated and [`advance`](Self::advance) has handed out its
    /// final result.
    pub fn is_done(&self) -> bool {
        matches!(self.state, SmPosition::Done)
    }
}

impl<R: Send + 'static> StateMachine for CoroutineSM<R> {
    type Result = R;

    fn get_operation(&mut self) -> Result<PhaseOperation, DeltaError> {
        match &self.state {
            SmPosition::Yielded(phase) => Ok(phase.operation.clone()),
            SmPosition::ResultReady(_) => bail_delta!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "CoroutineSM::get_operation: zero-yield SM has no pending operation; \
                 call advance() to receive the terminal result",
            ),
            SmPosition::Done => bail_delta!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "CoroutineSM::get_operation: state machine already completed",
            ),
        }
    }

    fn advance(
        &mut self,
        result: Result<PhaseState, EngineError>,
    ) -> Result<AdvanceResult<R>, DeltaError> {
        match std::mem::replace(&mut self.state, SmPosition::Done) {
            SmPosition::ResultReady(value) => value.map(AdvanceResult::Done),
            SmPosition::Done => bail_delta!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                "CoroutineSM::advance: cannot advance, already completed",
            ),
            SmPosition::Yielded(_) => match self.gen.resume_with(PhaseResume(result)) {
                GeneratorState::Yielded(next) => {
                    self.state = SmPosition::Yielded(next);
                    Ok(AdvanceResult::Continue)
                }
                GeneratorState::Complete(final_result) => final_result.map(AdvanceResult::Done),
            },
        }
    }

    fn phase_name(&self) -> &'static str {
        match &self.state {
            SmPosition::Yielded(phase) => phase.phase_name,
            SmPosition::ResultReady(_) | SmPosition::Done => "done",
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::plans::ir::nodes::{RelationHandle, ScanNode, SinkType};
    use crate::plans::ir::{DeclarativePlanNode, Plan};
    use crate::plans::state_machines::framework::engine_error::{EngineError, EngineErrorKind};

    // A throwaway plan whose identity we don't care about — the SM tests
    // only look at the shape of `PhaseOperation`, not what's inside.
    fn toy_plan() -> Plan {
        let schema = Arc::new(crate::schema::StructType::new_unchecked(Vec::<
            crate::schema::StructField,
        >::new()));
        let root = DeclarativePlanNode::Scan(ScanNode::new(
            crate::plans::ir::nodes::FileType::Parquet,
            Vec::new(),
            schema.clone(),
        ));
        let sink = SinkType::Relation(RelationHandle::fresh("toy", schema));
        Plan::new(root, sink)
    }

    /// Drive a two-phase SM: yield op A, observe empty PhaseState, yield op B, complete.
    #[test]
    fn two_phase_sm_executes_in_sequence() {
        let mut sm = CoroutineSM::<i64>::new(|mut co| async move {
            let mut phase = super::super::phase::Phase(&mut co);
            let _ = phase
                .execute(PhaseOperation::Plans(vec![toy_plan()]), "phase_a")
                .await
                .map_err(|e| e.into_delta(DeltaErrorCode::DeltaCommandInvariantViolation))?;
            let _ = phase
                .execute(PhaseOperation::Plans(vec![toy_plan()]), "phase_b")
                .await
                .map_err(|e| e.into_delta(DeltaErrorCode::DeltaCommandInvariantViolation))?;
            Ok(42)
        })
        .unwrap();

        assert_eq!(sm.phase_name(), "phase_a");
        let op = sm.get_operation().unwrap();
        match op {
            PhaseOperation::Plans(plans) => assert_eq!(plans.len(), 1),
            _ => panic!("expected Plans"),
        }

        let r = sm.advance(Ok(PhaseState::empty())).unwrap();
        assert!(matches!(r, AdvanceResult::Continue));
        assert_eq!(sm.phase_name(), "phase_b");

        let op = sm.get_operation().unwrap();
        assert!(matches!(op, PhaseOperation::Plans(p) if p.len() == 1));

        let r = sm.advance(Ok(PhaseState::empty())).unwrap();
        match r {
            AdvanceResult::Done(v) => assert_eq!(v, 42),
            _ => panic!("expected Done"),
        }
        assert!(sm.is_done());
    }

    /// Multiple plans in a single phase are passed through verbatim.
    #[test]
    fn multi_plan_phase_executes_in_one_engine_call() {
        let mut sm = CoroutineSM::<()>::new(|mut co| async move {
            let mut phase = super::super::phase::Phase(&mut co);
            let _ = phase
                .execute(PhaseOperation::Plans(vec![toy_plan(), toy_plan()]), "ab")
                .await
                .map_err(|e| e.into_delta(DeltaErrorCode::DeltaCommandInvariantViolation))?;
            Ok(())
        })
        .unwrap();

        let op = sm.get_operation().unwrap();
        match op {
            PhaseOperation::Plans(plans) => assert_eq!(plans.len(), 2),
            _ => panic!("expected Plans"),
        }

        let r = sm.advance(Ok(PhaseState::empty())).unwrap();
        assert!(matches!(r, AdvanceResult::Done(())));
    }

    /// Engine errors flow through as `Err(EngineError)` on resume; the SM body receives them and
    /// decides what to do.
    #[test]
    fn engine_error_flows_to_body_as_resume_err() {
        let mut sm = CoroutineSM::<String>::new(|mut co| async move {
            let mut phase = super::super::phase::Phase(&mut co);
            match phase
                .execute(PhaseOperation::Plans(vec![toy_plan()]), "p")
                .await
            {
                Err(e) => Ok(format!("got: {}", e.kind)),
                Ok(_) => panic!("expected engine error"),
            }
        })
        .unwrap();

        let _ = sm.get_operation().unwrap();
        let err = EngineError::new(EngineErrorKind::FileNotFound { path: "/x".into() });
        let r = sm.advance(Err(err)).unwrap();
        match r {
            AdvanceResult::Done(s) => assert!(s.contains("file not found: /x")),
            _ => panic!("expected Done"),
        }
    }

    /// Zero-yield coroutines are allowed: the first `advance` call hands the stored terminal
    /// value back. Useful for SMs that compute their plans entirely up-front and have no
    /// intermediate engine work.
    #[test]
    fn zero_phase_coroutine_returns_value_on_first_advance() {
        let mut sm = CoroutineSM::<i32>::new(|_co| async move { Ok(7) }).unwrap();
        assert!(sm.get_operation().is_err());
        let r = sm.advance(Ok(PhaseState::empty())).unwrap();
        match r {
            AdvanceResult::Done(v) => assert_eq!(v, 7),
            _ => panic!("expected Done"),
        }
        assert!(sm.is_done());
    }
}
