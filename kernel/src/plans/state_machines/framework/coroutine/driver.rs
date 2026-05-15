//! The [`CoroutineSM`] driver: compiles [`PhaseYield`] sequences into the
//! [`StateMachine`] contract.
//!
//! The protocol is strictly 1:1: each yield carries exactly one
//! [`PhaseOperation`], the driver hands it to the engine via
//! [`StateMachine::get_operation`], and on [`StateMachine::advance`] the
//! engine outcome is fed back through the coroutine as a [`PhaseResume`].
//!
//! There is no batching, no task IDs, and no same-batch replay — SM bodies
//! that need to run multiple plans in one engine call construct a
//! [`PhaseOperation::Plans`] with multiple plans and recover per-plan
//! outputs from the resulting [`PhaseState`] using their respective
//! extractors.
//!
//! **Invariant:** a successfully constructed [`CoroutineSM`] always has
//! at least one concrete phase queued. Zero-phase coroutines are invalid;
//! the builder must short-circuit before constructing an SM.
//!
//! ## Errors
//!
//! Driver/body protocol bugs surface as
//! [`GenError`](super::generator::GenError) from the underlying
//! [`Gen`](super::generator::Gen); this module lifts them into
//! [`DeltaError`] (`DeltaCommandInvariantViolation`). User-facing failures
//! produced by the engine flow through [`PhaseResume`] as
//! `Result<PhaseState, EngineError>` and are not handled here.

use std::future::Future;

use super::super::engine_error::EngineError;
use super::super::phase_operation::PhaseOperation;
use super::super::phase_state::PhaseState;
use super::super::state_machine::{AdvanceResult, StateMachine};
use super::generator::{Co, Gen, GenError, GeneratorState};
use super::phase::{PhaseCo, PhaseResume, PhaseYield};
use crate::plans::errors::{DeltaError, DeltaErrorCode};
use crate::{bail_delta, delta_error};

type InnerGen<R> = Gen<PhaseYield, PhaseResume, Result<R, DeltaError>>;

/// A state machine driven by a hand-rolled stackless coroutine.
///
/// At each phase the body yields a single [`PhaseYield`] (operation +
/// static phase name) and awaits the engine outcome. The driver simply
/// shuttles operations to [`StateMachine::get_operation`] and resumes the
/// body with the matching [`PhaseResume`] on [`StateMachine::advance`].
pub struct CoroutineSM<R: Send + 'static> {
    gen: InnerGen<R>,
    /// The yield we're currently positioned at — `Some` between
    /// `get_operation` (which reads it) and the next `advance` (which
    /// consumes it). `None` once the coroutine has completed.
    pending: Option<PhaseYield>,
}

impl<R: Send + 'static> CoroutineSM<R> {
    /// Construct a coroutine state machine and run the producer up to its
    /// first yield.
    ///
    /// The first yield becomes the initial [`PhaseYield`] returned by
    /// [`StateMachine::get_operation`]. If the producer completes without
    /// ever yielding, construction fails — the caller is expected to
    /// short-circuit that case in its builder rather than fake a plan.
    pub(crate) fn new<F, Fut>(producer: F) -> Result<Self, DeltaError>
    where
        F: FnOnce(PhaseCo) -> Fut + Send + 'static,
        Fut: Future<Output = Result<R, DeltaError>> + Send + 'static,
    {
        let mut gen: InnerGen<R> = Gen::new(move |co: Co<PhaseYield, PhaseResume>| producer(co));
        match gen
            .start()
            .map_err(|e| lift_gen_error(e, "CoroutineSM::new"))?
        {
            GeneratorState::Yielded(phase) => Ok(Self {
                gen,
                pending: Some(phase),
            }),
            GeneratorState::Complete(_) => bail_delta!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                operation = "CoroutineSM::new",
                detail = "coroutine completed during start without yielding any work; \
                          the caller should short-circuit before constructing an SM",
            ),
        }
    }

    /// `true` once the coroutine has terminated and [`advance`](Self::advance)
    /// has handed out its final result.
    pub fn is_done(&self) -> bool {
        self.pending.is_none()
    }
}

impl<R: Send + 'static> StateMachine for CoroutineSM<R> {
    type Result = R;

    fn get_operation(&mut self) -> Result<PhaseOperation, DeltaError> {
        match &self.pending {
            Some(phase) => Ok((*phase.operation).clone()),
            None => bail_delta!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                operation = "CoroutineSM::get_operation",
                detail = "state machine already completed",
            ),
        }
    }

    fn advance(
        &mut self,
        result: Result<PhaseState, EngineError>,
    ) -> Result<AdvanceResult<R>, DeltaError> {
        if self.pending.is_none() {
            bail_delta!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                operation = "CoroutineSM::advance",
                detail = "cannot advance, already completed",
            );
        }

        match self
            .gen
            .resume_with(PhaseResume(result))
            .map_err(|e| lift_gen_error(e, "CoroutineSM::advance"))?
        {
            GeneratorState::Yielded(next) => {
                self.pending = Some(next);
                Ok(AdvanceResult::Continue)
            }
            GeneratorState::Complete(final_result) => {
                self.pending = None;
                final_result.map(AdvanceResult::Done)
            }
        }
    }

    fn phase_name(&self) -> &'static str {
        match &self.pending {
            Some(phase) => phase.phase_name,
            None => "complete",
        }
    }
}

/// Convert a coroutine-shim protocol bug into a kernel-level `DeltaError`.
/// All [`GenError`] variants signal a kernel-internal contract violation;
/// they should never be observed at runtime in correct code.
fn lift_gen_error(e: GenError, operation: &'static str) -> DeltaError {
    delta_error!(
        DeltaErrorCode::DeltaCommandInvariantViolation,
        operation = operation,
        detail = e.to_string(),
        source = e,
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::plans::ir::nodes::{ScanNode, SinkNode, SinkType};
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
            schema,
        ));
        let sink = SinkNode {
            sink_type: SinkType::Results(None),
        };
        Plan::new(root, sink)
    }

    /// Drive a two-phase SM: yield op A, observe empty PhaseState, yield op B,
    /// complete.
    #[test]
    fn two_phase_sm_executes_in_sequence() {
        let mut sm = CoroutineSM::<i64>::new(|mut co| async move {
            let mut phase = super::super::phase::Phase(&mut co);
            let _ = phase
                .execute(PhaseOperation::Plans(vec![toy_plan()]), "phase_a")
                .await
                .map_err(EngineError::into_delta_error_for_test)?;
            let _ = phase
                .execute(PhaseOperation::Plans(vec![toy_plan()]), "phase_b")
                .await
                .map_err(EngineError::into_delta_error_for_test)?;
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
                .map_err(EngineError::into_delta_error_for_test)?;
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

    /// Engine errors flow through as `Err(EngineError)` on resume; the SM
    /// body receives them and decides what to do.
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

    /// Coroutines that complete without yielding a single operation should be
    /// rejected at construction.
    #[test]
    fn zero_phase_coroutine_is_rejected() {
        let r = CoroutineSM::<i32>::new(|_co| async move { Ok(7) });
        assert!(r.is_err(), "no-yield coroutine should fail at new()");
    }

    impl EngineError {
        fn into_delta_error_for_test(self) -> DeltaError {
            crate::delta_error!(
                DeltaErrorCode::DeltaCommandInvariantViolation,
                operation = "test::lift_engine_error",
                detail = self.display_with_source_chain(),
                source = self,
            )
        }
    }
}
