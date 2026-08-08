//! Coroutine-backed implementation of the state-machine protocol.
//!
//! The coroutine is a CPU-only sequencer. It never performs connector I/O itself: each
//! [`CoroutineEngine::execute`] call yields an operation to the external driver and resumes with
//! that operation's result. The `rc` generator flavor intentionally makes the machine `!Send`;
//! callers must drive one machine from one thread at a time.

use std::future::Future;
use std::pin::Pin;
use std::{fmt, mem};

use genawaiter2::rc::{Co, Gen};
use genawaiter2::GeneratorState;
use uuid::Uuid;

use super::{EngineRequest, EngineResponse, NextStep, StateMachine};
use crate::plans::{Operation, PlanResult};
use crate::{DeltaResult, Error};

struct StepYield {
    request: EngineRequest,
    step_name: &'static str,
}

struct StepResume(Option<Result<EngineResponse, Error>>);

impl fmt::Debug for StepResume {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            Some(Ok(response)) => f.debug_tuple("StepResume").field(response).finish(),
            Some(Err(error)) => f.debug_tuple("StepResume").field(error).finish(),
            None => f.write_str("StepResume(Prime)"),
        }
    }
}

type YieldChannel = Co<StepYield, StepResume>;
type InnerGen<R> = Gen<StepYield, StepResume, Pin<Box<dyn Future<Output = DeltaResult<R>>>>>;

/// Handle used by coroutine bodies to request connector work.
pub(crate) struct CoroutineEngine {
    channel: YieldChannel,
}

impl CoroutineEngine {
    /// Yield `operation` to the driver and resume with its plan result.
    pub(crate) async fn execute(
        &mut self,
        operation: Operation,
        step_name: &'static str,
    ) -> DeltaResult<PlanResult> {
        let response = self
            .channel
            .yield_(StepYield {
                request: EngineRequest::Execute(operation),
                step_name,
            })
            .await
            .0
            .ok_or_else(|| Error::internal_error("missing state-machine engine response"))??;
        let EngineResponse::Plan(result) = response;
        Ok(result)
    }
}

/// A state machine implemented as stackless coroutine control flow.
#[must_use = "a state machine must be driven to completion"]
pub struct CoroutineSM<R: 'static> {
    generator: InnerGen<R>,
    position: Position<R>,
    id: Uuid,
    kind: &'static str,
}

enum Position<R> {
    Yielded(StepYield),
    ResultReady(DeltaResult<R>),
    Done,
}

impl<R: 'static> CoroutineSM<R> {
    /// Construct and prime a coroutine workflow.
    pub(crate) fn new<F, Fut>(kind: &'static str, producer: F) -> DeltaResult<Self>
    where
        F: FnOnce(CoroutineEngine, Uuid) -> Fut + 'static,
        Fut: Future<Output = DeltaResult<R>> + 'static,
    {
        let id = Uuid::new_v4();
        let mut generator: InnerGen<R> = Gen::new(move |channel| {
            Box::pin(producer(CoroutineEngine { channel }, id))
                as Pin<Box<dyn Future<Output = DeltaResult<R>>>>
        });
        let position = match generator.resume_with(StepResume(None)) {
            GeneratorState::Yielded(step) => Position::Yielded(step),
            GeneratorState::Complete(result) => Position::ResultReady(result),
        };
        Ok(Self {
            generator,
            position,
            id,
            kind,
        })
    }

    /// Whether the terminal result has been returned to the driver.
    pub fn is_done(&self) -> bool {
        matches!(self.position, Position::Done)
    }

    /// Identifier unique to this state-machine instance.
    pub fn id(&self) -> Uuid {
        self.id
    }

    /// Static workflow kind used for diagnostics.
    pub fn kind(&self) -> &'static str {
        self.kind
    }
}

impl<R: 'static> StateMachine for CoroutineSM<R> {
    type Result = R;

    fn get_step(&mut self) -> DeltaResult<NextStep<R>> {
        match mem::replace(&mut self.position, Position::Done) {
            Position::Yielded(step) => {
                let request = step.request.clone();
                self.position = Position::Yielded(step);
                Ok(NextStep::Execute(request))
            }
            Position::ResultReady(result) => result.map(NextStep::Done),
            Position::Done => Err(Error::internal_error("state machine already completed")),
        }
    }

    fn submit(&mut self, result: Result<EngineResponse, Error>) -> DeltaResult<()> {
        match mem::replace(&mut self.position, Position::Done) {
            Position::ResultReady(result) => {
                self.position = Position::ResultReady(result);
                Err(Error::internal_error(
                    "cannot submit when a state-machine result is ready",
                ))
            }
            Position::Done => Err(Error::internal_error(
                "cannot submit to a completed state machine",
            )),
            Position::Yielded(_) => match self.generator.resume_with(StepResume(Some(result))) {
                GeneratorState::Yielded(step) => {
                    self.position = Position::Yielded(step);
                    Ok(())
                }
                GeneratorState::Complete(result) => {
                    self.position = Position::ResultReady(result);
                    Ok(())
                }
            },
        }
    }

    fn step_name(&self) -> &'static str {
        match &self.position {
            Position::Yielded(step) => step.step_name,
            Position::ResultReady(_) | Position::Done => "done",
        }
    }
}

#[cfg(test)]
mod tests {
    use url::Url;

    use super::*;
    use crate::plans::IoOperation;

    fn request(path: &str) -> Operation {
        Operation::IoOperation(IoOperation::file_listing(Url::parse(path).unwrap()))
    }

    #[test]
    fn executes_two_steps_in_order() {
        let mut sm = CoroutineSM::new("test", |mut engine, _| async move {
            let _ = engine.execute(request("memory:///a"), "a").await?;
            let _ = engine.execute(request("memory:///b"), "b").await?;
            Ok(42)
        })
        .unwrap();

        assert_eq!(sm.kind(), "test");
        assert_eq!(sm.step_name(), "a");
        assert!(matches!(sm.get_step(), Ok(NextStep::Execute(_))));
        assert!(matches!(
            sm.submit(Ok(EngineResponse::Plan(PlanResult::FileMeta(Box::new(
                std::iter::empty()
            ))))),
            Ok(())
        ));
        assert_eq!(sm.step_name(), "b");
        assert!(matches!(sm.get_step(), Ok(NextStep::Execute(_))));
        assert!(matches!(
            sm.submit(Ok(EngineResponse::Plan(PlanResult::FileMeta(Box::new(
                std::iter::empty()
            ))))),
            Ok(())
        ));
        assert!(matches!(sm.get_step(), Ok(NextStep::Done(42))));
        assert!(sm.is_done());
    }

    #[test]
    fn returns_zero_yield_result_from_first_get_step() {
        let mut sm = CoroutineSM::new("test", |_, _| async move { Ok(7) }).unwrap();
        assert!(matches!(sm.get_step(), Ok(NextStep::Done(7))));
    }

    #[test]
    fn propagates_executor_error_through_the_body() {
        let mut sm = CoroutineSM::<()>::new("test", |mut engine, _| async move {
            engine.execute(request("memory:///a"), "a").await?;
            Ok(())
        })
        .unwrap();
        let error = Error::generic("executor failed");
        assert!(sm.submit(Err(error)).is_ok());
        assert!(sm.get_step().is_err());
        assert!(sm.is_done());
    }
}
