//! Finite kernel operations that defer execution until composed or started.
//!
//! [`Workflow::run_with`] composes an operation into a parent request channel.
//! [`Workflow::start`] creates a [`WorkflowTask`] whose driver alternates between advancing kernel
//! and completing the reply carried by each [`WorkflowStep::Request`].

use std::future::Future;
use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::ops::AsyncFnOnce;

use tracing::{error, Instrument as _, Span};

use super::{Channel, Request};
use crate::coroutine::core::{DeltaFuture, Receiver, Step, Task};
use crate::coroutine::Reply;
use crate::DeltaResult;

pub(crate) struct WorkflowImpl<F, O> {
    body: F,
    output: PhantomData<fn() -> O>,
    span: Span,
}

impl<F, O> WorkflowImpl<F, O> {
    pub(crate) fn new(body: F) -> Self
    where
        F: for<'a> AsyncFnOnce(&'a Channel) -> DeltaResult<O>,
    {
        Self {
            body,
            output: PhantomData,
            span: Span::current(),
        }
    }
}

/// A lazy finite operation that can be composed into a parent task or started as a new root.
///
/// Calling [`run_with`](Self::run_with) or [`start`](Self::start) consumes the workflow. Creating
/// it performs no connector work.
pub trait Workflow: private::Sealed + Send + Sized + 'static {
    /// Value produced when the workflow completes.
    type Output: Send + 'static;

    /// Run inside an existing kernel task using its borrowed request channel.
    ///
    /// Requests made by this workflow surface from the parent task.
    fn run_with<'a>(
        self,
        channel: &'a Channel,
    ) -> impl Future<Output = DeltaResult<Self::Output>> + Send + 'a;

    /// Start an independently driven task with its own request channel.
    ///
    /// The caller must retain and advance the returned task until it completes or is abandoned.
    fn start(self) -> WorkflowTask<Self::Output>;
}

impl<F, O: Send + 'static> Workflow for WorkflowImpl<F, O>
where
    F: for<'a> RunWithLifetime<'a, O> + Send + 'static,
    O: Send + 'static,
{
    type Output = O;

    fn run_with<'a>(
        self,
        channel: &'a Channel,
    ) -> impl Future<Output = DeltaResult<Self::Output>> + Send + 'a {
        self.body.call(channel).instrument(self.span)
    }

    fn start(self) -> WorkflowTask<Self::Output> {
        WorkflowTask(Task::single_lane(move |channel| {
            let channel = Channel(channel);
            let future = async move { self.body.call(&channel).await };
            async move {
                let guard = WorkflowCompletionGuard;
                let output = future.await;
                let _ = ManuallyDrop::new(guard);
                output.inspect_err(|err| error!(error = %err, "coroutine workflow failed"))
            }
            .instrument(self.span)
        }))
    }
}

/// Connector-owned task for one started [`Workflow`].
pub struct WorkflowTask<O: Send + 'static>(Task<'static, Receiver<Request>, O>);

/// Completion or connector work returned by advancing a [`WorkflowTask`].
pub type WorkflowStep<O> = Step<Request, O>;

impl<O: Send + 'static> WorkflowTask<O> {
    /// Run kernel until the workflow completes or requests connector work.
    ///
    /// Calling after completion or before the preceding request receives a response returns an
    /// error.
    pub fn advance(&mut self) -> DeltaResult<WorkflowStep<O>> {
        self.0.advance()
    }

    /// Deliver `response` and run kernel until the next workflow step.
    ///
    /// `reply` must come from a step previously returned by this task.
    pub fn resume<In>(
        &mut self,
        reply: Reply<In>,
        response: DeltaResult<In>,
    ) -> DeltaResult<WorkflowStep<O>> {
        reply.send(response)?;
        self.advance()
    }
}

/// Proves that a workflow factory produces a sendable future for binding lifetime `'a`.
trait RunWithLifetime<'a, O> {
    /// Future created by the factory.
    type Future: DeltaFuture<O> + 'a;

    /// Bind the factory to `channel`.
    fn call(self, channel: &'a Channel) -> Self::Future;
}

impl<'a, O, F, Fut: DeltaFuture<O> + 'a> RunWithLifetime<'a, O> for F
where
    F: FnOnce(&'a Channel) -> Fut,
{
    type Future = Fut;

    fn call(self, channel: &'a Channel) -> Self::Future {
        self(channel)
    }
}

struct WorkflowCompletionGuard;

impl Drop for WorkflowCompletionGuard {
    fn drop(&mut self) {
        error!(
            error = "abandoned",
            "coroutine workflow was abandoned while suspended"
        );
    }
}

mod private {
    pub trait Sealed {}
}

impl<F, O> private::Sealed for WorkflowImpl<F, O> {}
