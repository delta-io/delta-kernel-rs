//! Coroutine workflows that complete with one final output.
//!
//! Independently driven entry points return an unpolled [`StaticWorkflow`] that owns its request
//! channel. Calling [`Workflow::advance`] runs the operation to its next request or completion. A
//! workflow may issue many requests, but unlike a [`super::generator::Generator`] it returns a
//! final output value instead of yielding intermediate items. Nested workflow logic passes a
//! borrowed [`Channel`] to ordinary async functions.

use std::future::Future;
use std::mem::ManuallyDrop;
use std::ops::AsyncFnOnce;
use std::sync::Arc;

use delta_kernel_derive::internal_api;
use tracing::{error, Instrument as _, Span};

use super::core::{Channel, DeltaFuture, Step, Task};
use crate::utils::PhantomType;
use crate::DeltaResult;

/// Independently driven operation with an owned request channel.
///
/// Process each returned request and complete its reply.
pub struct Workflow<'task, P: Send + 'static, O>(Task<'task, P, O>);

/// A Workflow that owns all captured state.
pub type StaticWorkflow<P, O> = Workflow<'static, P, O>;

/// Request or completion produced by advancing a [`Workflow`].
// The request variant is the common case, and boxing `P` would add an allocation to every request.
#[allow(clippy::large_enum_variant)]
pub enum WorkflowStep<P, O> {
    /// Connector work offered by the workflow.
    Request(P),
    /// Terminal output.
    Done(O),
}

impl<'task, P: Send + 'static, O> Workflow<'task, P, O> {
    /// Creates a workflow and passes its request channel to `make_body`.
    fn new<Fut>(make_body: impl FnOnce(Arc<Channel<P>>) -> Fut) -> Self
    where
        Fut: DeltaFuture<O> + 'task,
    {
        Self(Task::new(make_body))
    }

    /// Runs until the next request or completion.
    ///
    /// Calling after completion returns an error.
    pub fn advance(&mut self) -> DeltaResult<WorkflowStep<P, O>> {
        match self.0.advance()? {
            Step::Done(output) => Ok(WorkflowStep::Done(output)),
            Step::Request(request) => Ok(WorkflowStep::Request(request)),
        }
    }
}

/// Creates a lazy workflow for a request vocabulary.
///
/// The workflow owns its request channel but may borrow captured state.
///
/// The request type determines the concrete [`Channel`] received by the body. A vocabulary can
/// add typed operations with an extension trait over that channel:
///
/// ```
/// use delta_kernel::coroutine::workflow::{workflow, WorkflowStep};
/// use delta_kernel::coroutine::{Channel, ChannelExchange, DeltaFuture, Reply};
///
/// enum Request {
///     Length(String, Reply<usize>),
/// }
///
/// trait ChannelExt: ChannelExchange<Request> {
///     fn length(&self, value: String) -> impl DeltaFuture<usize> {
///         self.exchange(value, Request::Length)
///     }
/// }
///
/// impl ChannelExt for Channel<Request> {}
///
/// let mut workflow = workflow!(Request, async move |channel| {
///     let length = channel.length("delta".to_string()).await?;
///     Ok(length * 2)
/// });
/// let WorkflowStep::Request(Request::Length(value, reply)) = workflow.advance()? else {
///     unreachable!("workflow did not request a length");
/// };
/// reply.send(Ok(value.len()));
/// let WorkflowStep::Done(output) = workflow.advance()? else {
///     unreachable!("workflow did not complete");
/// };
/// assert_eq!(output, 10);
/// # Ok::<(), delta_kernel::Error>(())
/// ```
#[internal_api]
macro_rules! workflow {
    ($request:ty, $body:expr) => {
        $crate::coroutine::workflow::WorkflowFn::<_, $request, _>::new($body)
            .into_workflow_with($crate::coroutine::workflow::WorkflowFn::witness)
    };
}

/// Creates a lazy workflow that owns all captured state.
///
/// ```
/// use delta_kernel::coroutine::workflow::{static_workflow, StaticWorkflow};
///
/// enum Request {}
///
/// let workflow: StaticWorkflow<Request, ()> =
///     static_workflow!(Request, async move |_channel| Ok(()));
/// ```
#[internal_api]
macro_rules! static_workflow {
    ($request:ty, $body:expr) => {
        $crate::coroutine::workflow::WorkflowFn::<_, $request, _>::new($body)
            .into_workflow_with::<'static, _>($crate::coroutine::workflow::WorkflowFn::witness)
    };
}

/// Drive a workflow to completion with a strict request handler.
///
/// The handler expression runs once for each [`WorkflowStep::Request`] and must produce `()`.
/// [`WorkflowStep::Done`] returns the workflow output.
///
/// The handler is expanded in the caller's context, so it may use `?`, `.await`, `return`, or
/// `break`.
///
/// ```
/// use delta_kernel::coroutine::workflow::{drive_workflow, workflow, WorkflowStep};
/// use delta_kernel::coroutine::{Channel, ChannelExchange, DeltaFuture, Reply};
///
/// enum Request {
///     Length(String, Reply<usize>),
/// }
///
/// trait ChannelExt: ChannelExchange<Request> {
///     fn length(&self, value: String) -> impl DeltaFuture<usize> {
///         self.exchange(value, Request::Length)
///     }
/// }
///
/// impl ChannelExt for Channel<Request> {}
///
/// let workflow = workflow!(Request, async move |channel| {
///     Ok(channel.length("delta".to_string()).await? * 2)
/// });
/// let output = drive_workflow!(workflow, |request| match request {
///     Request::Length(value, reply) => {
///         reply.send(Ok(value.len()));
///     }
/// });
/// assert_eq!(output, 10);
///
/// // Equivalent expansion:
/// let mut workflow = workflow!(Request, async move |channel| {
///     Ok(channel.length("kernel".to_string()).await? * 2)
/// });
/// let output = loop {
///     match workflow.advance()? {
///         WorkflowStep::Done(output) => break output,
///         WorkflowStep::Request(Request::Length(value, reply)) => {
///             reply.send(Ok(value.len()));
///         }
///     }
/// };
/// assert_eq!(output, 12);
/// # Ok::<(), delta_kernel::Error>(())
/// ```
#[internal_api]
macro_rules! drive_workflow {
    ($workflow:expr, |$request:ident| $handler:expr) => {{
        let mut workflow = $workflow;
        loop {
            match workflow.advance()? {
                $crate::coroutine::workflow::WorkflowStep::Done(output) => break output,
                $crate::coroutine::workflow::WorkflowStep::Request($request) => $handler,
            }
        }
    }};
}

/// An uninvoked operation body that becomes a [`Workflow`] when called with its request channel.
///
/// The body receives a borrowed [`Channel`] and completes with `O`. [`Self::new`] stores its
/// concrete closure type `F` and tracing span without requiring `F: Send + 'static` or exposing the
/// body's future.
///
/// Constructing a workflow additionally requires a `Future + Send + 'task`, but stable Rust cannot
/// name or constrain an `AsyncFnOnce` closure's hidden call future.
///
/// Workflow construction occurs while `F` is concrete, so the macro passes [`Self::witness`] to a
/// constructor that can name its output `Fut` and prove it is a [`DeltaFuture`]. Static workflow
/// construction constrains the returned [`Workflow`] to a `'static` lifetime.
#[doc(hidden)]
#[internal_api]
pub(crate) struct WorkflowFn<F, P, O> {
    body: F,
    span: Span,
    _type_signature: PhantomType<(P, O)>,
}

impl<F, P: Send + 'static, O> WorkflowFn<F, P, O> {
    /// Stores `body` without choosing its call-future proof path.
    #[internal_api]
    pub(crate) fn new(body: F) -> Self
    where
        F: for<'call> AsyncFnOnce(&'call Channel<P>) -> DeltaResult<O>,
    {
        Self {
            body,
            span: Span::current(),
            _type_signature: PhantomType::default(),
        }
    }

    /// Creates a scoped workflow using the concrete call-future proof supplied by `witness`.
    #[internal_api]
    pub(crate) fn into_workflow_with<'task, Fut>(
        self,
        witness: impl FnOnce(Self, Arc<Channel<P>>) -> Fut,
    ) -> Workflow<'task, P, O>
    where
        Fut: DeltaFuture<O> + 'task,
        P: 'task,
        O: 'task,
    {
        Workflow::new(move |channel| witness(self, channel))
    }

    /// Runs the body while exposing its concrete future to [`Self::into_workflow_with`].
    #[internal_api]
    pub(crate) fn witness(self, channel: Arc<Channel<P>>) -> impl Future<Output = DeltaResult<O>>
    where
        F: for<'call> AsyncFnOnce(&'call Channel<P>) -> DeltaResult<O>,
    {
        let future = async move {
            let guard = WorkflowCompletionGuard;
            let output = (self.body)(channel.as_ref()).await;
            let _ = ManuallyDrop::new(guard);
            output.inspect_err(|err| error!(error = %err, "coroutine workflow failed"))
        };
        future.instrument(self.span)
    }
}

/// Logs when a workflow is dropped before producing its terminal result.
///
/// Early termination is ordinary for generators, but workflows must complete to produce a useful
/// output. The guard is disarmed with `ManuallyDrop` once the body completes. `Instrumented` enters
/// the workflow span while dropping the body, so the abandonment event retains its context.
struct WorkflowCompletionGuard;

impl Drop for WorkflowCompletionGuard {
    fn drop(&mut self) {
        error!(
            error = "abandoned",
            "coroutine workflow was abandoned while suspended"
        );
    }
}
