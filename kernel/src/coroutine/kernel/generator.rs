//! Streaming kernel operations that defer execution until bound or started.
//!
//! [`Generator::bind`] shares a parent task's request channel and exposes items through
//! [`BoundGenerator::next`]. [`Generator::start`] creates a [`GeneratorTask`] whose driver handles
//! request, yield, and completion [`GeneratorStep`]s directly.

use std::borrow::Borrow;
use std::future::{poll_fn, Future};
use std::marker::PhantomData;
use std::ops::AsyncFnOnce;
use std::task::Poll;

use delta_kernel_derive::internal_api;
use tracing::{error, Instrument as _, Span};

use super::{Channel, Request, YieldRequest, Yielder};
use crate::coroutine::core::{Channel as CoreChannel, DeltaFuture, Either, Receiver, Step, Task};
use crate::coroutine::Reply;
use crate::DeltaResult;

pub(crate) struct GeneratorImpl<F, Y, O = ()> {
    body: F,
    output: PhantomData<fn() -> (Y, O)>,
    span: Span,
}

impl<F, O, Y: Send + 'static> GeneratorImpl<F, Y, O> {
    pub(crate) fn new(body: F) -> Self
    where
        F: for<'a> AsyncFnOnce(Yielder<'a, Y>) -> DeltaResult<O>,
    {
        Self {
            body,
            output: PhantomData,
            span: Span::current(),
        }
    }

    fn into_future<'a>(
        self,
        channel: impl Borrow<Channel> + Send + 'a,
        yields: CoreChannel<YieldRequest<Y>>,
    ) -> impl Future<Output = DeltaResult<O>> + Send + 'a
    where
        F: for<'b> BindLifetime<'b, Y, O> + Send + 'a,
        Y: Send + 'static,
        O: 'a,
    {
        let future = async move {
            let yielder = Yielder {
                channel: channel.borrow(),
                yields: &yields,
            };
            BindLifetime::bind(self.body, yielder).await
        };
        async move {
            future
                .await
                .inspect_err(|err| error!(error = %err, "coroutine generator failed"))
        }
        .instrument(self.span)
    }
}

/// A lazy streaming operation that can be bound to a parent task or started as a new root.
///
/// Creating a generator performs no connector work. Calling [`bind`](Self::bind) or
/// [`start`](Self::start) consumes it.
pub trait Generator<Y: Send + 'static, O: Send + 'static = ()>:
    private::Sealed + Send + Sized
{
    /// Bind permanently to a parent task's borrowed request channel.
    ///
    /// [`BoundGenerator::next`] yields items while routing connector requests through the parent
    /// task. Dropping the bound generator abandons its remaining output.
    fn bind<'a>(self, channel: &'a Channel) -> BoundGenerator<'a, Y, O>
    where
        Self: 'a;

    /// Start an independently driven task with its own request and yield lanes.
    ///
    /// The caller must handle every [`GeneratorStep::Request`] and acknowledge every
    /// [`GeneratorStep::Yield`] until the task completes or is abandoned.
    fn start(self) -> GeneratorTask<Y, O>;
}

impl<F, O: Send + 'static, Y: Send + 'static> Generator<Y, O> for GeneratorImpl<F, Y, O>
where
    F: for<'a> BindLifetime<'a, Y, O> + Send + 'static,
{
    fn bind<'a>(self, channel: &'a Channel) -> BoundGenerator<'a, Y, O>
    where
        Self: 'a,
    {
        BoundGenerator {
            inner: Task::single_lane(move |yields| self.into_future(channel, yields)),
            complete: false,
        }
    }

    fn start(self) -> GeneratorTask<Y, O> {
        GeneratorTask(Task::dual_lane(move |channel, yields| {
            self.into_future(Channel::from(channel), yields)
        }))
    }
}

/// Consumer-side handle for a generator bound to its parent task's request channel.
pub struct BoundGenerator<'a, Y: Send + 'static, O> {
    inner: Task<'a, Receiver<YieldRequest<Y>>, O>,
    complete: bool,
}

/// One step returned to the driver of a started [`Generator`].
pub enum GeneratorStep<Y, O> {
    /// The generator yielded one item and remains suspended until its reply is completed.
    Yield(Y, Reply<()>),
    /// The generator requested connector work and remains suspended until its reply is completed.
    Request(Request),
    /// The generator completed.
    Done(O),
}

/// Connector-owned task for one started [`Generator`].
pub struct GeneratorTask<Y: Send + 'static, O: Send + 'static = ()>(
    Task<'static, (Receiver<Request>, Receiver<YieldRequest<Y>>), O>,
);

impl<Y: Send + 'static, O: Send + 'static> GeneratorTask<Y, O> {
    /// Run kernel until the generator completes, yields, or requests connector work.
    ///
    /// Calling after completion or before the preceding request or yield receives a response
    /// returns an error.
    pub fn advance(&mut self) -> DeltaResult<GeneratorStep<Y, O>> {
        match self.0.advance()? {
            Step::Done(output) => Ok(GeneratorStep::Done(output)),
            Step::Request(Either::Left(request)) => Ok(GeneratorStep::Request(request)),
            Step::Request(Either::Right(yielded)) => {
                Ok(GeneratorStep::Yield(yielded.item, yielded.reply))
            }
        }
    }

    /// Deliver `response` and run kernel until the next generator step.
    ///
    /// `reply` must come from a step previously returned by this task.
    pub fn resume<In>(
        &mut self,
        reply: Reply<In>,
        response: DeltaResult<In>,
    ) -> DeltaResult<GeneratorStep<Y, O>> {
        reply.send(response)?;
        self.advance()
    }
}

impl<Y: Send + 'static> BoundGenerator<'_, Y, ()> {
    /// Await and acknowledge the next item, or return `None` after completion.
    pub async fn next(&mut self) -> DeltaResult<Option<Y>> {
        if self.complete {
            return Ok(None);
        }
        poll_fn(|context| match self.inner.poll(context) {
            Poll::Ready(Ok(Step::Done(()))) => {
                self.complete = true;
                Poll::Ready(Ok(None))
            }
            Poll::Ready(Ok(Step::Request(yielded))) => {
                Poll::Ready(yielded.reply.send(Ok(())).map(|()| Some(yielded.item)))
            }
            Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
            Poll::Pending => Poll::Pending,
        })
        .await
    }
}

trait ErasedGenerator<Y: Send + 'static>: Send {
    fn bind_boxed<'a>(self: Box<Self>, channel: &'a Channel) -> BoundGenerator<'a, Y, ()>
    where
        Self: 'a;

    fn start_boxed(self: Box<Self>) -> GeneratorTask<Y>;
}

/// Lazy generator with its body type erased.
#[internal_api]
pub(crate) struct BoxedGenerator<Y: Send + 'static>(Box<dyn ErasedGenerator<Y>>);

impl<Y: Send + 'static> BoxedGenerator<Y> {
    pub(crate) fn new<G>(generator: G) -> Self
    where
        G: Generator<Y> + 'static,
    {
        Self(Box::new(generator))
    }
}

impl<Y: Send + 'static> Generator<Y> for BoxedGenerator<Y> {
    fn bind<'a>(self, channel: &'a Channel) -> BoundGenerator<'a, Y, ()>
    where
        Self: 'a,
    {
        self.0.bind_boxed(channel)
    }
    fn start(self) -> GeneratorTask<Y> {
        self.0.start_boxed()
    }
}

impl<G: Generator<Y> + 'static, Y: Send + 'static> ErasedGenerator<Y> for G {
    fn bind_boxed<'a>(self: Box<Self>, channel: &'a Channel) -> BoundGenerator<'a, Y, ()>
    where
        Self: 'a,
    {
        Generator::bind(*self, channel)
    }

    fn start_boxed(self: Box<Self>) -> GeneratorTask<Y> {
        Generator::start(*self)
    }
}

/// Proves that a body factory produces a sendable future for binding lifetime `'a`.
pub(crate) trait BindLifetime<'a, Y: Send + 'static, O> {
    /// Future created by the factory.
    type Future: DeltaFuture<O> + 'a;

    /// Bind the factory to `yielder`.
    fn bind(self, yielder: Yielder<'a, Y>) -> Self::Future;
}

impl<'a, F, O, Fut: DeltaFuture<O> + 'a, Y: Send + 'static> BindLifetime<'a, Y, O> for F
where
    F: FnOnce(Yielder<'a, Y>) -> Fut,
{
    type Future = Fut;

    fn bind(self, yielder: Yielder<'a, Y>) -> Self::Future {
        self(yielder)
    }
}

mod private {
    pub trait Sealed {}
}

impl<F, Y, O> private::Sealed for GeneratorImpl<F, Y, O> {}

impl<Y: Send + 'static, O> private::Sealed for BoundGenerator<'_, Y, O> {}

impl<Y: Send + 'static> private::Sealed for BoxedGenerator<Y> {}
