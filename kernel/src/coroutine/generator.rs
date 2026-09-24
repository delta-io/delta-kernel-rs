//! Streaming generators and independently driven static wrappers.
//!
//! A streaming operation receives a [`Channel`] and returns a [`Generator`]. Code within a parent
//! coroutine awaits [`Generator::next`]; connector requests flow through the supplied channel.
//!
//! A caller without a parent channel uses [`StaticGenerator::new`] with owned inputs. The resulting
//! wrapper owns the request channel and a static generator.  [`StaticGenerator::advance`]
//! synchronously returns a [`GeneratorStep`].

use std::borrow::Borrow;
use std::future::{poll_fn, Future};
use std::ops::AsyncFnOnce;
use std::sync::Arc;
use std::task::Poll;

use delta_kernel_derive::internal_api;
use derive_more::Deref;
use tracing::{error, Instrument as _, Span};

use super::core::{noop_context, Channel, ChannelExchange, DeltaFuture, Reply, Step, Task};
use crate::utils::PhantomType;
use crate::{DeltaResult, DeltaResultIteratorStatic, Error};

#[doc(hidden)]
#[internal_api]
pub(crate) type YieldRequest<Y> = (Y, Reply<()>);

/// Active streaming operation using an existing request channel.
///
/// Connector requests surface through the driver that owns the channel. The `'task` lifetime
/// covers that channel and any state captured by the operation. Successful completion is exposed
/// as exhaustion through [`Generator::next`].
pub struct Generator<'task, Y: Send + 'static>(Task<'task, YieldRequest<Y>, ()>);

impl<'task, Y: Send + 'static> Generator<'task, Y> {
    /// Creates a generator and passes its yield channel to `make_body`.
    fn new<Fut>(make_body: impl FnOnce(Arc<Channel<YieldRequest<Y>>>) -> Fut) -> Self
    where
        Fut: DeltaFuture<()> + 'task,
    {
        Self(Task::new(make_body))
    }

    /// Waits for and acknowledges the next item, or returns `None` after completion.
    ///
    /// Connector requests made while producing the item flow through the supplied request channel.
    /// Each item is acknowledged with `Ok(())` before being returned. Body and request failures are
    /// returned to the caller.
    pub async fn next(&mut self) -> DeltaResult<Option<Y>> {
        poll_fn(|context| match self.0.poll_step(context) {
            Poll::Ready(Ok(Step::Done(()))) => Poll::Ready(Ok(None)),
            Poll::Ready(Ok(Step::Request((item, reply)))) => {
                reply.send(Ok(()));
                Poll::Ready(Ok(Some(item)))
            }
            Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
            Poll::Pending => Poll::Pending,
        })
        .await
    }
}

/// Yield, request, or completion produced by advancing a [`StaticGenerator`].
///
/// Yielded items are acknowledged before this step returns. Requests carry their own replies.
pub enum GeneratorStep<P: Send + 'static, Y: Send + 'static> {
    /// A yielded item.
    Yield(Y),
    /// Connector work offered by the generator.
    Request(P),
    /// The generator completed.
    Done,
}

/// Independently driven generator with owned request state.
///
/// A static generator owns its request channel and a [`Generator<'static, Y>`].
pub struct StaticGenerator<P: Send + 'static, Y: Send + 'static> {
    requests: Arc<Channel<P>>,
    generator: Generator<'static, Y>,
}

impl<P: Send + 'static, Y: Send + 'static> StaticGenerator<P, Y> {
    /// Constructs a static generator around an infallible channel-taking operation.
    ///
    /// `make_generator` runs immediately with the owned request channel. The returned generator
    /// remains unpolled until [`Self::advance`] is called and must own all captured state.
    pub fn new(make_generator: impl FnOnce(Arc<Channel<P>>) -> Generator<'static, Y>) -> Self {
        let channel = Arc::new(Channel::default());
        Self {
            requests: Arc::clone(&channel),
            generator: make_generator(channel),
        }
    }

    /// Runs until the next request, yielded item, or completion.
    ///
    /// Calling after completion returns an error.
    pub fn advance(&mut self) -> DeltaResult<GeneratorStep<P, Y>> {
        match self.generator.0.poll_step(&mut noop_context()) {
            Poll::Ready(Ok(Step::Done(()))) => Ok(GeneratorStep::Done),
            Poll::Ready(Ok(Step::Request((item, reply)))) => {
                reply.send(Ok(()));
                Ok(GeneratorStep::Yield(item))
            }
            Poll::Ready(Err(err)) => Err(err),
            Poll::Pending => match self.requests.take() {
                Some(request) => Ok(GeneratorStep::Request(request)),
                None => Err(Error::internal_error(
                    "coroutine suspended without posting a request",
                )),
            },
        }
    }
}

/// Creates a generator using an existing request channel.
///
/// The generator may borrow both its channel and caller state:
///
/// ```
/// use delta_kernel::coroutine::generator::{generator, Generator};
/// use delta_kernel::coroutine::Channel;
///
/// enum Request {}
///
/// fn lengths<'task>(
///     channel: &'task Channel<Request>,
///     value: &'task str,
/// ) -> Generator<'task, usize> {
///     generator!(channel, async move |channel| {
///         channel.yield_item(value.len()).await
///     })
/// }
/// ```
#[internal_api]
macro_rules! generator {
    ($channel:expr, $body:expr) => {
        $crate::coroutine::generator::GeneratorFn::<_, _, _>::new($body)
            .into_generator_with($channel, $crate::coroutine::generator::GeneratorFn::witness)
    };
}

/// Creates a lazy generator that owns its request channel and captured state.
///
/// ```
/// use delta_kernel::coroutine::generator::{static_generator, GeneratorStep};
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
/// let mut generator = static_generator!(Request, async move |channel| {
///     let length = channel.length("delta".to_string()).await?;
///     channel.yield_item(length).await
/// });
/// let GeneratorStep::Request(Request::Length(value, reply)) = generator.advance()? else {
///     unreachable!("generator did not request a length");
/// };
/// reply.send(Ok(value.len()));
/// let GeneratorStep::Yield(length) = generator.advance()? else {
///     unreachable!("generator did not yield");
/// };
/// assert_eq!(length, 5);
/// assert!(matches!(generator.advance()?, GeneratorStep::Done));
/// # Ok::<(), delta_kernel::Error>(())
/// ```
#[internal_api]
macro_rules! static_generator {
    ($request:ty, $body:expr) => {
        $crate::coroutine::generator::StaticGenerator::<$request, _>::new(move |channel| {
            $crate::coroutine::generator::generator!(channel, $body)
        })
    };
}

/// Creates a single-use generator without choosing its request channel.
///
/// The async body receives a [`GeneratorChannel`] when the returned [`UnboundGenerator`] is bound.
/// Use [`UnboundGenerator::bind`] to compose it with a parent channel, or
/// [`StaticGenerator::from`] to give it a new root channel. The body and its captured state must be
/// sendable and `'static` because they are stored until binding.
#[internal_api]
macro_rules! unbound_generator {
    ($body:expr) => {
        $crate::coroutine::generator::UnboundGenerator::from_binder(move |channel| {
            $crate::coroutine::generator::generator!(channel, $body)
        })
    };
}

impl<P: Send + 'static, Y: Send + 'static> StaticGenerator<P, Y> {
    /// Adapt a sendable iterator into a lazy generator.
    ///
    /// Advancing the generator polls `items` inline and propagates item errors.
    pub fn from_iterator(items: DeltaResultIteratorStatic<Y>) -> Self {
        static_generator!(P, async move |channel| {
            for item in items {
                channel.yield_item(item?).await?;
            }
            Ok(())
        })
    }
}

/// Generator access to connector requests and yielded output.
#[derive(Deref)]
#[internal_api]
pub(crate) struct GeneratorChannel<'call, P: Send + 'static, Y: Send + 'static> {
    #[deref]
    channel: &'call Channel<P>,
    yields: &'call Channel<YieldRequest<Y>>,
}

impl<'call, P: Send + 'static, Y: Send + 'static> GeneratorChannel<'call, P, Y> {
    fn new(channel: &'call Channel<P>, yields: &'call Channel<YieldRequest<Y>>) -> Self {
        Self { channel, yields }
    }

    /// Yield one item and suspend until the consumer receives it.
    #[internal_api]
    pub(crate) async fn yield_item(&self, item: Y) -> DeltaResult<()> {
        self.yields
            .exchange(item, |item, reply| (item, reply))
            .await
    }

    /// Forward every item from `generator`.
    pub(crate) async fn yield_from(&self, generator: UnboundGenerator<P, Y>) -> DeltaResult<()> {
        let mut generator = generator.bind(self.channel);
        while let Some(item) = generator.next().await? {
            self.yield_item(item).await?;
        }
        Ok(())
    }
}

/// Request-channel ownership supplied when binding an unbound generator.
pub(crate) enum ChannelBinding<'task, P: 'static> {
    Borrowed(&'task Channel<P>),
    Owned(Arc<Channel<P>>),
}

impl<P: 'static> Borrow<Channel<P>> for ChannelBinding<'_, P> {
    fn borrow(&self) -> &Channel<P> {
        match self {
            Self::Borrowed(channel) => channel,
            Self::Owned(channel) => channel.as_ref(),
        }
    }
}

/// Erased binder contract for an unbound generator body.
pub(crate) trait BindGeneratorFn<P: 'static, Y: Send + 'static>:
    for<'task> FnOnce(ChannelBinding<'task, P>) -> Generator<'task, Y>
{
}
impl<F, P: 'static, Y: Send + 'static> BindGeneratorFn<P, Y> for F where
    F: for<'task> FnOnce(ChannelBinding<'task, P>) -> Generator<'task, Y>
{
}

/// Unbound generator body factory whose concrete type has been erased.
pub struct UnboundGenerator<P: Send + 'static, Y: Send + 'static>(
    Box<dyn BindGeneratorFn<P, Y> + Send + 'static>,
);

impl<P: Send + 'static, Y: Send + 'static> UnboundGenerator<P, Y> {
    /// Erase a binder created while its generator body type is still concrete.
    pub(crate) fn from_binder(bind: impl BindGeneratorFn<P, Y> + Send + 'static) -> Self {
        Self(Box::new(bind))
    }

    /// Bind this unbound generator to `channel`.
    pub fn bind<'task>(self, channel: &'task Channel<P>) -> Generator<'task, Y> {
        (self.0)(ChannelBinding::Borrowed(channel))
    }
}

impl<P: Send + 'static, Y: Send + 'static> From<UnboundGenerator<P, Y>> for StaticGenerator<P, Y> {
    fn from(generator: UnboundGenerator<P, Y>) -> Self {
        StaticGenerator::new(move |channel| (generator.0)(ChannelBinding::Owned(channel)))
    }
}

/// An uninvoked generator body that becomes a [`Generator`] when paired with a [`Channel`].
///
/// The body consumes a [`GeneratorChannel`], may post connector requests and yield items, and
/// completes with `()`. [`Self::new`] stores its concrete closure type `F` and tracing span without
/// requiring `F: Send + 'static` or exposing the body's future.
///
/// Constructing a generator additionally requires a `Future + Send + 'task`, but stable Rust cannot
/// name or constrain an `AsyncFnOnce` closure's hidden call future.
///
/// Every construction path passes [`Self::witness`] while `F` is still concrete, allowing the
/// caller to name its output `Fut` and prove `Fut: DeltaFuture<()> + 'task`. Unbound generators
/// erase a higher-ranked binder built with that witness instead of erasing `F` directly.
#[doc(hidden)]
#[internal_api]
pub(crate) struct GeneratorFn<F, P, Y> {
    body: F,
    span: Span,
    _type_signature: PhantomType<(P, Y)>,
}

impl<F, P: Send + 'static, Y: Send + 'static> GeneratorFn<F, P, Y> {
    /// Stores `body` without choosing its call-future proof path.
    #[internal_api]
    pub(crate) fn new(body: F) -> Self
    where
        F: for<'call> AsyncFnOnce(GeneratorChannel<'call, P, Y>) -> DeltaResult<()>,
    {
        Self {
            body,
            span: Span::current(),
            _type_signature: PhantomType::default(),
        }
    }

    /// Creates a generator using the concrete call-future proof supplied by `witness`.
    #[internal_api]
    pub(crate) fn into_generator_with<'task, C, Fut>(
        self,
        channel: C,
        witness: impl FnOnce(Self, C, Arc<Channel<YieldRequest<Y>>>) -> Fut,
    ) -> Generator<'task, Y>
    where
        C: Borrow<Channel<P>> + Send + 'task,
        Fut: DeltaFuture<()> + 'task,
    {
        Generator::new(move |yields| witness(self, channel, yields))
    }

    /// Runs the body while exposing its concrete future to [`Self::into_generator_with`].
    #[internal_api]
    pub(crate) fn witness(
        self,
        channel: impl Borrow<Channel<P>> + Send,
        yields: Arc<Channel<YieldRequest<Y>>>,
    ) -> impl Future<Output = DeltaResult<()>>
    where
        F: for<'call> AsyncFnOnce(GeneratorChannel<'call, P, Y>) -> DeltaResult<()>,
    {
        let future = async move {
            let channel = GeneratorChannel::new(channel.borrow(), yields.as_ref());
            let output = (self.body)(channel).await;
            output.inspect_err(|err| error!(error = %err, "coroutine generator failed"))
        };
        future.instrument(self.span)
    }
}
