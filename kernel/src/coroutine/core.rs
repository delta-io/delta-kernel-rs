//! Transport primitives for connector-driven coroutines.
//!
//! A [`Channel`] posts a request and suspends its coroutine until the connector replies. The task
//! driver polls the coroutine until it either completes or posts another request.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::task::{Context, Poll, Waker};

use delta_kernel_derive::internal_api;

use crate::utils::PhantomType;
use crate::{DeltaResult, Error};

/// Sendable future whose output uses Kernel's error type.
#[internal_api]
pub(crate) trait DeltaFuture<T>: Future<Output = DeltaResult<T>> + Send {}

impl<T, F> DeltaFuture<T> for F where F: Future<Output = DeltaResult<T>> + Send {}

/// Coroutine-side capability for posting typed work to a task driver.
///
/// Posting suspends the coroutine; it never invokes the driver.
pub struct Channel<P: 'static>(Mutex<Weak<dyn Accept<P>>>);

impl<P: 'static> Default for Channel<P> {
    fn default() -> Self {
        Self(Mutex::new(Self::EMPTY_ACCEPT))
    }
}

impl<P: 'static> Channel<P> {
    // `Weak::new` requires a sized target; this specialization is never upgraded or constructed.
    const EMPTY_ACCEPT: Weak<dyn Accept<P>> =
        Weak::<Exchange<(), (), fn((), Reply<()>) -> P, P>>::new();

    pub(crate) fn take(&self) -> Option<P> {
        let request = std::mem::replace(&mut *try_lock(&self.0)?, Self::EMPTY_ACCEPT);
        request.upgrade()?.accept()
    }

    fn put(&self, request: Weak<dyn Accept<P>>) {
        // Drop the lock before dropping P (which could run arbitrary code)
        let _ = {
            let Some(mut current) = try_lock(&self.0) else {
                return;
            };
            std::mem::replace(&mut *current, request)
        };
    }
}

trait Accept<P>: Send + Sync {
    fn accept(self: Arc<Self>) -> Option<P>;
}

trait Complete<In>: Send + Sync {
    fn complete(&self, result: DeltaResult<In>);
}

/// Single-use capability to complete a connector request.
///
/// Dropping this capability without replying rejects the request.
#[must_use = "dropping a reply rejects its connector request"]
pub struct Reply<In: Send + 'static>(Weak<dyn Complete<In>>);

impl<In: Send + 'static> Reply<In> {
    /// Deliver `result` to the suspended Kernel operation.
    ///
    /// The result is discarded if that operation has been abandoned.
    pub fn send(self, result: DeltaResult<In>) {
        if let Some(exchange) = self.0.upgrade() {
            exchange.complete(result);
        }
    }
}

enum ExchangeState<Out, In, Make> {
    Outbound(Out, Make),
    Inbound(DeltaResult<In>),
    Empty,
}

struct Exchange<Out, In, Make, P> {
    state: Mutex<ExchangeState<Out, In, Make>>,
    request_type: PhantomType<P>,
}

impl<Out, In, Make, P> Exchange<Out, In, Make, P> {
    fn new(outbound: Out, make_request: Make) -> Self {
        Self {
            state: Mutex::new(ExchangeState::Outbound(outbound, make_request)),
            request_type: PhantomType::default(),
        }
    }
}

impl<Out, In, Make, P> Complete<In> for Exchange<Out, In, Make, P>
where
    Out: Send,
    In: Send,
    Make: Send,
{
    fn complete(&self, result: DeltaResult<In>) {
        let Some(mut state) = try_lock(&self.state) else {
            return;
        };
        if matches!(*state, ExchangeState::Empty) {
            *state = ExchangeState::Inbound(result);
        } else {
            drop(state);
            debug_assert!(false, "accepted exchange was not awaiting a reply");
        }
    }
}

impl<Out, In, Make, P> Accept<P> for Exchange<Out, In, Make, P>
where
    Out: Send + 'static,
    In: Send + 'static,
    Make: FnOnce(Out, Reply<In>) -> P + Send + 'static,
    P: 'static,
{
    fn accept(self: Arc<Self>) -> Option<P> {
        let (outbound, make_request) = {
            let mut state = try_lock(&self.state)?;
            match std::mem::replace(&mut *state, ExchangeState::Empty) {
                ExchangeState::Outbound(outbound, make_request) => (outbound, make_request),
                invalid => {
                    *state = invalid;
                    drop(state);
                    debug_assert!(false, "live exchange was not outbound");
                    return None;
                }
            }
        };

        let completion: Arc<dyn Complete<In>> = self;
        let reply = Reply(Arc::downgrade(&completion));
        Some(make_request(outbound, reply))
    }
}

/// Request/reply exchange available on every typed coroutine channel.
#[internal_api]
pub(crate) trait ChannelExchange<P: Send + 'static> {
    /// Post a request containing `outbound` and suspend until its driver replies.
    fn exchange<Out: Send + 'static, In: Send + 'static, R: Into<P>>(
        &self,
        outbound: Out,
        make_request: impl FnOnce(Out, Reply<In>) -> R + Send + 'static,
    ) -> impl DeltaFuture<In>;
}

impl<P: Send + 'static> ChannelExchange<P> for Channel<P> {
    fn exchange<Out: Send + 'static, In: Send + 'static, R: Into<P>>(
        &self,
        outbound: Out,
        make_request: impl FnOnce(Out, Reply<In>) -> R + Send + 'static,
    ) -> impl DeltaFuture<In> {
        Wait {
            channel: self,
            exchange: Arc::new(Exchange::new(outbound, move |outbound, reply| {
                make_request(outbound, reply).into()
            })),
        }
    }
}

/// Result of advancing a [`Task`].
pub(crate) enum Step<R, O> {
    /// The task completed.
    Done(O),
    /// The request currently stored in the task's channel.
    Request(R),
}

/// Retains one pinned coroutine and the request lanes observed by its driver.
pub(crate) struct Task<'task, P: Send + 'static, O> {
    body: Option<Pin<Box<dyn DeltaFuture<O> + 'task>>>,
    channel: Arc<Channel<P>>,
}

impl<'task, P: Send + 'static, O> Task<'task, P, O> {
    /// Poll once with a no-op waker and return completion or the request posted by that poll.
    ///
    /// Calling after completion returns an error.
    pub(crate) fn advance(&mut self) -> DeltaResult<Step<P, O>> {
        match self.poll_step(&mut noop_context()) {
            Poll::Ready(step) => step,
            Poll::Pending => Err(Error::internal_error(
                "coroutine suspended without posting a request",
            )),
        }
    }

    /// Poll once using `context`, returning pending when the task offers no request.
    pub(crate) fn poll_step(&mut self, context: &mut Context<'_>) -> Poll<DeltaResult<Step<P, O>>> {
        let Some(body) = self.body.as_mut() else {
            return Poll::Ready(Err(Error::internal_error(
                "coroutine task was advanced after completion",
            )));
        };
        if let Poll::Ready(output) = body.as_mut().poll(context) {
            self.body = None;
            return Poll::Ready(output.map(Step::Done));
        }
        if let Some(request) = self.channel.take() {
            return Poll::Ready(Ok(Step::Request(request)));
        }
        Poll::Pending
    }

    /// Create a task and pass its request channel to `make_body`.
    pub(crate) fn new<Fut>(make_body: impl FnOnce(Arc<Channel<P>>) -> Fut) -> Self
    where
        Fut: DeltaFuture<O> + 'task,
    {
        let channel = Arc::new(Channel::default());
        Self {
            body: Some(Box::pin(make_body(Arc::clone(&channel)))),
            channel,
        }
    }
}

/// Create a task context backed by a no-op waker.
pub(super) fn noop_context() -> Context<'static> {
    Context::from_waker(Waker::noop())
}

struct Wait<'channel, P: 'static, Out, In, Make> {
    channel: &'channel Channel<P>,
    exchange: Arc<Exchange<Out, In, Make, P>>,
}

impl<P, Out, In, Make> Future for Wait<'_, P, Out, In, Make>
where
    P: Send + 'static,
    Out: Send + 'static,
    In: Send + 'static,
    Make: FnOnce(Out, Reply<In>) -> P + Send + 'static,
{
    type Output = DeltaResult<In>;

    fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
        if Arc::weak_count(&self.exchange) != 0 {
            return Poll::Pending;
        }
        let Some(mut state) = try_lock(&self.exchange.state) else {
            return Poll::Pending;
        };
        match std::mem::replace(&mut *state, ExchangeState::Empty) {
            ExchangeState::Outbound(outbound, make_request) => {
                *state = ExchangeState::Outbound(outbound, make_request);
                drop(state);
                let acceptor: Arc<dyn Accept<P>> = self.exchange.clone();
                self.channel.put(Arc::downgrade(&acceptor));
                Poll::Pending
            }
            ExchangeState::Empty => {
                drop(state);
                Poll::Ready(Err(Error::generic(
                    "connector dropped request without replying",
                )))
            }
            ExchangeState::Inbound(inbound) => {
                drop(state);
                Poll::Ready(inbound)
            }
        }
    }
}

fn try_lock<T>(mutex: &Mutex<T>) -> Option<MutexGuard<'_, T>> {
    match mutex.try_lock() {
        Ok(guard) => Some(guard),
        Err(error) => {
            drop(error);
            debug_assert!(false, "structurally uncontended mutex was held or poisoned");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::pin::pin;

    use super::*;

    enum Request {
        Ping(&'static str, Reply<()>),
    }

    #[test]
    fn accepted_request_completes_with_its_reply() {
        let channel = Channel::default();
        let mut future = pin!(channel.exchange("request", Request::Ping));
        assert!(future.as_mut().poll(&mut noop_context()).is_pending());
        let Some(Request::Ping(outbound, reply)) = channel.take() else {
            panic!("live exchange was not accepted");
        };
        assert_eq!(outbound, "request");
        reply.send(Ok(()));
        assert!(matches!(
            future.as_mut().poll(&mut noop_context()),
            Poll::Ready(Ok(()))
        ));
    }

    #[test]
    fn overlapping_requests_use_last_writer_wins() {
        let channel = Channel::default();
        let mut first = pin!(channel.exchange("first", Request::Ping));
        let mut second = pin!(channel.exchange("second", Request::Ping));
        assert!(first.as_mut().poll(&mut noop_context()).is_pending());
        assert!(second.as_mut().poll(&mut noop_context()).is_pending());

        let Some(Request::Ping(outbound, second_reply)) = channel.take() else {
            panic!("second exchange was not accepted");
        };
        assert_eq!(outbound, "second");

        assert!(first.as_mut().poll(&mut noop_context()).is_pending());
        let Some(Request::Ping(outbound, first_reply)) = channel.take() else {
            panic!("first exchange was not reoffered");
        };
        assert_eq!(outbound, "first");

        second_reply.send(Ok(()));
        first_reply.send(Ok(()));
        assert!(second.as_mut().poll(&mut noop_context()).is_ready());
        assert!(first.as_mut().poll(&mut noop_context()).is_ready());
    }

    #[test]
    fn dropping_accepted_reply_rejects_request() {
        let channel = Channel::default();
        let mut future = pin!(channel.exchange("request", Request::Ping));
        assert!(future.as_mut().poll(&mut noop_context()).is_pending());
        let Some(Request::Ping(_outbound, reply)) = channel.take() else {
            panic!("live exchange was not accepted");
        };
        drop(reply);

        let Poll::Ready(Err(err)) = future.as_mut().poll(&mut noop_context()) else {
            panic!("dropped reply did not reject its request");
        };
        assert!(err
            .to_string()
            .contains("connector dropped request without replying"));
    }

    #[test]
    fn abandoned_waiter_expires_offer_and_reply() {
        let channel = Channel::default();
        let mut future = Box::pin(channel.exchange("request", Request::Ping));
        assert!(future.as_mut().poll(&mut noop_context()).is_pending());
        drop(future);
        assert!(channel.take().is_none());

        let mut future = Box::pin(channel.exchange("request", Request::Ping));
        assert!(future.as_mut().poll(&mut noop_context()).is_pending());
        let Some(Request::Ping(_outbound, reply)) = channel.take() else {
            panic!("live exchange was not accepted");
        };
        drop(future);
        reply.send(Ok(()));
    }
}
